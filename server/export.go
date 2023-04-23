package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/BitTraceProject/BitTrace-Types/pkg/common"
	"github.com/BitTraceProject/BitTrace-Types/pkg/config"
	"github.com/BitTraceProject/BitTrace-Types/pkg/constants"
	"github.com/BitTraceProject/BitTrace-Types/pkg/logger"
	"github.com/BitTraceProject/BitTrace-Types/pkg/metric"
	"github.com/BitTraceProject/BitTrace-Types/pkg/protocol"

	"github.com/fsnotify/fsnotify"
)

type (
	// ExporterServer 串行化的，无需加锁
	ExporterServer struct {
		conf config.ExporterConfig

		watcher *fsnotify.Watcher

		// 进度信息，用于同步和恢复启动
		currentProgress protocol.ExporterProgress
		// filepath = LogFileBasePath + "/" + loggerName + "/" + currentDay + "/" + currentFileID + ".log"
		currentFilepath string
		// 设置的同步时间间隔
		pollInterval time.Duration
		// 当前的同步时间间隔
		currentPollInterval time.Duration

		exporterLogger logger.Logger

		stopCh <-chan struct{}
	}
)

const (
	JoinAPIPattern = "/exporter/join?exporter_tag=%s"
	DataAPIPattern = "/exporter/data"
	QuitAPIPattern = "/exporter/quit?exporter_tag=%s&lazy_quit=%v"
)

func NewExporterServer(conf config.ExporterConfig, stopCh <-chan struct{}) *ExporterServer {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		panic(err)
	}
	currentProgress := protocol.ExporterProgress{
		CurrentN:      0,
		CurrentFileID: 0,
		CurrentDay:    common.CurrentDay(time.Now()),
	}
	s := &ExporterServer{
		conf:                conf,
		currentProgress:     currentProgress,
		currentFilepath:     common.GenLogFilepath(constants.LOGGER_FILE_BASE_PATH, conf.ExporterTag, currentProgress.CurrentDay, currentProgress.CurrentFileID),
		pollInterval:        time.Duration(conf.PollInterval) * time.Millisecond,
		currentPollInterval: time.Duration(conf.PollInterval) * time.Millisecond,
		watcher:             watcher,
		exporterLogger:      logger.GetLogger(fmt.Sprintf("bittrace_exporter_%s", conf.ExporterTag)),
		stopCh:              stopCh,
	}
	return s
}

// Run 不会加锁，所以不要调用多次
func (s *ExporterServer) Run() {
	// join receiver
	var (
		resp        *http.Response
		receiveResp protocol.ReceiverJoinResponse
		err         error
	)
	for i := 0; i < constants.DEFAULT_RETRY_COUNT; i++ {
		resp, err = http.Get(s.conf.ReceiverServerAddr + fmt.Sprintf(JoinAPIPattern, s.conf.ExporterTag))
		if err == nil && resp.StatusCode == http.StatusOK {
			respBody, err := io.ReadAll(resp.Body)
			err = json.Unmarshal(respBody, &receiveResp)
			if err == nil {
				break
			}
		}
	}
	if err != nil || resp.StatusCode != http.StatusOK {
		s.exporterLogger.Error("[Run]get resp:%v,err:%v", resp, err)
		panic(err)
	}

	if !receiveResp.OK {
		s.exporterLogger.Error("[Run]get resp not ok,%v", receiveResp)
		panic(err)
	}

	// exporter 历史进度信息有效
	if receiveResp.Info.CurrentProgress.CurrentDay != "" {
		s.currentProgress = receiveResp.Info.CurrentProgress
		s.currentFilepath = common.GenLogFilepath(constants.LOGGER_FILE_BASE_PATH, s.conf.ExporterTag, s.currentProgress.CurrentDay, s.currentProgress.CurrentFileID)
	}
	go s.poll()
	s.runAndWait()
}

func (s *ExporterServer) runAndWait() {
	<-s.stopCh
	// quit receiver
	var (
		resp        *http.Response
		receiveResp protocol.ReceiverQuitResponse
		err         error
	)
	for i := 0; i < constants.DEFAULT_RETRY_COUNT; i++ {
		// 默认 lazy quit 是 false
		resp, err = http.Get(s.conf.ReceiverServerAddr + fmt.Sprintf(QuitAPIPattern, s.conf.ExporterTag, false))
		if err == nil && resp.StatusCode == http.StatusOK {
			respBody, err := io.ReadAll(resp.Body)
			err = json.Unmarshal(respBody, &receiveResp)
			if err == nil {
				break
			}
		}
	}
	if err != nil || resp.StatusCode != http.StatusOK {
		s.exporterLogger.Error("[runAndWait]get resp:%v,err:%v", resp, err)

		// metric
		metric.MetricLogModuleError(metric.MetricModuleError{
			Tag:       s.conf.ExporterTag,
			Timestamp: common.FromNow().String(),
			Module:    metric.ModuleTypeExporter,
		})
	}

	if !receiveResp.OK {
		s.exporterLogger.Error("[runAndWait]get resp not ok")
	}
}

func (s *ExporterServer) poll() {
	var (
		timer       = time.NewTimer(s.currentPollInterval)
		waitWatchCh = make(chan bool, 1)
	)
	for {
		select {
		case <-timer.C:
			// export
			nextDay, nextFile, err := s.export()
			if err != nil {
				s.exporterLogger.Error("[poll]err:%v", err)
			} else {
				// 此时的 s.currentProgress 是最新的
				if nextDay {
					s.currentFilepath = common.GenLogFilepath(constants.LOGGER_FILE_BASE_PATH, s.conf.ExporterTag, s.currentProgress.CurrentDay, s.currentProgress.CurrentFileID)
					s.exporterLogger.Info("[poll]next day :%s", s.currentFilepath)
				}
				if nextFile {
					s.currentFilepath = common.GenLogFilepath(constants.LOGGER_FILE_BASE_PATH, s.conf.ExporterTag, s.currentProgress.CurrentDay, s.currentProgress.CurrentFileID)
					s.exporterLogger.Info("[poll]next file :%s", s.currentFilepath)
				}
			}
			if err != nil || nextDay || nextFile {
				// 普通情况直接开始下一次
				waitWatchCh <- true
			}
			if !nextDay && !nextFile {
				// 使用 watch 优化 exporter 轮询的次数，
				// 启动一个 goroutine watch 当前 file，
				// 直到 watch 到 write 事件，期间不进行 poll，
				// write 后关闭该协程，进行下一次 poll，
				// 循环以上过程。
				go s.watch(waitWatchCh)
			}
			// 阻塞用于等到 next 或者 write 事件
			<-waitWatchCh
			// 使用 timer 而非 ticker，保证串行化，因为 export 这个函数运行可能超过 pollInterval
			timer.Reset(s.currentPollInterval)
		case <-s.stopCh:
			timer.Stop()
			return
		}
	}
}

func (s *ExporterServer) watch(waitWatchCh chan<- bool) {
	s.exporterLogger.Info("[watch]watch file :%s", s.currentFilepath)
	err := s.watcher.Add(s.currentFilepath)
	if err != nil {
		return
	}
	defer func() {
		waitWatchCh <- true
		s.watcher.Remove(s.currentFilepath)
	}()
	timer := time.NewTimer(constants.EXPORTER_WATCH_MAX_DURATION) // 防止一直 watch 阻塞，导致切换到下一天时，exporter 依然阻塞，进而导致元信息进度与真实进度脱轨
	for {
		select {
		case e, ok := <-s.watcher.Events:
			if ok && e.Op == fsnotify.Write {
				return
			}
		case <-timer.C:
			s.exporterLogger.Error("[watch]watch timeout:%s", s.currentFilepath)
			return
		case err := <-s.watcher.Errors:
			s.exporterLogger.Error("[watch]watch file:%s,err:%v", s.currentFilepath, err)
		}
	}
}

func (s *ExporterServer) export() (nextDay, nextFile bool, err error) {
	var currentProgress = s.currentProgress

	lines, eof, err := common.ScanFileLines(s.currentFilepath, currentProgress.CurrentN)
	if err != nil {
		// metric
		metric.MetricLogModuleError(metric.MetricModuleError{
			Tag:       s.conf.ExporterTag,
			Timestamp: common.FromNow().String(),
			Module:    metric.ModuleTypeExporter,
		})
		return nextDay, nextFile, err
	}

	readCount := int64(len(lines))
	if eof {
		// 切换下一天，更新 n，id，day
		nextDay = true
	} else {
		nextDay = false
		if currentProgress.CurrentN+readCount >= constants.EXPORTER_DATA_PACKAGE_MAXN {
			nextFile = true
		}
	}

	if readCount == 0 && !eof {
		return nextDay, nextFile, err
	}
	currentProgress.CurrentN = currentProgress.CurrentN + readCount
	if nextDay {
		currentProgress.CurrentN = 0
		currentProgress.CurrentFileID = 0
		currentProgress.CurrentDay = common.CurrentDay(common.DayTime(currentProgress.CurrentDay).Add(24 * time.Hour))
	}
	if nextFile {
		currentProgress.CurrentN = 0
		currentProgress.CurrentFileID += 1
	}
	// 调用接口上报，这里可能 len(lines) = 0，但是为了上报 eof 必须这样
	var receiveReq = protocol.ReceiverDataRequest{
		ExporterTag: s.conf.ExporterTag,
		// s.currentProgress 是旧 progress 用于失败时回滚，currentProgress 是最新 progress，用于更新最新进度
		DataPackage: protocol.ReceiverDataPackage{
			Day:         s.currentProgress.CurrentDay,
			LeftSeq:     constants.EXPORTER_DATA_PACKAGE_MAXN*s.currentProgress.CurrentFileID + s.currentProgress.CurrentN,
			RightSeq:    readCount + constants.EXPORTER_DATA_PACKAGE_MAXN*s.currentProgress.CurrentFileID + s.currentProgress.CurrentN,
			DataPackage: lines,
			EOF:         eof,
		},
		CurrentProgress: currentProgress,
	}

	reqAsBytes, err := json.Marshal(receiveReq)
	if err != nil {
		s.exporterLogger.Error("[export]json err:%v", err)
		nextDay, nextFile = false, false

		// metric
		metric.MetricLogModuleError(metric.MetricModuleError{
			Tag:       s.conf.ExporterTag,
			Timestamp: common.FromNow().String(),
			Module:    metric.ModuleTypeExporter,
		})
		return nextDay, nextFile, err
	}
	var (
		resp        *http.Response
		receiveResp protocol.ReceiverDataResponse
	)
	for i := 0; i < constants.DEFAULT_RETRY_COUNT; i++ {
		resp, err = http.Post(s.conf.ReceiverServerAddr+DataAPIPattern, "application/json", bytes.NewReader(reqAsBytes))
		if err == nil && resp.StatusCode == http.StatusOK {
			respBody, err := io.ReadAll(resp.Body)
			err = json.Unmarshal(respBody, &receiveResp)
			if err == nil {
				break
			}
		}
	}
	if err != nil || resp.StatusCode != http.StatusOK {
		s.exporterLogger.Error("[export]post resp:%v,err:%v", resp, err)
		nextDay, nextFile = false, false

		// metric
		metric.MetricLogModuleError(metric.MetricModuleError{
			Tag:       s.conf.ExporterTag,
			Timestamp: common.FromNow().String(),
			Module:    metric.ModuleTypeExporter,
		})
		return nextDay, nextFile, err
	}

	if !receiveResp.OK {
		s.exporterLogger.Error("[export]post resp not ok")
		// 可能在 mq 处发生了拥塞，避免 OOM，同时调整 poll interval
		s.currentPollInterval = constants.EXPORTER_POLL_DEFAULT_INTERVAL_BLOCKING
		nextDay, nextFile = false, false
		return nextDay, nextFile, err
	}

	s.exporterLogger.Info("[export]old progress:%v, new progress:%v, others:%d,%v,%v", s.currentProgress, currentProgress, readCount, nextFile, nextDay)

	s.currentProgress = currentProgress
	// 调整 poll interval
	s.currentPollInterval = s.pollInterval
	return nextDay, nextFile, nil
}
