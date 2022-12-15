package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/BitTraceProject/BitTrace-Types/pkg/constants"
	"github.com/fsnotify/fsnotify"
	"io"
	"log"
	"net/http"
	"time"

	"github.com/BitTraceProject/BitTrace-Exporter/common"
	"github.com/BitTraceProject/BitTrace-Types/pkg/config"
	"github.com/BitTraceProject/BitTrace-Types/pkg/protocol"
)

type (
	// ExporterServer 串行化的，无需加锁
	ExporterServer struct {
		conf config.ExporterConfig

		// 当前日志文件的读取进度
		currentN int64
		// 当前日志文件的 id
		currentFileID int64
		// 当前日志文件的 day
		currentDay string
		// filepath = LogFileBasePath + "/" + loggerName + "/" + currentDay + "/" + currentFileID + ".log"
		currentFilepath string
		// 设置的同步时间间隔
		pollInterval time.Duration
		// 当前的同步时间间隔
		currentPollInterval time.Duration

		watcher *fsnotify.Watcher

		stopCh <-chan struct{}
	}
)

const (
	JoinAPIPattern = "/join?exporter_tag=%s"
	QuitAPIPattern = "/quit?exporter_tag=%s&lazy_quit=%v"
)

func NewExporterServer(conf config.ExporterConfig, stopCh <-chan struct{}) *ExporterServer {
	currentN := conf.StartSeq % constants.RECEIVE_DATA_PACKAGE_MAXN
	currentFileID := conf.StartSeq / constants.RECEIVE_DATA_PACKAGE_MAXN
	currentDay := conf.StartDay
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		panic(err)
	}
	s := &ExporterServer{
		conf:                conf,
		currentN:            currentN,
		currentFileID:       currentFileID,
		currentDay:          currentDay,
		currentFilepath:     common.CurrentIDLogFilepath(common.LogFileBasePath, conf.Tag, currentDay, currentFileID),
		pollInterval:        time.Duration(conf.PollInterval) * time.Millisecond,
		currentPollInterval: time.Duration(conf.PollInterval) * time.Millisecond,
		watcher:             watcher,
		stopCh:              stopCh,
	}
	return s
}

// Run 不会加锁，所以不要调用多次
func (s *ExporterServer) Run() {
	// join receiver
	var (
		resp        *http.Response
		receiveResp protocol.ReceiverDataResponse
		err         error
	)
	for i := 0; i < constants.RETRY_COUNT; i++ {
		resp, err = http.Get(s.conf.ReceiverServerAddr + fmt.Sprintf(JoinAPIPattern, s.conf.Tag))
		if err == nil && resp.StatusCode == http.StatusOK {
			respBody, err := io.ReadAll(resp.Body)
			err = json.Unmarshal(respBody, &receiveResp)
			if err == nil {
				break
			}
		}
	}
	if err != nil || resp.StatusCode != http.StatusOK {
		log.Printf("[Run]get resp:%v,err:%v", resp, err)
		panic(err)
	}

	if !receiveResp.OK {
		log.Printf("[Run]get resp not ok")
		panic(err)
	}

	go s.poll()
	s.runAndWait()
}

func (s *ExporterServer) runAndWait() {
	<-s.stopCh
	// quit receiver
	// join receiver
	var (
		resp        *http.Response
		receiveResp protocol.ReceiverDataResponse
		err         error
	)
	for i := 0; i < constants.RETRY_COUNT; i++ {
		// TODO exporter 添加 lazyQuit 配置
		resp, err = http.Get(s.conf.ReceiverServerAddr + fmt.Sprintf(QuitAPIPattern, s.conf.Tag, false))
		if err == nil && resp.StatusCode == http.StatusOK {
			respBody, err := io.ReadAll(resp.Body)
			err = json.Unmarshal(respBody, &receiveResp)
			if err == nil {
				break
			}
		}
	}
	if err != nil || resp.StatusCode != http.StatusOK {
		log.Printf("[runAndWait]get resp:%v,err:%v", resp, err)
	}

	if !receiveResp.OK {
		log.Printf("[runAndWait]get resp not ok")
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
				log.Printf("[poll]err:%v", err)
			} else {
				if nextDay {
					s.currentN = 0
					s.currentFileID = 0
					s.currentDay = common.CurrentDay(common.DayTime(s.currentDay).Add(24 * time.Hour))
					s.currentFilepath = common.CurrentIDLogFilepath(common.LogFileBasePath, s.conf.Tag, s.currentDay, s.currentFileID)
				}
				if nextFile {
					s.currentN = 0
					s.currentFileID += 1
					s.currentFilepath = common.CurrentIDLogFilepath(common.LogFileBasePath, s.conf.Tag, s.currentDay, s.currentFileID)
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
	err := s.watcher.Add(s.currentFilepath)
	if err != nil {
		return
	}
	defer func() {
		waitWatchCh <- true
		s.watcher.Remove(s.currentFilepath)
	}()
	for {
		select {
		case e, ok := <-s.watcher.Events:
			if ok && e.Op == fsnotify.Write {
				return
			}
		case err := <-s.watcher.Errors:
			log.Printf("[watch]watch file:%s,err:%v", s.currentFilepath, err)
		}
	}
}

func (s *ExporterServer) export() (nextDay, nextFile bool, err error) {
	lines, eof, err := common.ScanFileLines(s.currentFilepath, s.currentN)
	if err != nil {
		return nextDay, nextFile, err
	}

	readCount := int64(len(lines))
	if eof {
		// 切换下一天，更新 n，id，day
		nextDay = true
	} else {
		nextDay = false
		if s.currentN+readCount >= constants.RECEIVE_DATA_PACKAGE_MAXN {
			nextFile = true
		}
	}
	if len(lines) == 0 {
		return nextDay, nextFile, nil
	}

	// 调用接口上报
	var receiveReq = protocol.ReceiverDataRequest{
		ExporterTag: s.conf.Tag,
		DataPackage: protocol.ReceiverDataPackage{
			Day:      s.currentDay,
			LeftSeq:  constants.RECEIVE_DATA_PACKAGE_MAXN*s.currentFileID + s.currentN,
			RightSeq: readCount + constants.RECEIVE_DATA_PACKAGE_MAXN*s.currentFileID + s.currentN,
			//DataPackage: lines,
		},
	}

	reqAsBytes, err := json.Marshal(receiveReq)
	if err != nil {
		log.Printf("[export]json err:%v", err)
		nextDay, nextFile = false, false
		return nextDay, nextFile, err
	}
	var (
		resp        *http.Response
		receiveResp protocol.ReceiverDataResponse
	)
	for i := 0; i < constants.RETRY_COUNT; i++ {
		resp, err = http.Post(s.conf.ReceiverServerAddr, "", bytes.NewReader(reqAsBytes))
		if err == nil && resp.StatusCode == http.StatusOK {
			respBody, err := io.ReadAll(resp.Body)
			err = json.Unmarshal(respBody, &receiveResp)
			if err == nil {
				break
			}
		}
	}
	if err != nil || resp.StatusCode != http.StatusOK {
		log.Printf("[export]post resp:%v,err:%v", resp, err)
		nextDay, nextFile = false, false
		return nextDay, nextFile, err
	}

	if !receiveResp.OK {
		log.Printf("[export]post resp not ok")
		// 可能在 mq 处发生了拥塞，避免 OOM，同时调整 poll interval
		s.currentPollInterval = constants.EXPORTER_POLL_DEFAULT_INTERVAL_BLOCK
		nextDay, nextFile = false, false
		return nextDay, nextFile, err
	}

	s.currentN += readCount
	// 调整 poll interval
	s.currentPollInterval = s.pollInterval
	return nextDay, nextFile, nil
}
