package common

import (
	"bufio"
	"bytes"
	"os"

	"github.com/BitTraceProject/BitTrace-Types/pkg/constants"
	"github.com/edsrzf/mmap-go"
)

// ScanFileLines 从 startN 处开始，scan 数据到达文件结尾，返回数据数组，eof 和 error，
// eof 判断是按照自定义的一行来判断的，注意这里潜在的问题是 logger 与 exporter 读写竞争可能导致日志输出到错误的 day，
// 所以在 resolver 处理时，只依赖数据本身，不依赖 logger 的信息，logger 和 exporter 只需要保证不丢数据就行
func ScanFileLines(filePath string, startN int64) ([][]byte, bool, error) {
	f, err := os.OpenFile(filePath, os.O_RDWR, 0644)
	if err != nil {
		return nil, false, err
	}
	defer f.Close()

	m, err := mmap.Map(f, mmap.RDONLY, 0)
	if err != nil {
		return nil, false, err
	}
	defer m.Unmap()

	r := bytes.NewReader(m)
	s := bufio.NewScanner(r)
	for i := int64(0); i < startN; i++ {
		if ok := s.Scan(); !ok {
			return nil, false, nil
		}
		// 当前 n 处还没有数据，但是已经 eof 了
		if string(s.Bytes()) == constants.LOG_EOF_DAY {
			return nil, true, nil
		}
	}
	var lines = make([][]byte, 0)
	for s.Scan() {
		line := s.Bytes()
		if string(line) == constants.LOG_EOF_DAY {
			return lines, true, nil
		}
		lines = append(lines, line)
	}
	return lines, false, nil
}

func IsFileExisted(filePath string) bool {
	info, err := os.Stat(filePath)
	return (err == nil || os.IsExist(err)) && !info.IsDir()
}

func IsDirExisted(dirPath string) bool {
	info, err := os.Stat(dirPath)
	return (err == nil || os.IsExist(err)) && info.IsDir()
}

func DirFileCount(dirPath string) int {
	files, _ := os.ReadDir(dirPath)
	return len(files)
}
