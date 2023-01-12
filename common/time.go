package common

import (
	"path/filepath"
	"strconv"
	"time"

	"github.com/BitTraceProject/BitTrace-Types/pkg/constants"
)

func DayTime(dayStr string) time.Time {
	day, _ := time.Parse(constants.TIME_LAYOUT_DAY, dayStr)
	return day
}

func CurrentDay(day time.Time) string {
	return day.Format(constants.TIME_LAYOUT_DAY)
}

func CurrentIDLogFilename(id int64) string {
	return strconv.FormatInt(id, 10) + ".log"
}

// CurrentIDLogFileParentPath basePath/loggerName/day/
func CurrentIDLogFileParentPath(basePath, loggerName string, day string) string {
	return filepath.Join(basePath, loggerName, day)
}

// CurrentIDLogFilepath basePath/loggerName/day/id.log
func CurrentIDLogFilepath(basePath, loggerName string, day string, id int64) string {
	return filepath.Join(basePath, loggerName, day, CurrentIDLogFilename(id))
}
