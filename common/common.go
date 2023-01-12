package common

import (
	"path/filepath"

	"github.com/BitTraceProject/BitTrace-Types/pkg/constants"
)

var (
	LogFileBasePath string
)

func init() {
	LogFileBasePath = filepath.Join("/root", constants.BITTRACE_ROOT_DIR, constants.BITTRACE_LOG_DIR)
}
