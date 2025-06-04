package utiltest

import (
	"testing"

	"github.com/WlayRay/ElectricSearch/util"
)

func TestLog(t *testing.T) {
	// 测试日志记录功能
	util.Info("This is an info message")
	util.Warn("This is a warning message")
	util.Error("This is an error message")
}
