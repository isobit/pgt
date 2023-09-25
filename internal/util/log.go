package util

import (
	"fmt"
	"io"
	"os"
)

var Log io.Writer = os.Stderr
var LogLevel int = 0
var LogColor bool = false

var Logf func(int, string, ...interface{}) (int, error) = defaultLogf

func defaultLogf(level int, format string, v ...interface{}) (int, error) {
	if level > LogLevel {
		return 0, nil
	}
	if LogColor {
		if level >= 0 {
			format = "\u001b[30;1m" + format + "\u001b[0m"
		} else if level == -1 {
			format = "\u001b[33;1m" + format + "\u001b[0m"
		} else {
			format = "\u001b[31;1m" + format + "\u001b[0m"
		}
	}
	if len(format) > 0 && format[len(format)-1] != '\n' {
		format = format + "\n"
	}
	return fmt.Fprintf(Log, format, v...)
}
