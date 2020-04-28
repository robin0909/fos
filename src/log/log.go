package log

import (
	"io"
	"io/ioutil"
	"log"
	"os"
)

// 定义级别
var (
	Trace *log.Logger
	Info  *log.Logger
	Warn  *log.Logger
	Error *log.Logger
)

func init() {

	file, err := os.OpenFile("fos.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalln("Failed to open error log file", err)
	}

	// 输出到 /dev/null 不做任何显示
	Trace = log.New(ioutil.Discard, "TRACE: ", log.Ldate|log.Ltime|log.Lshortfile)
	// 输出到控制台
	Info = log.New(io.MultiWriter(file, os.Stdout), "INFO: ", log.Ldate|log.Ltime|log.Lshortfile)

	Warn = log.New(io.MultiWriter(file, os.Stdout), "WARN: ", log.Ldate|log.Ltime|log.Lshortfile)
	Error = log.New(io.MultiWriter(file, os.Stderr), "ERROR: ", log.Ldate|log.Ltime|log.Lshortfile)

}

func FailOnErr(err error, msg string) {
	if err != nil {
		Error.Fatalf("%s:%s", msg, err)
	}
}

func FailOnWarn(err error, msg string) {
	if err != nil {
		Warn.Printf("%s:%s", msg, err)
	}
}
