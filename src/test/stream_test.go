package test

import (
	"com.github/robin0909/fos/src/stream"
	"testing"
)

func TestGet(t *testing.T) {

	s, err := stream.New("192.168.31.234:8083", "media", "demo.pdf")
	if err != nil {
		t.Error("error", err)
		return
	}

	if s == nil {
		t.Log("获取stream失败")
	}

}
