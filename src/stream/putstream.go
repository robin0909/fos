package stream

import (
	"fmt"
	"io"
	"net/http"
	"strings"
)

type PutStream struct {
	writer *io.PipeWriter
	c      chan error
}

func NewPutStream(address, bucket, obj string) *PutStream {
	reader, writer := io.Pipe()
	c := make(chan error)
	go func() {
		url := strings.Join([]string{"http:/", address, "objects", bucket, obj}, "/")
		// 管道读取 reader 是阻塞，所以需要放在 goroutine 来执行
		request, _ := http.NewRequest(http.MethodPut, url, reader)
		client := http.Client{}
		r, err := client.Do(request)
		if err == nil && r.StatusCode != http.StatusOK {
			err = fmt.Errorf("dataServer put data error http code %d", r.StatusCode)
		}
		c <- err
	}()

	return &PutStream{writer, c}
}

func (w *PutStream) Write(p []byte) (n int, err error) {
	n, err = w.writer.Write(p)
	return
}

func (w *PutStream) Close() error {
	_ = w.writer.Close()
	return <-w.c
}
