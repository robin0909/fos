package stream

import (
	"fmt"
	"io"
	"net/http"
	"strings"
)

type GetStream struct {
	reader io.Reader
}

func New(address, bucket, obj string) (*GetStream, error) {

	url := strings.Join([]string{"http:/", address, "objects", bucket, obj}, "/")
	r, err := http.Get(url)
	if err != nil {
		return nil, err
	}

	if r.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("dataSerevr return http code %d", r.StatusCode)
	}

	return &GetStream{r.Body}, nil
}

func (r *GetStream) Read(p []byte) (n int, err error) {
	return r.reader.Read(p)
}
