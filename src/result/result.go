package result

import (
	"com.github/robin0909/fos/log"
	"encoding/json"
	"fmt"
)

type H map[string]interface{}

// map 本身就是引用类型
func (h H) ToBytes() []byte {
	bytes, err := json.Marshal(h)
	if err != nil {
		log.Warn.Println("序列化失败", err)
		return nil
	}
	return bytes
}

func ResultOk() []byte {
	hh := H{"code": 200, "message": "ok"}
	return hh.ToBytes()
}

func Result(code int, message string) []byte {
	hh := H{"code": code, "message": message}
	return hh.ToBytes()
}

func ResultMessage(message string) []byte {
	hh := H{"code": 500, "message": message}
	return hh.ToBytes()
}

func ResultError(err error) []byte {
	hh := H{"code": 500, "message": fmt.Sprint(err)}
	return hh.ToBytes()
}
