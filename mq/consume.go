package mq

import "com.github/robin0909/fos/log"

type DefaultConsumer struct{}

func (c *DefaultConsumer) Consume(msg string) {
	log.Info.Printf("msg: %s", msg)
}
