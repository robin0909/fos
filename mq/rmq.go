package mq

import (
	"com.github/robin0909/fos/log"
	"fmt"
	"github.com/streadway/amqp"
	"time"
)

type Consumer interface {
	Consume(msg string)
}

const (
	mqUrl    = "amqp://guest:guest@127.0.0.1:5672/test"
	exchange = "test.go.fos.exchange"
)

var channel *amqp.Channel

func init() {
	conn()
}

// 建立连接
func conn() {
	conn, err := amqp.Dial(mqUrl)
	log.FailOnErr(err, "建立连接失败 rabbitmq")

	channel, err = conn.Channel()
	log.FailOnErr(err, "打开通道失败 rabbitmq")
}

func Push(msg string) {
	err := channel.Publish(exchange, "fos.test.queue.one", false, false, amqp.Publishing{
		ContentType: "text/plain",
		Body:        []byte(msg),
	})
	log.FailOnWarn(err, "消息发送失败")
}

func Receive(c Consumer) {

	queueName := "fos.test.queue.1"
	channel.QueueDeclare(queueName, true, true, false, false, nil)
	// 将 queue bind 到  exchange , key 为 "fos.test.queue.*"
	channel.QueueBind(queueName, "fos.test.queue.*", exchange, false, nil)
	consume, err := channel.Consume(queueName, "", true, false, false, false, nil)
	log.FailOnErr(err, "消费消息失败")

	go func() {
		<-time.After(time.Second * 10)
		channel.Close()
	}()

	go func() {

		defer channel.Close()

		for d := range consume {
			c.Consume(string(d.Body))
		}

		fmt.Println("consume end")
	}()
}
