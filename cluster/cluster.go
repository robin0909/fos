package cluster

import (
	"com.github/robin0909/fos/log"
	"com.github/robin0909/fos/utils"
	"github.com/rs/xid"
	"github.com/streadway/amqp"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"time"
)

type server struct {
	Amq Amqp `yaml:"amqp"`
}

var nodeQueue = "heart-" + xid.New().String()

type Amqp struct {
	Uri      string `yaml:"uri"`
	Exchange string `yaml:"exchange"`
	RouteKey string `yaml:"routeKey"`

	address string

	connection *amqp.Connection
	channel    *amqp.Channel
}

func New(configPath string, address string) *Amqp {
	file, err := ioutil.ReadFile(configPath)
	log.FailOnErr(err, "读取config文件出错")

	var s server
	err = yaml.Unmarshal(file, &s)
	log.FailOnErr(err, "config yaml 解析出错")

	s.Amq.conn()
	s.Amq.address = address
	return &s.Amq
}

// 建立连接
func (a *Amqp) conn() {
	a.printAmqp()

	var err error
	a.connection, err = amqp.Dial(a.Uri)
	log.FailOnErr(err, "建立连接失败 rabbitmq")
	a.channel, err = a.connection.Channel()
	log.FailOnErr(err, "打开通道失败 rabbitmq")
}

func (a *Amqp) printAmqp() {
	log.Info.Printf(`
	amqp config info meta:
	uri:		%s
	exchange:	%s
	routeKey:	%s
	queue:		%s`,
		a.Uri, a.Exchange, a.RouteKey, nodeQueue)
}

func (a *Amqp) HeartListen(c Consumer) {

	// 声明临时 queue
	_, err := a.channel.QueueDeclare(nodeQueue, true, true, false, false, nil)
	log.FailOnErr(err, "创建队列失败")

	// 将 queue bind 到  exchange , key 为 "fos.test.queue.*"
	err = a.channel.QueueBind(nodeQueue, a.RouteKey, a.Exchange, false, nil)
	log.FailOnErr(err, "队列绑定到exchange失败")

	consume, err := a.channel.Consume(nodeQueue, "", true, false, false, false, nil)
	log.FailOnErr(err, "获取消费的channel失败")

	go func() {
		defer a.channel.Close()
		for d := range consume {
			c.Consume(string(d.Body))
		}
	}()
}

func (a *Amqp) PushMetaTimer() {
	go func() {
		for {
			<-time.After(time.Second * 2)
			localIp := utils.GetLocalIp()
			port := utils.GetPort(a.address)
			err := a.channel.Publish(a.Exchange, a.RouteKey, false, false, amqp.Publishing{
				ContentType: "text/plain",
				Body:        []byte(localIp + ":" + port),
			})
			log.FailOnWarn(err, "消息发送失败")
		}
	}()
}
