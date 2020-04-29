package cluster

import (
	"com.github/robin0909/fos/src/log"
	"github.com/streadway/amqp"
	"gopkg.in/yaml.v2"
	"io/ioutil"
)

type Server struct {
	Amq     Amqp `yaml:"amqp"`
	address string
	dataDir string
}

type Amqp struct {
	Uri                string `yaml:"uri"`
	HeartExchange      string `yaml:"heartExchange"`
	HeartRouteKey      string `yaml:"heartRouteKey"`
	LocateExchange     string `yaml:"locateExchange"`
	LocateBackExchange string `yaml:"locateBackExchange"`
	LocateRouteKey     string `yaml:"locateRouteKey"`

	connection *amqp.Connection
	channel    *amqp.Channel
}

func New(configPath string, address, dataDir string) *Server {
	file, err := ioutil.ReadFile(configPath)
	log.FailOnErr(err, "读取config文件出错")

	var s Server
	err = yaml.Unmarshal(file, &s)
	log.FailOnErr(err, "config yaml 解析出错")

	s.address = address
	s.dataDir = dataDir
	return &s
}

func (s *Server) Start() {
	// 打印mq相关信息
	s.Amq.printAmqp()
	// 连接mq
	s.Amq.conn()
	// 启动集群心跳服务
	s.runHeart()
	// 启动集群广播资源定位服务
	s.runLocate()
}

// 建立连接
func (a *Amqp) conn() {
	var err error
	a.connection, err = amqp.Dial(a.Uri)
	log.FailOnErr(err, "建立连接失败 rabbitmq")
	a.channel, err = a.connection.Channel()
	log.FailOnErr(err, "打开通道失败 rabbitmq")
}

// 打印 amqp 信息
func (a *Amqp) printAmqp() {
	log.Info.Printf(`
	amqp config info meta:
	uri:              %s
	heartExchange:    %s
	heartRouteKey:    %s
	locateExchange:   %s
	locateRouteKey:   %s
	heartQueue:       %s`,
		a.Uri, a.HeartExchange, a.HeartRouteKey, a.LocateExchange, a.LocateRouteKey, heartQueue)
}
