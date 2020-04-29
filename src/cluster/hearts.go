package cluster

import (
	"com.github/robin0909/fos/src/log"
	"com.github/robin0909/fos/src/utils"
	"github.com/rs/xid"
	"github.com/streadway/amqp"
	"sync"
	"time"
)

var dataServers = make(map[string]time.Time)
var dataMutex sync.Mutex

var heartQueue = "heart-" + xid.New().String()

func (s *Server) runHeart() {
	// 监听上报的节点信息
	s.Amq.heartListen()
	// 定时上报自己的节点信息
	s.Amq.pushMetaTimer(s.address)
	// 清除过时节点的信息
	clearExpiredDataTimer()
}

// 监听心跳数据包
func (a *Amqp) heartListen() {
	// 声明临时 queue
	_, err := a.channel.QueueDeclare(heartQueue, true, true, false, false, nil)
	log.FailOnErr(err, "创建heart队列失败")

	// 将 queue bind 到  exchange , key 为 "fos.test.queue.*"
	err = a.channel.QueueBind(heartQueue, a.HeartRouteKey, a.HeartExchange, false, nil)
	log.FailOnErr(err, "heart队列绑定到exchange失败")

	consume, err := a.channel.Consume(heartQueue, "", true, false, false, false, nil)
	log.FailOnErr(err, "获取heart消费的channel失败")

	go func() {
		defer a.channel.Close()
		for d := range consume {
			msg := string(d.Body)
			if msg == "" {
				continue
			}
			dataMutex.Lock()
			dataServers[msg] = time.Now()
			dataMutex.Unlock()
		}
	}()
}

// 上报自己的心跳包
func (a *Amqp) pushMetaTimer(address string) {
	go func() {
		for {
			<-time.After(time.Second * 2)
			localIp := utils.GetLocalIp()
			port := utils.GetPort(address)
			err := a.channel.Publish(a.HeartExchange, a.HeartRouteKey, false, false, amqp.Publishing{
				ContentType: "text/plain",
				Body:        []byte(localIp + ":" + port),
			})
			log.FailOnWarn(err, "消息发送失败")
		}
	}()
}

// 清除过期的数据
func clearExpiredDataTimer() {
	go func() {
		for {
			<-time.After(time.Second * 5)
			dataMutex.Lock()
			for s, t := range dataServers {
				if t.Add(10 * time.Second).Before(time.Now()) {
					delete(dataServers, s)
				}
			}
			dataMutex.Unlock()
		}
	}()
}
