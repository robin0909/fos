package cluster

// 监听心跳数据包
func (a *Amqp) heartListen() {
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
			msg := string(d.Body)
			if msg == "" {
				continue
			}
			mutex.Lock()
			dataServers[msg] = time.Now()
			mutex.Unlock()
		}
	}()
}

// 上报自己的心跳包
func (a *Amqp) pushMetaTimer() {
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

// 清除过期的数据
func (s *Server) clearExpiredDataTimer() {
	go func() {
		for {
			<-time.After(time.Second * 5)
			mutex.Lock()
			for s, t := range dataServers {
				if t.Add(10 * time.Second).Before(time.Now()) {
					delete(dataServers, s)
				}
			}
			mutex.Unlock()
		}
	}()
}
