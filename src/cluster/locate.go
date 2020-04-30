package cluster

import (
	"com.github/robin0909/fos/src/log"
	"com.github/robin0909/fos/src/resource"
	"com.github/robin0909/fos/src/utils"
	"encoding/json"
	"github.com/rs/xid"
	"github.com/streadway/amqp"
	"strings"
	"sync"
	"time"
)

type Locate struct {
	Id             string
	Bucket         string
	Obj            string
	Address        string
	LocateRouteKey string
}

// 随机id 用于确定资源请求的id
var sourceChannels = make(map[string]chan<- string)
var sourceMutex sync.Mutex

// 一旦别的节点找到资源就push locate queue
var locateQueue = "locate-" + xid.New().String()

// 用于广播的队列，接收需要定位的资源
var locateBroadcastQueue = "locate-broadcast-" + xid.New().String()

var locateBakRandomKey = "locate." + xid.New().String() + "route.key"

func (s *Server) runLocate() {
	// 监听查找到资源信息
	s.Amq.locateListen()
	// 监听广播查找资源消息
	s.Amq.locateBroadcastListen(s.dataDir, s.address)
}

// 从集群中寻找 obj 资源
func FindClusterResource(cs *Server, bucketName, objName string) (address string) {

	id := xid.New().String()
	var addressChan = make(chan string)
	// 设置 30s 超时时间
	var timeoutChan = time.After(time.Second * 30)
	cs.LocateSource(id, bucketName, objName, addressChan)

	select {
	case <-timeoutChan:
		// 在30s 内未拿到数据，超时结束，默认没有定位到资源
	case address = <-addressChan:
		// 定位到资源
	}
	RemoveIdSource(id)
	close(addressChan)
	return
}

// 定位资源在那个节点
func (s *Server) LocateSource(id, bucket, obj string, ch chan string) {
	addIdSource(id, ch)
	var locate = Locate{
		Id:             id,
		Bucket:         bucket,
		Obj:            obj,
		LocateRouteKey: locateBakRandomKey,
	}
	s.Amq.pushBroadcastLocate(locate)
}

// 定位资源的时候，需要加入 sourceChannels 进行等待
func addIdSource(id string, ch chan string) {
	sourceMutex.Lock()
	sourceChannels[id] = ch
	sourceMutex.Unlock()
}

// 监听到资源
// 走的事一对一模式，一旦别的节点查到数据，直接向 locateQueue push 数据
func (a *Amqp) locateListen() {
	_, err := a.channel.QueueDeclare(locateQueue, true, true, false, false, nil)
	log.FailOnErr(err, "创建locate队列失败")

	err = a.channel.QueueBind(locateQueue, locateBakRandomKey, a.LocateBackExchange, false, nil)
	log.FailOnErr(err, "locate队列绑定到exchange失败")

	consume, err := a.channel.Consume(locateQueue, "", true, false, false, false, nil)
	log.FailOnErr(err, "获取locate消费的channel失败")

	go func() {
		defer a.channel.Close()
		for d := range consume {
			var locate Locate
			_ = json.Unmarshal(d.Body, &locate)
			log.Info.Println("收到定位成功的消息 locate: ", locate)
			ch := sourceChannels[locate.Id]
			if ch != nil {
				ch <- locate.Address
			}
			RemoveIdSource(locate.Id)
		}
	}()
}

// 发送定位消息
// 一旦在本机找到资源，就返回定位
func (a *Amqp) pushLocate(locate Locate) {
	bytes, _ := json.Marshal(locate)
	err := a.channel.Publish(a.LocateBackExchange, locate.LocateRouteKey, false, false, amqp.Publishing{
		ContentType: "text/plain",
		Body:        bytes,
	})
	log.FailOnWarn(err, "发送资源定位back消息失败")
}

// 监听需要被定位的资源
// 走广播方式，一旦受到就在自己服务器上查找资源，一旦找到就返回给对应的locateQueue
func (a *Amqp) locateBroadcastListen(dataDir, address string) {

	_, err := a.channel.QueueDeclare(locateBroadcastQueue, true, true, false, false, nil)
	log.FailOnErr(err, "创建locate broadcast 队列失败")

	err = a.channel.QueueBind(locateBroadcastQueue, a.LocateRouteKey, a.LocateExchange, false, nil)
	log.FailOnErr(err, "locate broadcast 队列绑定到exchange失败")

	consume, err := a.channel.Consume(locateBroadcastQueue, "", true, false, false, false, nil)
	log.FailOnErr(err, "获取locate消费的channel失败")

	go func() {
		defer a.channel.Close()
		for d := range consume {
			var locate Locate
			_ = json.Unmarshal(d.Body, &locate)
			if locate.LocateRouteKey == locateBakRandomKey {
				continue
			}
			log.Info.Println("收到广播定位消息 locate: ", locate)
			if resource.IsExistResourceObj(dataDir, locate.Bucket, locate.Obj) {
				log.Info.Println("在本主机上定位到资源 locate: ", locate)
				locate.Address = strings.Join([]string{utils.GetLocalIp(), utils.GetPort(address)}, ":")
				a.pushLocate(locate)
			}
		}
	}()

}

// 广播寻找资源
func (a *Amqp) pushBroadcastLocate(locate Locate) {
	bytes, _ := json.Marshal(locate)
	err := a.channel.Publish(a.LocateExchange, a.LocateRouteKey, false, false, amqp.Publishing{
		ContentType: "text/plain",
		Body:        bytes,
	})
	log.FailOnWarn(err, "发送广播资源定位消息失败")
}

// 移除资源释放内存
func RemoveIdSource(id string) {
	sourceMutex.Lock()
	delete(sourceChannels, id)
	sourceMutex.Unlock()
}
