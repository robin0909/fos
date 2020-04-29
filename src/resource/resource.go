package resource

import (
	"com.github/robin0909/fos/src/cluster"
	"com.github/robin0909/fos/src/log"
	"github.com/rs/xid"
	"os"
	"path/filepath"
	"time"
)

// 是否存在 obj resource 资源
func IsExistResourceObj(dataDir, bucket, obj string) bool {
	// filepath.Rel()
	path := filepath.Join(dataDir, bucket, obj+".gzip")
	stat, _ := os.Stat(path)
	if stat != nil && !stat.IsDir() {
		log.Info.Println("定位资源成功 path: ", path)
		return true
	}

	log.Info.Println("定位资源失败 path: ", path)
	return false
}

// 从集群中寻找 obj 资源
func FindClusterResource(cs *cluster.Server, bucketName, objName string) (address string) {

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
	cluster.RemoveIdSource(id)
	close(addressChan)
	return
}
