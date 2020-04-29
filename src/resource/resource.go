package resource

import (
	"com.github/robin0909/fos/src/log"
	"os"
	"path/filepath"
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
