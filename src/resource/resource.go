package resource

import (
	"os"
	"path/filepath"
)

// 是否存在 obj resource 资源
func IsExistResourceObj(dataDir, bucket, obj string) bool {
	// filepath.Rel()
	path := filepath.Join(dataDir, bucket, obj+".gzip")
	stat, _ := os.Stat(path)
	if stat != nil && !stat.IsDir() {
		return true
	}

	return false
}
