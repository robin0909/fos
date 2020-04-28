package resource

import (
	"os"
	"path/filepath"
)

// 是否存在 obj resource 资源
func IsExistResourceObj(dataDir, bucket, obj string) bool {
	// filepath.Rel()
	path := filepath.Join(dataDir, bucket, obj)
	stat, err := os.Stat(path)
	if os.IsExist(err) && !stat.IsDir() {
		return true
	}
	return false
}
