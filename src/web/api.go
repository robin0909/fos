// 提供 web 的 api相关功能
// obj的存储都是以 gzip 的格式

// bucket 应该是放在数据库，做权限判断使用

//  api 说明：
//  PUT /objects/<bucket_name>/<obj_name>  		上传一个资源到服务器
//  GET /objects/<bucket_name>/<obj_name>		获取一个网络资源

package web

import (
	"com.github/robin0909/fos/src/cluster"
	"com.github/robin0909/fos/src/log"
	"com.github/robin0909/fos/src/resource"
	"com.github/robin0909/fos/src/result"
	"com.github/robin0909/fos/src/stream"
	"compress/gzip"
	"errors"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
)

type FosServer struct {
	dataDir       string          // 数据文件存放位置
	address       string          // 本服务地址
	clusterServer *cluster.Server // 集群服务
}

func New(dataDir, address string, clusterServer *cluster.Server) *FosServer {
	return &FosServer{dataDir: dataDir, address: address, clusterServer: clusterServer}
}

// 运行 web api 服务
func (fs *FosServer) RunWeb() {
	// restful web api
	http.HandleFunc("/objects/", fs.objectsHandle)
	err := http.ListenAndServe(fs.address, nil)
	if err != nil {
		log.Warn.Println("服务启动失败", err)
	}
}

// 对象存储相关api
func (fs *FosServer) objectsHandle(writer http.ResponseWriter, request *http.Request) {
	method := request.Method
	switch method {
	case http.MethodPut:
		fs.putObj(writer, request)
		return
	case http.MethodGet:
		// fs.getLocalObj(writer, request)
		fs.getGlobeObj(writer, request)
		return
	case http.MethodDelete:
		fs.delObj(writer, request)
		return
	default:
		log.Warn.Println("不支持的请求方法", method)
		writer.WriteHeader(http.StatusNotFound)
		return
	}
}

// put 文件对象
func (fs *FosServer) putObj(writer http.ResponseWriter, request *http.Request) {
	bucketName, objName, err := parseUrlMeta(request.URL)
	if err != nil {
		writer.WriteHeader(http.StatusBadRequest)
		return
	}

	fs.createBucket(bucketName)
	f, err := fs.createFile(bucketName, objName)
	if err != nil {
		log.Warn.Println("打开文件失败", err)
		writer.WriteHeader(http.StatusInternalServerError)
		return
	}
	defer f.Close()
	gw := gzip.NewWriter(f)

	_, _ = io.Copy(gw, request.Body)
	_, _ = writer.Write(result.ResultOk())
}

// 在整个集群里寻找 obj
func (fs *FosServer) getGlobeObj(writer http.ResponseWriter, request *http.Request) {
	bucketName, objName, err := parseUrlMeta(request.URL)
	if err != nil {
		writer.WriteHeader(http.StatusBadRequest)
		return
	}

	// 本地是否存在
	if resource.IsExistResourceObj(fs.dataDir, bucketName, objName) {
		fs.getLocalObj(writer, bucketName, objName)
	} else {
		// 如果本地不存在就去远程查找
		fs.getRemoteObj(writer, bucketName, objName)
	}

}

// get 文件对象
func (fs *FosServer) getLocalObj(writer http.ResponseWriter, bucketName, objName string) {

	f, err := fs.openFile(bucketName, objName)
	if err != nil {
		log.Warn.Println("打开文件失败", err)
		writer.WriteHeader(http.StatusNotFound)
		return
	}
	defer f.Close()

	gr, err := gzip.NewReader(f)
	if err != nil {
		writer.WriteHeader(http.StatusNotFound)
		return
	}
	io.Copy(writer, gr)
}

// 获取远程的资源
func (fs *FosServer) getRemoteObj(writer http.ResponseWriter, bucketName, objName string) {

	address := cluster.FindClusterResource(fs.clusterServer, bucketName, objName)
	if address == "" {
		writer.WriteHeader(http.StatusNotFound)
		return
	}
	// 去远程调取资源
	getStream, err := stream.NewGetStream(address, bucketName, objName)
	if err != nil {
		log.FailOnWarn(err, "获取远程的数据流失败")
		writer.WriteHeader(http.StatusInternalServerError)
		return
	}

	io.Copy(writer, getStream)
}

// del 文件对象
func (fs *FosServer) delObj(writer http.ResponseWriter, request *http.Request) {
	bucketName, objName, err := parseUrlMeta(request.URL)
	if err != nil {
		writer.WriteHeader(http.StatusBadRequest)
		return
	}

	err = os.Remove(filepath.Join(fs.dataDir, bucketName, objName+".gzip"))
	if err != nil {
		log.Warn.Println("删除obj失败", err)
		writer.WriteHeader(http.StatusInternalServerError)
		writer.Write(result.ResultMessage("删除失败"))
		return
	}

	_, _ = writer.Write(result.ResultOk())
}

// 在 bucket 下创建一个文件
func (fs *FosServer) createFile(bucketName, objName string) (*os.File, error) {
	log.Info.Printf("open file bucketName = [%s], objName = [%s]", bucketName, objName)
	f, err := os.Create(filepath.Join(fs.dataDir, bucketName, objName+".gzip"))
	return f, err
}

// 打开一个 bucket 下的文件
func (fs *FosServer) openFile(bucketName, objName string) (*os.File, error) {
	log.Info.Printf("open file bucketName = [%s], objName = [%s]", bucketName, objName)
	f, err := os.Open(filepath.Join(fs.dataDir, bucketName, objName+".gzip"))
	return f, err
}

// 解析 url 中的参数
func parseUrlMeta(url *url.URL) (bucketName, objName string, err error) {
	paths := strings.Split(url.EscapedPath(), "/")
	if len(paths) != 4 {
		err = errors.New("url error")
		return
	}
	bucketName = paths[2]
	objName = paths[3]
	return
}

// 创建 bucket
// 如果存在就什么都不做
func (fs *FosServer) createBucket(bucket string) {
	path := filepath.Join(fs.dataDir, bucket)
	fileInfo, err := os.Stat(path)
	if os.IsNotExist(err) || !fileInfo.IsDir() {
		err = os.Mkdir(path, 0777)
		log.FailOnWarn(err, "创建bucket失败")
	}
}
