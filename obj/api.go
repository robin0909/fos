// 提供 obj 的 api相关功能
// obj的存储都是以 gzip 的格式

//  api 说明：
//  PUT /objects/<bucket_name>/<obj_name>  		上传一个资源到服务器
//  GET /objects/<bucket_name>/<obj_name>		获取一个网络资源

package obj

import (
	"com.github/robin0909/fos/log"
	"compress/gzip"
	"errors"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
)

type FosServer struct {
	dataDir string // 数据文件存放位置
}

func New(dataDir string) *FosServer {
	return &FosServer{dataDir: dataDir}
}

// 运行 obj api 服务
func (fs *FosServer) RunFos() {
	// restful obj api
	http.HandleFunc("/objects/", fs.handleObjects)
	// restful bucket api
	http.HandleFunc("/bucket/", fs.handleBucket)
	err := http.ListenAndServe(":9000", nil)
	if err != nil {
		log.Warn.Println("服务启动失败", err)
	}
}

// 对象存储相关api
func (fs *FosServer) handleObjects(writer http.ResponseWriter, request *http.Request) {
	method := request.Method

	switch method {
	case http.MethodPut:
		fs.putObj(writer, request)
		return
	case http.MethodGet:
		fs.getObj(writer, request)
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

// put obj
func (fs *FosServer) putObj(writer http.ResponseWriter, request *http.Request) {
	bucketName, objName, err := parseObjMeta(request.URL)
	if err != nil {
		writer.WriteHeader(http.StatusBadRequest)
		return
	}
	f, err := fs.createFile(bucketName, objName)
	if err != nil {
		log.Warn.Println("打开文件失败", err)
		writer.WriteHeader(http.StatusInternalServerError)
		return
	}
	defer f.Close()
	gw := gzip.NewWriter(f)

	io.Copy(gw, request.Body)
	writer.Write(ResultOk())
}

// get obj
func (fs *FosServer) getObj(writer http.ResponseWriter, request *http.Request) {
	bucketName, objName, err := parseObjMeta(request.URL)
	if err != nil {
		writer.WriteHeader(http.StatusBadRequest)
		return
	}
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

// del obj
func (fs *FosServer) delObj(writer http.ResponseWriter, request *http.Request) {
	bucketName, objName, err := parseObjMeta(request.URL)
	if err != nil {
		writer.WriteHeader(http.StatusBadRequest)
		return
	}

	err = os.Remove(fs.dataDir + "/" + bucketName + "/" + objName + ".gzip")
	if err != nil {
		log.Warn.Println("删除obj失败", err)
		writer.WriteHeader(http.StatusInternalServerError)
		writer.Write(ResultMessage("删除失败"))
		return
	}

	writer.Write(ResultOk())
}

func (fs *FosServer) createFile(bucketName, objName string) (*os.File, error) {
	log.Info.Printf("open file bucketName = [%s], objName = [%s]", bucketName, objName)
	f, err := os.Create(fs.dataDir + "/" + bucketName + "/" + objName + ".gzip")
	return f, err
}

func (fs *FosServer) openFile(bucketName, objName string) (*os.File, error) {
	log.Info.Printf("open file bucketName = [%s], objName = [%s]", bucketName, objName)
	f, err := os.Open(fs.dataDir + "/" + bucketName + "/" + objName + ".gzip")
	return f, err
}

func parseObjMeta(url *url.URL) (bucketName, objName string, err error) {
	paths := strings.Split(url.EscapedPath(), "/")
	if len(paths) != 4 {
		err = errors.New("url error")
		return
	}
	bucketName = paths[2]
	objName = paths[3]
	return
}

// bucket 操作相关api
func (fs *FosServer) handleBucket(writer http.ResponseWriter, request *http.Request) {
	method := request.Method
	if method == http.MethodPut || method == http.MethodPost {
		// 创建bucket
		fs.createBucket(writer, request)
	} else if method == http.MethodDelete {
		// 删除bucket
		fs.deleteBucket(writer, request)
	} else {
		writer.WriteHeader(http.StatusNotFound)
	}
}

func (fs *FosServer) createBucket(w http.ResponseWriter, r *http.Request) {
	bucketName, err := parseBucketName(r.URL)
	if err != nil {
		log.Warn.Printf("url error, %s", r.URL)
		w.WriteHeader(http.StatusNotFound)
		return
	}

	err = os.Mkdir(fs.dataDir+"/"+bucketName, 0777)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		log.Warn.Printf("创建bucket失败, buckect=%s, err=%s", bucketName, err)
		w.Write(ResultMessage("创建bucket失败"))
		return
	}

	w.Write(ResultOk())
}

func (fs *FosServer) deleteBucket(w http.ResponseWriter, r *http.Request) {
	bucketName, err := parseBucketName(r.URL)
	if err != nil {
		log.Warn.Printf("url error, %s", r.URL)
		w.WriteHeader(http.StatusNotFound)
		return
	}

	err = os.Remove(fs.dataDir + "/" + bucketName)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		log.Warn.Printf("删除bucket失败, buckect=%s, err=%s", bucketName, err)
		w.Write(ResultMessage("删除bucket失败"))
		return
	}

	w.Write(ResultOk())
}

func parseBucketName(url *url.URL) (string, error) {
	paths := strings.Split(url.EscapedPath(), "/")
	if len(paths) != 3 {
		return "", errors.New("url error")
	}

	return paths[2], nil
}
