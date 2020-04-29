package main

import (
	"com.github/robin0909/fos/src/cluster"
	"com.github/robin0909/fos/src/log"
	"com.github/robin0909/fos/src/web"
	"flag"
)

var (
	dataDir    string
	serverType string // store or web
	configFile string
	address    string // 0.0.0.0:8080
)

func main() {
	parseFlag()
	startFos()
}

func parseFlag() {
	flag.StringVar(&dataDir, "dataDir", "/tmp/data", "配置服务文件数据目录")
	flag.StringVar(&serverType, "serverType", "store", "服务类型（web or store）")
	flag.StringVar(&configFile, "configFile", "./config.yml", "配置文件路径")
	flag.StringVar(&address, "address", ":8080", "服务器主机地址和端口")
	// 解析
	flag.Parse()

	log.Info.Printf(`
	fos server config list:
	dataDir:       %s
	serverType:    %s
	configFile:    %s
	address:       %s`,
		dataDir, serverType, configFile, address)
}

func startFos() {
	// 服务心跳检测
	c := cluster.New(configFile, address, dataDir)
	c.Start()

	// 启动 web api
	server := web.New(dataDir, address, c)
	server.RunWeb()
}
