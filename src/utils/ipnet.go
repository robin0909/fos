package utils

import (
	"com.github/robin0909/fos/src/log"
	"net"
	"strings"
)

const ()

func GetLocalIp() string {

	addrs, err := net.InterfaceAddrs()
	log.FailOnErr(err, "huo获取网卡信息失败")

	var ip string
	for _, address := range addrs {

		ipnet, ok := address.(*net.IPNet)
		if !ok || ipnet.IP.IsLoopback() {
			// 检查ip地址判断是否回环地址
			continue
		}

		if ipnet.IP.To4() != nil {

			if ip == "" {
				ip = ipnet.IP.String()
			} else if !strings.HasPrefix(ipnet.IP.String(), "192.168.") {
				ip = ipnet.IP.String()
			}
		}
	}

	return ip
}

func GetPort(address string) string {
	_, port, err := net.SplitHostPort(address)
	log.FailOnErr(err, "解析 address 出错")
	return port
}
