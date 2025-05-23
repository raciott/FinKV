package main

import (
	"FinKV/config" // 引入配置文件模块
	"FinKV/database"
	"FinKV/network/server" // 引入网络服务模块
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"syscall"
)

// 打印专属logo
func printLogo() {
	filePath := "./logo.txt"
	file, err := os.Open(filePath) // os打开文件
	if err != nil {
		log.Fatal(err) // 文件打开失败、错误时终止程序
	}

	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			log.Fatal(err)
		}
	}(file) // 函数退出时关闭文件

	logo, err := io.ReadAll(file) // 读取文件
	if err != nil {
		log.Fatal(err) // 文件读取失败、错误时终止程序
	}

	fmt.Println(string(logo))

}

func main() {

	confPath := flag.String("conf", "./conf.yaml", "path to conf file") // 配置文件路径
	raftID := flag.String("raftID", "node1", "raft id")
	raftAddr := flag.String("raft_addr", "", "raft listen address") // 默认单机模式启动
	joinAddr := flag.String("join_addr", "", "join raft cluster")

	flag.Parse() // 解析命令行参数

	if _, err := os.Stat(*confPath); os.IsNotExist(err) {
		log.Fatal("conf file not exist")
	}

	// 初始化配置文件
	err := config.Init(*confPath)
	if err != nil {
		log.Fatal(err)
	}

	// 创建数据库实例
	db := database.NewFincasDB(config.Get().Base.DataDir) // 使用指定的数据目录初始化数据库
	defer db.Close()                                      // 确保程序退出前关闭数据库

	srv, err := server.New(db, &config.Get().Network.Addr)
	if err != nil {
		log.Fatal(err)
	}

	printLogo()

	// 设置信号处理，用于优雅关闭
	sigCh := make(chan os.Signal, 1)                      // 创建信号通道
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM) // 监听中断和终止信号

	// 在后台启动服务器
	go func() {
		if err := srv.Start(*raftAddr, *raftID, *joinAddr); err != nil {
			log.Fatal(err) // 服务器启动,失败，记录错误并退出
		}
	}()

	// 等待接收终止信号
	<-sigCh

	// 停止服务器
	if err := srv.Stop(); err != nil {
		log.Printf("Error shutting down: %v", err) // 关闭过程中出现错误，记录但不退出
	}

	log.Println("Shutting down...") // 收到信号，开始关闭服务

}
