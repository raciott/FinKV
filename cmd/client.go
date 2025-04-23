package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	STRING  byte = '+' // 简单字符串
	ERROR   byte = '-' // 错误消息
	INTEGER byte = ':' // 整数
	BULK    byte = '$' // 批量字符串
	ARRAY   byte = '*' // 数组
)

var (
	CRLF = []byte{'\r', '\n'}
)

// 发送PING命令并接收响应
func sendPing(conn net.Conn, message string) (string, error) {
	// 构建RESP格式的PING命令
	var command string
	if message == "" {
		fmt.Errorf("命令不能为空")
	} else {
		// *2\r\n$4\r\nPING\r\n$<len>\r\n<message>\r\n
		command = fmt.Sprintf("*2\r\n$4\r\nPING\r\n$%d\r\n%s\r\n", len(message), message)
	}

	//log.Println(command)

	// 发送命令
	_, err := conn.Write([]byte(command))
	if err != nil {
		return "", fmt.Errorf("发送命令失败: %v", err)
	}

	return readResponse(conn)
}

// sendSet 发送SET命令到服务器
func sendSet(conn net.Conn, key, value string) (string, error) {
	// 构建RESP格式的SET命令: *3\r\n$3\r\nSET\r\n$<keylen>\r\n<key>\r\n$<valuelen>\r\n<value>\r\n
	command := fmt.Sprintf("*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n",
		len(key), key, len(value), value)

	//log.Println(command)

	// 发送命令
	_, err := conn.Write([]byte(command))
	if err != nil {
		return "", fmt.Errorf("发送命令失败: %v", err)
	}

	return readResponse(conn)
}

// sendDel 发送DEL命令到服务器
func sendDel(conn net.Conn, keys ...string) (string, error) {
	// 构建DEL命令
	cmd := []byte(fmt.Sprintf("*%d\r\n$3\r\nDEL\r\n", len(keys)+1))

	// 添加所有键
	for _, key := range keys {
		cmd = append(cmd, []byte(fmt.Sprintf("$%d\r\n%s\r\n", len(key), key))...)
	}

	// 发送命令
	if _, err := conn.Write(cmd); err != nil {
		return "", err
	}

	// 读取响应
	resp, err := readResponse(conn)
	if err != nil {
		return "", err
	}

	return resp, nil
}

// sendIncr 发送INCR命令到服务器
func sendIncr(conn net.Conn, key string) (string, error) {
	// 构建RESP格式的INCR命令: *2\r\n$4\r\nINCR\r\n$<keylen>\r\n<key>\r\n
	command := fmt.Sprintf("*2\r\n$4\r\nINCR\r\n$%d\r\n%s\r\n",
		len(key), key)

	// 发送命令
	_, err := conn.Write([]byte(command))
	if err != nil {
		return "", fmt.Errorf("发送命令失败: %v", err)
	}

	return readResponse(conn)
}

// sendIncrBy 发送INCRBY命令到服务器
func sendIncrBy(conn net.Conn, key string, increment int) (string, error) {
	// 构建RESP格式的INCRBY命令: *3\r\n$6\r\nINCRBY\r\n$<keylen>\r\n<key>\r\n$<incrementlen>\r\n<increment>\r\n
	incrementStr := strconv.Itoa(increment)
	command := fmt.Sprintf("*3\r\n$6\r\nINCRBY\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n",
		len(key), key, len(incrementStr), incrementStr)

	// 发送命令
	_, err := conn.Write([]byte(command))
	if err != nil {
		return "", fmt.Errorf("发送命令失败: %v", err)
	}

	return readResponse(conn)
}

// readResponse 从连接中读取服务器响应
func readResponse(conn net.Conn) (string, error) {
	// 设置读取超时
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))

	// 读取响应
	reader := bufio.NewReader(conn)
	respType, err := reader.ReadByte()
	if err != nil {
		return "", fmt.Errorf("读取响应类型失败: %v", err)
	}

	//println("读取响应类型:", string(respType))

	switch respType {
	case STRING:
		// 读取简单字符串响应 (+OK\r\n 或 +PONG <message>\r\n)
		line, err := reader.ReadString('\n')
		if err != nil {
			return "", fmt.Errorf("读取响应内容失败: %v", err)
		}
		// 去除CRLF
		return strings.TrimRight(line, "\r\n"), nil
	case ERROR:
		// 读取错误响应
		line, err := reader.ReadString('\n')
		if err != nil {
			return "", fmt.Errorf("读取错误响应失败: %v", err)
		}
		return "", fmt.Errorf("服务器返回错误: %s", strings.TrimRight(line, "\r\n"))
	case INTEGER:
		// 读取整数响应
		line, err := reader.ReadString('\n')
		if err != nil {
			return "", fmt.Errorf("读取整数响应失败: %v", err)
		}
		// 去除CRLF
		return strings.TrimRight(line, "\r\n"), nil
	default:
		return "", fmt.Errorf("未知的响应类型: %c", respType)
	}
}

func main() {
	// 默认连接地址
	addr := "localhost:8911"

	// 如果提供了命令行参数，使用第一个参数作为地址
	if len(os.Args) > 1 {
		addr = os.Args[1]
	}

	fmt.Printf("连接到 %s...\n", addr)

	// 建立TCP连接
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		fmt.Printf("连接失败: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close()

	fmt.Println("连接成功！")

	// 创建一个读取用户输入的scanner
	scanner := bufio.NewScanner(os.Stdin)

	for {
		fmt.Print(">")
		if !scanner.Scan() {
			break
		}

		input := scanner.Text()
		if input == "exit" {
			break
		}

		// 解析输入
		parts := strings.Fields(input)
		if len(parts) == 0 {
			continue
		}

		var response string
		var err error

		// 根据命令类型处理
		switch strings.ToLower(parts[0]) {
		case "ping":
			// 提取message
			var message string
			if len(parts) > 1 {
				message = strings.Join(parts[1:], " ")
			} else {
				message = " "
			}
			// 发送PING命令
			response, err = sendPing(conn, message)

		case "set":
			// 检查参数数量
			if len(parts) < 3 {
				fmt.Println("错误: SET命令格式为 'set <key> <value>'")
				continue
			}
			// 提取key和value
			key := parts[1]
			value := strings.Join(parts[2:], " ")
			// 发送SET命令
			response, err = sendSet(conn, key, value)

		case "get":
			// 检查参数数量
			if len(parts) != 2 {
				fmt.Println("错误: GET命令格式为 'get <key>'")
				continue
			}
			// 提取key
			key := parts[1]
			// 发送GET命令
			response, err = sendGet(conn, key)

		case "del":
			// 检查参数数量
			if len(parts) < 2 {
				fmt.Println("错误: DEL命令格式为 'del <key> [<key>...]'")
				continue
			}
			// 提取所有键
			keys := parts[1:]
			// 发送DEL命令
			response, err = sendDel(conn, keys...)

		case "incr":
			// 检查参数数量
			if len(parts) != 2 {
				fmt.Println("错误: INCR命令格式为 'incr <key>'")
				continue
			}
			// 提取key
			key := parts[1]
			// 发送INCR命令
			response, err = sendIncr(conn, key)

		case "incrby":
			// 检查参数数量
			if len(parts) != 3 {
				fmt.Println("错误: INCRBY命令格式为 'incrby <key> <increment>'")
				continue
			}
			// 提取key和increment
			key := parts[1]
			increment, err := strconv.Atoi(parts[2])
			if err != nil {
				fmt.Println("错误: increment必须是整数")
				continue
			}
			// 发送INCRBY命令
			response, err = sendIncrBy(conn, key, increment)

		default:
			fmt.Printf("错误: 未知命令 '%s'\n", parts[0])
			continue
		}

		if err != nil {
			fmt.Printf("错误: %v\n", err)
			continue
		}

		fmt.Printf("%s\n", response)
	}

	fmt.Println("客户端已关闭")
}

func sendGet(conn net.Conn, key string) (string, error) {
	// 构建RESP格式的GET命令: *2\r\n$3\r\nGET\r\n$<keylen>\r\n<key>\r\n
	command := fmt.Sprintf("*2\r\n$3\r\nGET\r\n$%d\r\n%s\r\n",
		len(key), key)

	//log.Println(command)

	// 发送命令
	_, err := conn.Write([]byte(command))
	if err != nil {
		return "", fmt.Errorf("发送命令失败: %v", err)
	}

	return readResponse(conn)
}
