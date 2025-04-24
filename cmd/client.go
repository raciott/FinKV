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

// sendSetNX 发送SETNX命令到服务器
func sendSetNX(conn net.Conn, key, value string) (string, error) {
	// 构建RESP格式的SETNX命令: *3\r\n$5\r\nSETNX\r\n$<keylen>\r\n<key>\r\n$<valuelen>\r\n<value>\r\n
	command := fmt.Sprintf("*3\r\n$5\r\nSETNX\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n",
		len(key), key, len(value), value)

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

// sendDecr 发送DECR命令到服务器
func sendDecr(conn net.Conn, key string) (string, error) {
	// 构建RESP格式的DECR命令: *2\r\n$4\r\nDECR\r\n$<keylen>\r\n<key>\r\n
	command := fmt.Sprintf("*2\r\n$4\r\nDECR\r\n$%d\r\n%s\r\n",
		len(key), key)

	// 发送命令
	_, err := conn.Write([]byte(command))
	if err != nil {
		return "", fmt.Errorf("发送命令失败: %v", err)
	}

	return readResponse(conn)
}

// sendDecrBy 发送DECRBY命令到服务器
func sendDecrBy(conn net.Conn, key string, decrement int) (string, error) {
	// 构建RESP格式的DECRBY命令: *3\r\n$6\r\nDECRBY\r\n$<keylen>\r\n<key>\r\n$<decrementlen>\r\n<decrement>\r\n
	decrementStr := strconv.Itoa(decrement)
	command := fmt.Sprintf("*3\r\n$6\r\nDECRBY\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n",
		len(key), key, len(decrementStr), decrementStr)

	// 发送命令
	_, err := conn.Write([]byte(command))
	if err != nil {
		return "", fmt.Errorf("发送命令失败: %v", err)
	}

	return readResponse(conn)
}

// sendAppend 发送APPEND命令到服务器
func sendAppend(conn net.Conn, key, value string) (string, error) {
	// 构建RESP格式的APPEND命令: *3\r\n$6\r\nAPPEND\r\n$<keylen>\r\n<key>\r\n$<valuelen>\r\n<value>\r\n
	command := fmt.Sprintf("*3\r\n$6\r\nAPPEND\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n",
		len(key), key, len(value), value)

	// 发送命令
	_, err := conn.Write([]byte(command))
	if err != nil {
		return "", fmt.Errorf("发送命令失败: %v", err)
	}

	return readResponse(conn)
}

// sendGetSet 发送GETSET命令到服务器
func sendGetSet(conn net.Conn, key, value string) (string, error) {
	// 构建RESP格式的GETSET命令: *3\r\n$6\r\nGETSET\r\n$<keylen>\r\n<key>\r\n$<valuelen>\r\n<value>\r\n
	command := fmt.Sprintf("*3\r\n$6\r\nGETSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n",
		len(key), key, len(value), value)

	// 发送命令
	_, err := conn.Write([]byte(command))
	if err != nil {
		return "", fmt.Errorf("发送命令失败: %v", err)
	}

	return readResponse(conn)
}

// sendMSet 发送MSET命令到服务器
func sendMSet(conn net.Conn, kvPairs map[string]string) (string, error) {
	// 计算参数数量：命令名 + 2*键值对数量
	argCount := 1 + 2*len(kvPairs)

	// 构建RESP格式的MSET命令开头: *<argCount>\r\n$4\r\nMSET\r\n
	command := fmt.Sprintf("*%d\r\n$4\r\nMSET\r\n", argCount)

	// 添加所有键值对
	for k, v := range kvPairs {
		command += fmt.Sprintf("$%d\r\n%s\r\n$%d\r\n%s\r\n", len(k), k, len(v), v)
	}

	// 发送命令
	_, err := conn.Write([]byte(command))
	if err != nil {
		return "", fmt.Errorf("发送命令失败: %v", err)
	}

	return readResponse(conn)
}

// sendMGet 发送MGET命令到服务器
func sendMGet(conn net.Conn, keys ...string) (string, error) {
	// 计算参数数量：命令名 + 键数量
	argCount := 1 + len(keys)

	// 构建RESP格式的MGET命令开头: *<argCount>\r\n$4\r\nMGET\r\n
	command := fmt.Sprintf("*%d\r\n$4\r\nMGET\r\n", argCount)

	// 添加所有键
	for _, key := range keys {
		command += fmt.Sprintf("$%d\r\n%s\r\n", len(key), key)
	}

	// 发送命令
	_, err := conn.Write([]byte(command))
	if err != nil {
		return "", fmt.Errorf("发送命令失败: %v", err)
	}

	return readResponse(conn)
}

// sendStrLen 发送STRLEN命令到服务器
func sendStrLen(conn net.Conn, key string) (string, error) {
	// 构建RESP格式的STRLEN命令: *2\r\n$6\r\nSTRLEN\r\n$<keylen>\r\n<key>\r\n
	command := fmt.Sprintf("*2\r\n$6\r\nSTRLEN\r\n$%d\r\n%s\r\n",
		len(key), key)

	// 发送命令
	_, err := conn.Write([]byte(command))
	if err != nil {
		return "", fmt.Errorf("发送命令失败: %v", err)
	}

	return readResponse(conn)
}

// sendLPush 发送LPUSH命令到服务器
func sendLPush(conn net.Conn, key string, values ...string) (string, error) {
	// 计算参数数量：命令名 + 键 + 值的数量
	argCount := 2 + len(values)

	// 构建RESP格式的LPUSH命令开头: *<argCount>\r\n$5\r\nLPUSH\r\n$<keylen>\r\n<key>\r\n
	command := fmt.Sprintf("*%d\r\n$5\r\nLPUSH\r\n$%d\r\n%s\r\n", argCount, len(key), key)

	// 添加所有值
	for _, value := range values {
		command += fmt.Sprintf("$%d\r\n%s\r\n", len(value), value)
	}

	// 发送命令
	_, err := conn.Write([]byte(command))
	if err != nil {
		return "", fmt.Errorf("发送命令失败: %v", err)
	}

	return readResponse(conn)
}

// sendRPush 发送RPUSH命令到服务器
func sendRPush(conn net.Conn, key string, values ...string) (string, error) {
	// 计算参数数量：命令名 + 键 + 值的数量
	argCount := 2 + len(values)

	// 构建RESP格式的RPUSH命令开头: *<argCount>\r\n$5\r\nRPUSH\r\n$<keylen>\r\n<key>\r\n
	command := fmt.Sprintf("*%d\r\n$5\r\nRPUSH\r\n$%d\r\n%s\r\n", argCount, len(key), key)

	// 添加所有值
	for _, value := range values {
		command += fmt.Sprintf("$%d\r\n%s\r\n", len(value), value)
	}

	// 发送命令
	_, err := conn.Write([]byte(command))
	if err != nil {
		return "", fmt.Errorf("发送命令失败: %v", err)
	}

	return readResponse(conn)
}

// sendLPop 发送LPOP命令到服务器
func sendLPop(conn net.Conn, key string) (string, error) {
	// 构建RESP格式的LPOP命令: *2\r\n$4\r\nLPOP\r\n$<keylen>\r\n<key>\r\n
	command := fmt.Sprintf("*2\r\n$4\r\nLPOP\r\n$%d\r\n%s\r\n", len(key), key)

	// 发送命令
	_, err := conn.Write([]byte(command))
	if err != nil {
		return "", fmt.Errorf("发送命令失败: %v", err)
	}

	return readResponse(conn)
}

// sendRPop 发送RPOP命令到服务器
func sendRPop(conn net.Conn, key string) (string, error) {
	// 构建RESP格式的RPOP命令: *2\r\n$4\r\nRPOP\r\n$<keylen>\r\n<key>\r\n
	command := fmt.Sprintf("*2\r\n$4\r\nRPOP\r\n$%d\r\n%s\r\n", len(key), key)

	// 发送命令
	_, err := conn.Write([]byte(command))
	if err != nil {
		return "", fmt.Errorf("发送命令失败: %v", err)
	}

	return readResponse(conn)
}

// sendLLen 发送LLEN命令到服务器
func sendLLen(conn net.Conn, key string) (string, error) {
	// 构建RESP格式的LLEN命令: *2\r\n$4\r\nLLEN\r\n$<keylen>\r\n<key>\r\n
	command := fmt.Sprintf("*2\r\n$4\r\nLLEN\r\n$%d\r\n%s\r\n", len(key), key)

	// 发送命令
	_, err := conn.Write([]byte(command))
	if err != nil {
		return "", fmt.Errorf("发送命令失败: %v", err)
	}

	return readResponse(conn)
}

// sendLRange 发送LRANGE命令到服务器
func sendLRange(conn net.Conn, key string, start, stop int) (string, error) {
	// 构建RESP格式的LRANGE命令: *4\r\n$6\r\nLRANGE\r\n$<keylen>\r\n<key>\r\n$<startlen>\r\n<start>\r\n$<stoplen>\r\n<stop>\r\n
	startStr := strconv.Itoa(start)
	stopStr := strconv.Itoa(stop)
	command := fmt.Sprintf("*4\r\n$6\r\nLRANGE\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n",
		len(key), key, len(startStr), startStr, len(stopStr), stopStr)

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

		case "decr":
			// 检查参数数量
			if len(parts) != 2 {
				fmt.Println("错误: DECR命令格式为 'decr <key>'")
				continue
			}
			// 提取key
			key := parts[1]
			// 发送DECR命令
			response, err = sendDecr(conn, key)

		case "decrby":
			// 检查参数数量
			if len(parts) != 3 {
				fmt.Println("错误: DECRBY命令格式为 'decrby <key> <decrement>'")
				continue
			}
			// 提取key和decrement
			key := parts[1]
			decrement, err := strconv.Atoi(parts[2])
			if err != nil {
				fmt.Println("错误: decrement必须是整数")
				continue
			}
			// 发送DECRBY命令
			response, err = sendDecrBy(conn, key, decrement)

		case "append":
			// 检查参数数量
			if len(parts) < 3 {
				fmt.Println("错误: APPEND命令格式为 'append <key> <value>'")
				continue
			}
			// 提取key和value
			key := parts[1]
			value := strings.Join(parts[2:], " ")
			// 发送APPEND命令
			response, err = sendAppend(conn, key, value)

		case "getset":
			// 检查参数数量
			if len(parts) < 3 {
				fmt.Println("错误: GETSET命令格式为 'getset <key> <value>'")
				continue
			}
			// 提取key和value
			key := parts[1]
			value := strings.Join(parts[2:], " ")
			// 发送GETSET命令
			response, err = sendGetSet(conn, key, value)

		case "setnx":
			// 检查参数数量
			if len(parts) < 3 {
				fmt.Println("错误: SETNX命令格式为 'setnx <key> <value>'")
				continue
			}
			// 提取key和value
			key := parts[1]
			value := strings.Join(parts[2:], " ")
			// 发送SETNX命令
			response, err = sendSetNX(conn, key, value)

		case "mset":
			// 检查参数数量
			if len(parts) < 3 || len(parts)%2 != 1 {
				fmt.Println("错误: MSET命令格式为 'mset <key1> <value1> <key2> <value2> ...'")
				continue
			}
			// 提取所有键值对
			kvPairs := make(map[string]string)
			for i := 1; i < len(parts); i += 2 {
				key := parts[i]
				value := parts[i+1]
				kvPairs[key] = value
			}
			// 发送MSET命令
			response, err = sendMSet(conn, kvPairs)

		case "mget":
			// 检查参数数量
			if len(parts) < 2 {
				fmt.Println("错误: MGET命令格式为 'mget <key1> <key2> ...'")
				continue
			}
			// 提取所有键
			keys := parts[1:]
			// 发送MGET命令
			response, err = sendMGet(conn, keys...)

		case "strlen":
			// 检查参数数量
			if len(parts) != 2 {
				fmt.Println("错误: STRLEN命令格式为 'strlen <key>'")
				continue
			}
			// 提取key
			key := parts[1]
			// 发送STRLEN命令
			response, err = sendStrLen(conn, key)

		case "lpush":
			// 检查参数数量
			if len(parts) < 3 {
				fmt.Println("错误: LPUSH命令格式为 'lpush <key> <value1> [<value2> ...]'")
				continue
			}
			// 提取key和values
			key := parts[1]
			values := parts[2:]
			// 发送LPUSH命令
			response, err = sendLPush(conn, key, values...)

		case "rpush":
			// 检查参数数量
			if len(parts) < 3 {
				fmt.Println("错误: RPUSH命令格式为 'rpush <key> <value1> [<value2> ...]'")
				continue
			}
			// 提取key和values
			key := parts[1]
			values := parts[2:]
			// 发送RPUSH命令
			response, err = sendRPush(conn, key, values...)

		case "lpop":
			// 检查参数数量
			if len(parts) != 2 {
				fmt.Println("错误: LPOP命令格式为 'lpop <key>'")
				continue
			}
			// 提取key
			key := parts[1]
			// 发送LPOP命令
			response, err = sendLPop(conn, key)

		case "rpop":
			// 检查参数数量
			if len(parts) != 2 {
				fmt.Println("错误: RPOP命令格式为 'rpop <key>'")
				continue
			}
			// 提取key
			key := parts[1]
			// 发送RPOP命令
			response, err = sendRPop(conn, key)

		case "llen":
			// 检查参数数量
			if len(parts) != 2 {
				fmt.Println("错误: LLEN命令格式为 'llen <key>'")
				continue
			}
			// 提取key
			key := parts[1]
			// 发送LLEN命令
			response, err = sendLLen(conn, key)

		case "lrange":
			// 检查参数数量
			if len(parts) != 4 {
				fmt.Println("错误: LRANGE命令格式为 'lrange <key> <start> <stop>'")
				continue
			}
			// 提取key和范围
			key := parts[1]
			start, err := strconv.Atoi(parts[2])
			if err != nil {
				fmt.Println("错误: start必须是整数")
				continue
			}
			stop, err := strconv.Atoi(parts[3])
			if err != nil {
				fmt.Println("错误: stop必须是整数")
				continue
			}
			// 发送LRANGE命令
			response, err = sendLRange(conn, key, start, stop)

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
