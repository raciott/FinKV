package protocol

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"log"
	"strconv"
)

const (
	STRING  byte = '+' // 简单字符串
	ERROR   byte = '-' // 错误消息
	INTEGER byte = ':' // 整数
	BULK    byte = '$' // 批量字符串
	ARRAY   byte = '*' // 数组
)

var (
	ErrInvalidRESP = errors.New("invalid RESP")
	CRLF           = []byte{'\r', '\n'}
)

type Command struct {
	Name string
	Args [][]byte
}

type Parser struct {
	reader *bufio.Reader
}

type Writer struct {
	writer io.Writer
}

// NewParser 创建一个Parser
func NewParser(r io.Reader) *Parser {
	return &Parser{
		reader: bufio.NewReader(r),
	}
}

func (p *Parser) Parse() (*Command, error) {
	typ, err := p.reader.ReadByte()
	if err != nil {
		return nil, err
	}
	switch typ {
	case ARRAY:
		return p.parseArray()
	default:
		return nil, ErrInvalidRESP
	}
}

// readLine 读取一行数据，去除CRLF
func (p *Parser) readLine() ([]byte, error) {
	line, err := p.reader.ReadBytes('\n')
	if err != nil {
		return nil, err
	}

	if len(line) < 2 || line[len(line)-2] != '\r' {
		return nil, ErrInvalidRESP
	}

	return line[:len(line)-2], nil
}

// parseInt 将字节数组解析为整数
func parseInt(b []byte) (int, error) {
	return strconv.Atoi(string(b))
}

// parseArray 解析数组
func (p *Parser) parseArray() (*Command, error) {
	// 读取数组长度
	line, err := p.readLine()
	if err != nil {
		return nil, err
	}

	// 解析数组长度
	count, err := parseInt(line)
	if err != nil {
		return nil, err
	}

	if count <= 0 {
		return nil, ErrInvalidRESP
	}

	// 读取命令和参数
	args := make([][]byte, count)
	for i := 0; i < count; i++ {
		// 读取类型
		typ, err := p.reader.ReadByte()
		if err != nil {
			return nil, err
		}

		if typ != BULK {
			return nil, ErrInvalidRESP
		}

		// 读取批量字符串长度
		line, err := p.readLine()
		if err != nil {
			return nil, err
		}

		// 解析批量字符串长度
		size, err := parseInt(line)
		if err != nil {
			return nil, err
		}

		if size < 0 {
			args[i] = nil
			continue
		}

		// 读取批量字符串内容
		data := make([]byte, size)
		if _, err := io.ReadFull(p.reader, data); err != nil {
			return nil, err
		}

		// 跳过CRLF
		if _, err := p.reader.ReadByte(); err != nil {
			return nil, err
		}
		if _, err := p.reader.ReadByte(); err != nil {
			return nil, err
		}

		args[i] = data
	}

	log.Printf("%d , %s\n", len(args), args)

	// 构建命令
	cmd := &Command{
		Name: string(args[0]),
		Args: args[1:],
	}

	return cmd, nil
}

// NewWriter 创建一个Writer
func NewWriter(w io.Writer) *Writer {
	return &Writer{
		writer: w,
	}
}

// WriteString 写入一个字符串
func (w *Writer) WriteString(s string) error {
	_, err := w.writer.Write([]byte{STRING})
	if err != nil {
		return err
	}

	_, err = w.writer.Write([]byte(s))
	if err != nil {
		return err
	}

	_, err = w.writer.Write(CRLF)
	return err
}

// WriteError 写入一个错误
func (w *Writer) WriteError(err error) error {
	_, werr := w.writer.Write([]byte{ERROR})
	if werr != nil {
		return err
	}

	_, werr = w.writer.Write([]byte(fmt.Sprintf("%v", err)))
	if werr != nil {
		return werr
	}

	_, werr = w.writer.Write(CRLF)
	return werr
}

// WriteInteger 写入一个整数
func (w *Writer) WriteInteger(n int64) error {
	_, err := w.writer.Write([]byte{INTEGER})
	if err != nil {
		return err
	}

	_, err = w.writer.Write([]byte(strconv.FormatInt(n, 10)))
	if err != nil {
		return err
	}

	_, err = w.writer.Write(CRLF)
	return err
}

// WriteBulk 写入一个数组
func (w *Writer) WriteBulk(b []byte) error {
	if b == nil {
		_, err := w.writer.Write([]byte("$-1\r\n"))
		return err
	}

	_, err := w.writer.Write([]byte{BULK})
	if err != nil {
		return err
	}

	_, err = w.writer.Write([]byte(strconv.Itoa(len(b))))
	if err != nil {
		return err
	}

	_, err = w.writer.Write(CRLF)
	if err != nil {
		return err
	}

	_, err = w.writer.Write(b)
	if err != nil {
		return err
	}

	_, err = w.writer.Write(CRLF)
	return err
}

// WriteArray 写入一堆数组
func (w *Writer) WriteArray(arr [][]byte) error {
	if arr == nil {
		_, err := w.writer.Write([]byte("$-1\r\n"))
		return err
	}

	_, err := w.writer.Write([]byte{ARRAY})
	if err != nil {
		return err
	}

	_, err = w.writer.Write([]byte(strconv.Itoa(len(arr))))
	if err != nil {
		return err
	}

	_, err = w.writer.Write(CRLF)
	if err != nil {
		return err
	}

	for _, item := range arr {
		err = w.WriteBulk(item)
		if err != nil {
			return err
		}
	}

	return nil
}
