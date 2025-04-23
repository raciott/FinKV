// Package err_def 定义了FincasKV系统中使用的所有错误类型
package err_def

import (
	"errors" // 标准错误包
	"fmt"    // 格式化包
)

// 系统中使用的错误常量定义
var (
	ErrKeyNotFound       = errors.New("key not found")           // 键不存在错误
	ErrChecksumInvalid   = errors.New("checksum invalid")        // 校验和无效错误
	ErrDBClosed          = errors.New("database is closed")      // 数据库已关闭错误
	ErrWriteFailed       = errors.New("write failed")            // 写入失败错误
	ErrReadFailed        = errors.New("read failed")             // 读取失败错误
	ErrFileNotFound      = errors.New("file not found")          // 文件未找到错误
	ErrNilRecord         = errors.New("nil record")              // 记录为空错误
	ErrKeyTooLarge       = errors.New("key too large")           // 键过大错误
	ErrValueTooLarge     = errors.New("value too large")         // 值过大错误
	ErrEmptyKey          = errors.New("empty key")               // 空键错误
	ErrChecksumMismatch  = errors.New("checksum mismatch")       // 校验和不匹配错误
	ErrInsufficientData  = errors.New("insufficient data")       // 数据不足错误
	ErrDataLengthInvalid = errors.New("invalid data length")     // 数据长度无效错误
	ErrValueNotInteger   = fmt.Errorf("value is not an integer") // 值不是整数错误
	ErrValueNotFloat     = fmt.Errorf("value is not a float")    // 值不是浮点数错误
)
