package file_manager

import (
	"FinKV/err_def"
	storage2 "FinKV/storage"
	lru "FinKV/storage/cache"
	"encoding/binary"
	"fmt"
	"hash/crc64"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

// FileManager 管理多文件读写、异步写入、定期 fsync 等
type FileManager struct {
	dir          string
	maxFileSize  int64
	maxOpenFiles int
	syncInterval time.Duration

	// 活动文件相关
	activeFile atomic.Value // 存 *DataFile
	fileID     atomic.Int32 // 下一个文件ID
	fileMu     sync.Mutex   // 轮转文件的锁

	// 打开的文件管理
	openFiles    *lru.LRUCache[int, *os.File]
	sync.RWMutex // 读多写少，用于 openFiles 的双检锁

	// 异步写线程
	writeChan  chan AsyncWriteReq
	stopChan   chan struct{}
	wg         sync.WaitGroup // 用于等待异步写线程退出
	syncTicker *time.Ticker   // 定期 Sync 定时器
}

// AsyncWriteReq/Resp
type AsyncWriteReq struct {
	DataByte []byte
	Resp     chan AsyncWriteResp
}

type AsyncWriteResp struct {
	Entry storage2.Entry
	Err   error
}

// NewFileManager 创建 FileManager 并完成初始化
func NewFileManager(
	dataDir string,
	maxFileSize int64,
	maxOpenFiles int,
	syncInterval time.Duration,
) (*FileManager, error) {

	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data directory: %w", err)
	}

	cache := lru.NewLRUCache[int, *os.File](maxOpenFiles)

	fm := &FileManager{
		dir:          dataDir,
		maxFileSize:  maxFileSize,
		maxOpenFiles: maxOpenFiles,
		syncInterval: syncInterval,
		openFiles:    cache,
		writeChan:    make(chan AsyncWriteReq, 1024),
		stopChan:     make(chan struct{}),
		syncTicker:   time.NewTicker(syncInterval),
	}

	// 初始化，找出目前已有的最大文件编号 + 1
	if err := fm.initialize(); err != nil {
		return nil, err
	}

	// 启动异步写线程
	fm.wg.Add(1)
	go fm.processWrites()

	// 启动定时 fsync 线程
	fm.wg.Add(1)
	go fm.autoSync()

	return fm, nil
}

// initialize 找出现存文件的最大编号，打开它作为活动文件，或创建新文件
func (fm *FileManager) initialize() error {
	files, err := os.ReadDir(fm.dir)
	if err != nil {
		return fmt.Errorf("read directory failed: %w", err)
	}
	var maxID int
	for _, f := range files {
		var id int
		_, err := fmt.Sscanf(f.Name(), storage2.FilePrefix+"%d"+storage2.FileSuffix, &id)
		if err == nil && id > maxID {
			maxID = id
		}
	}
	// 设置下一个可用文件 ID
	fm.fileID.Store(int32(maxID + 1))

	if maxID > 0 {
		// 重新打开旧的最大编号文件，用于后续追加
		filePath := filepath.Join(fm.dir, fmt.Sprintf("%s%d%s", storage2.FilePrefix, maxID, storage2.FileSuffix))
		file, err := os.OpenFile(filePath, os.O_RDWR, 0644)
		if err != nil {
			return fmt.Errorf("open active file failed: %w", err)
		}
		stat, err := file.Stat()
		if err != nil {
			file.Close()
			return fmt.Errorf("stat file failed: %w", err)
		}
		df := &storage2.DataFile{
			ID:   maxID,
			Path: filePath,
			File: file,
		}
		df.Offset.Store(stat.Size()) // 活动文件的当前写偏移
		fm.activeFile.Store(df)
		fm.openFiles.Insert(maxID, file)
	} else {
		// 没有任何历史文件，创建新的
		if _, err := fm.rotateFile(); err != nil {
			return fmt.Errorf("initial rotate failed: %v", err)
		}
	}
	return nil
}

// WriteAsync 对外提供的异步写入接口，返回结果的 chan
func (fm *FileManager) WriteAsync(r *storage2.Record) <-chan AsyncWriteResp {
	result := make(chan AsyncWriteResp, 1)

	// 1.编码成二进制
	data, err := encodeRecord(r)
	if err != nil {
		// 如果编码失败，直接返回错误
		go func() {
			result <- AsyncWriteResp{Err: err}
			close(result)
		}()
		return result
	}

	// 2. 创建异步写请求
	req := AsyncWriteReq{
		DataByte: data,
		Resp:     make(chan AsyncWriteResp, 1),
	}

	// 3. 发送到写入通道
	select {
	case fm.writeChan <- req:
		// 异步读出写结果再转发
		go func() {
			res := <-req.Resp
			result <- res
			close(result)
		}()
	case <-fm.stopChan:
		// 如果已经关闭，则立即返回错误
		go func() {
			result <- AsyncWriteResp{Err: err_def.ErrDBClosed}
			close(result)
		}()
	}
	return result
}

// processWrites 消费 fm.writeChan，执行实际写入
func (fm *FileManager) processWrites() {
	defer fm.wg.Done()
	for {
		select {
		case req, ok := <-fm.writeChan:
			if !ok {
				return
			}
			// 执行实际的同步写入
			entry, err := fm.syncWrite(req.DataByte)
			req.Resp <- AsyncWriteResp{Entry: entry, Err: err}
			close(req.Resp)
		case <-fm.stopChan:
			return
		}
	}
}

// syncWrite 内部真正执行写入的函数
func (fm *FileManager) syncWrite(data []byte) (storage2.Entry, error) {
	for {
		// 1. 获取当前活动文件
		current := fm.GetActiveFile()
		if current == nil {
			return storage2.Entry{}, err_def.ErrFileNotFound
		}

		// 2. 检查文件是否已关闭
		if current.Closed.Load() {
			// 已关闭，进行轮转
			_, err := fm.rotateFile()
			if err != nil {
				return storage2.Entry{}, err
			}
			continue
		}

		// 检查剩余空间，如果不够则轮转
		offsetNow := current.Offset.Load()
		if offsetNow+int64(len(data)) > fm.maxFileSize {
			_, err := fm.rotateFile()
			if err != nil {
				return storage2.Entry{}, err
			}
			continue
		}

		// 4. 计算写入起始位置
		newOffset := current.Offset.Add(int64(len(data)))
		writePos := newOffset - int64(len(data))

		// 5. 执行写入
		n, err := current.File.WriteAt(data, writePos)
		if err != nil || n != len(data) {
			// 写失败，则关闭当前文件并轮转
			_ = current.File.Close()
			current.Closed.Store(true)

			_, rotateErr := fm.rotateFile()
			if rotateErr != nil {
				return storage2.Entry{}, rotateErr
			}
			return storage2.Entry{}, err_def.ErrWriteFailed
		}

		// 6. 写成功，返回对应的索引信息
		return storage2.Entry{
			FileID:    current.ID,
			Offset:    writePos,
			Size:      uint32(len(data)),
			Timestamp: time.Now().UnixNano(),
		}, nil
	}
}

// Read 按 Entry 信息从对应的文件偏移处读出数据并解码
func (fm *FileManager) Read(entry storage2.Entry) (*storage2.Record, error) {
	file, err := fm.GetFile(entry.FileID)
	if err != nil {
		return nil, err
	}
	buf := make([]byte, entry.Size)

	_, err = file.ReadAt(buf, entry.Offset)
	if err != nil {
		// 如果是 io.EOF 也要返回错误，说明数据不完整
		if err == io.EOF {
			return nil, fmt.Errorf("%w: unexpected EOF (fileID=%d)", err_def.ErrReadFailed, entry.FileID)
		}
		return nil, fmt.Errorf("%w: %v", err_def.ErrReadFailed, err)
	}

	record, err := DecodeRecord(buf)
	if err != nil {
		return nil, err
	}

	// 二次校验
	if success, err := validateChecksum(record); !success || err != nil {
		return nil, err_def.ErrChecksumInvalid
	}
	return record, nil
}

// rotateFile 轮转当前活跃文件，创建新文件并设为活跃文件
func (fm *FileManager) rotateFile() (*storage2.DataFile, error) {
	fm.fileMu.Lock()
	defer fm.fileMu.Unlock()

	oldFile := fm.GetActiveFile()
	if oldFile != nil && !oldFile.Closed.Load() {
		// 并非已经关闭则先关闭
		oldFile.Closed.Store(true)
		_ = oldFile.File.Sync()
		_ = oldFile.File.Close()
	}

	fileID := int(fm.fileID.Load())
	path := filepath.Join(fm.dir, fmt.Sprintf("%s%d%s", storage2.FilePrefix, fileID, storage2.FileSuffix))

	// 以追加方式打开新文件，以便按 Offset 写
	newF, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("create file failed: %w", err)
	}

	df := &storage2.DataFile{
		ID:   fileID,
		Path: path,
		File: newF,
	}
	df.Offset.Store(0)

	// 更新活跃文件
	fm.activeFile.Store(df)
	fm.openFiles.Insert(fileID, newF)
	fm.fileID.Add(1)

	return df, nil
}

// GetActiveFile 获取当前活跃文件
func (fm *FileManager) GetActiveFile() *storage2.DataFile {
	val := fm.activeFile.Load()
	if val == nil {
		return nil
	}
	return val.(*storage2.DataFile)
}

// GetFile 根据 fileID 从缓存中获取文件指针，若无则重新打开并放入缓存
func (fm *FileManager) GetFile(fileID int) (*os.File, error) {
	// 读锁检测
	fm.RLock()
	if file, ok := fm.openFiles.Get(fileID); ok {
		fm.RUnlock()
		return file, nil
	}
	fm.RUnlock()

	// 未命中缓存，加写锁
	fm.Lock()
	defer fm.Unlock()

	// 二次检查
	if file, ok := fm.openFiles.Get(fileID); ok {
		return file, nil
	}

	path := filepath.Join(fm.dir, fmt.Sprintf("%s%d%s", storage2.FilePrefix, fileID, storage2.FileSuffix))
	file, err := os.OpenFile(path, os.O_RDWR, 0644)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("%w: fileID=%d", err_def.ErrFileNotFound, fileID)
		}
		return nil, fmt.Errorf("open file failed (ID=%d): %w", fileID, err)
	}

	// 简单校验文件状态
	if _, err := file.Stat(); err != nil {
		file.Close()
		return nil, fmt.Errorf("file stat failed (ID=%d): %w", fileID, err)
	}

	fm.openFiles.Insert(fileID, file)
	return file, nil
}

// autoSync 定时对活跃文件做 fsync
func (fm *FileManager) autoSync() {
	defer fm.wg.Done()
	for {
		select {
		case <-fm.syncTicker.C:
			fm.fileMu.Lock()
			if current := fm.GetActiveFile(); current != nil && !current.Closed.Load() {
				_ = current.File.Sync()
			}
			fm.fileMu.Unlock()

		case <-fm.stopChan:
			return
		}
	}
}

// Close 关闭 FileManager，等待所有异步操作完成并关闭所有文件
func (fm *FileManager) Close() error {
	// 停止定时器与写入协程
	fm.syncTicker.Stop()
	close(fm.stopChan)

	// 关闭写入通道，让 processWrites 退出
	close(fm.writeChan)
	// 等待异步协程退出
	fm.wg.Wait()

	// 再加锁确保没有人再写文件
	fm.fileMu.Lock()
	defer fm.fileMu.Unlock()

	// 关闭活跃文件
	if current := fm.GetActiveFile(); current != nil && !current.Closed.Load() {
		current.Closed.Store(true)
		_ = current.File.Sync()
		_ = current.File.Close()
	}

	// 将缓存里的文件都关闭
	fm.Lock()
	for _, file := range fm.openFiles.Values() {
		_ = file.Sync()
		_ = file.Close()
	}
	fm.openFiles.Purge()
	fm.Unlock()

	return nil
}

/* ------------------------------- 工具方法 -------------------------------- */

// encodeRecord 将 Record 编码为二进制格式
// 格式: [Timestamp(8)|Flags(4)|KeyLen(4)|ValueLen(4)|Key(?)|Value(?)|Checksum(8)]
// encodeRecord 将记录对象编码为二进制格式以便存储到文件中
// 编码格式：
// - 0-7字节：时间戳（8字节，大端序）
// - 8-11字节：标志位（4字节，大端序）
// - 12-15字节：键长度（4字节，大端序）
// - 16-19字节：值长度（4字节，大端序）
// - 20-?字节：键内容
// - ?-?字节：值内容
// - 最后8字节：CRC64校验和（大端序）
func encodeRecord(r *storage2.Record) ([]byte, error) {
	// 1. 输入验证 - 检查记录是否有效
	if r == nil {
		return nil, err_def.ErrNilRecord // 记录为空，返回错误
	}
	if len(r.Key) == 0 {
		return nil, err_def.ErrEmptyKey // 键为空，返回错误
	}
	if len(r.Key) > storage2.MaxKeySize {
		return nil, fmt.Errorf("%w: key length %d exceeds maximum %d", err_def.ErrKeyTooLarge, len(r.Key), storage2.MaxKeySize) // 键太长，返回错误
	}
	if len(r.Value) > storage2.MaxValueSize {
		return nil, fmt.Errorf("%w: value length %d exceeds maximum %d", err_def.ErrValueTooLarge, len(r.Value), storage2.MaxValueSize) // 值太长，返回错误
	}

	// 2. 计算长度 - 确定需要分配的缓冲区大小
	keyLen := len(r.Key)                                // 键的长度
	valueLen := len(r.Value)                            // 值的长度
	dataSize := storage2.HeaderSize + keyLen + valueLen // 数据部分的总大小（头部+键+值）
	totalSize := dataSize + 8                           // 加上校验和的8字节，得到总大小

	// 3. 分配并填充缓冲区 - 创建字节数组并写入数据
	buf := make([]byte, totalSize) // 分配足够大小的缓冲区

	// 写入头部信息 - 使用大端序写入各个字段
	binary.BigEndian.PutUint64(buf[0:8], uint64(r.Timestamp)) // 写入时间戳（8字节）
	binary.BigEndian.PutUint32(buf[8:12], r.Flags)            // 写入标志位（4字节）
	binary.BigEndian.PutUint32(buf[12:16], uint32(keyLen))    // 写入键长度（4字节）
	binary.BigEndian.PutUint32(buf[16:20], uint32(valueLen))  // 写入值长度（4字节）

	// 写入键值对 - 将键和值的内容复制到缓冲区中
	copy(buf[storage2.HeaderSize:storage2.HeaderSize+keyLen], r.Key) // 复制键内容
	copy(buf[storage2.HeaderSize+keyLen:dataSize], r.Value)          // 复制值内容

	// 计算并写入校验和 - 使用CRC64算法计算数据部分的校验和
	checksum := crc64.Checksum(buf[:dataSize], crc64.MakeTable(crc64.ISO)) // 计算校验和
	binary.BigEndian.PutUint64(buf[dataSize:], checksum)                   // 将校验和写入缓冲区末尾

	return buf, nil // 返回编码后的字节数组
}

// DecodeRecord 从二进制数据解析记录
func DecodeRecord(data []byte) (*storage2.Record, error) {
	// 1. 基础长度检查
	if len(data) < storage2.HeaderSize+8 { // HeaderSize + 校验和长度
		return nil, fmt.Errorf("%w: got %d bytes, need at least %d", err_def.ErrInsufficientData, len(data), storage2.HeaderSize+8)
	}

	// 2. 读取头部信息
	timestamp := int64(binary.BigEndian.Uint64(data[0:8]))
	flags := binary.BigEndian.Uint32(data[8:12])
	keyLen := binary.BigEndian.Uint32(data[12:16])
	valueLen := binary.BigEndian.Uint32(data[16:20])

	// 3. 验证长度
	expectedLen := storage2.HeaderSize + int(keyLen) + int(valueLen) + 8
	if len(data) != expectedLen {
		return nil, fmt.Errorf("%w: got %d bytes, expected %d", err_def.ErrDataLengthInvalid, len(data), expectedLen)
	}

	// 4. 边界检查
	if keyLen > uint32(storage2.MaxKeySize) {
		return nil, err_def.ErrKeyTooLarge
	}
	if valueLen > uint32(storage2.MaxValueSize) {
		return nil, err_def.ErrValueTooLarge
	}

	dataSize := len(data) - 8

	// 5. 校验和验证
	storedChecksum := binary.BigEndian.Uint64(data[dataSize:])
	calculatedChecksum := crc64.Checksum(data[:dataSize], crc64.MakeTable(crc64.ISO))
	if calculatedChecksum != storedChecksum {
		return nil, fmt.Errorf("%w: stored=%x, calculated=%x", err_def.ErrChecksumMismatch, storedChecksum, calculatedChecksum)
	}

	// 6. 提取键值对
	keyStart := storage2.HeaderSize
	keyEnd := keyStart + int(keyLen)
	valueStart := keyEnd
	valueEnd := valueStart + int(valueLen)

	// 创建新的缓冲区，避免与原始数据共享内存
	key := make([]byte, keyLen)
	value := make([]byte, valueLen)
	copy(key, data[keyStart:keyEnd])
	copy(value, data[valueStart:valueEnd])

	// 7. 构造并返回记录
	return &storage2.Record{
		Timestamp: timestamp,
		Flags:     flags,
		Checksum:  storedChecksum,
		KVItem: storage2.KVItem{
			Key:   key,
			Value: value,
		},
	}, nil
}

// validateChecksum 验证记录的完整性
func validateChecksum(r *storage2.Record) (bool, error) {
	if r == nil {
		return false, err_def.ErrNilRecord
	}
	if r.Checksum == 0 {
		return false, nil
	}

	// 重新编码记录
	data, err := encodeRecord(r)
	if err != nil {
		return false, fmt.Errorf("failed to encode record: %w", err)
	}

	dataSize := len(data) - 8
	storedChecksum := binary.BigEndian.Uint64(data[dataSize:])
	calculatedChecksum := crc64.Checksum(data[:dataSize], crc64.MakeTable(crc64.ISO))

	return calculatedChecksum == storedChecksum, nil
}
