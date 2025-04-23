package database

import (
	"FinKV/config"
	redis2 "FinKV/database/redis"
	"FinKV/storage"
	"log"
)

// FincasDB 是主数据库结构体，包含了各种数据类型的操作接口
type FincasDB struct {
	*redis2.RString // 字符串操作接口
	//*redis.RHash   // 哈希表操作接口
	//*redis.RList   // 列表操作接口
	//*redis.RSet    // 集合操作接口
	//*redis.RZSet   // 有序集合操作接口
}

// NewFincasDB 创建并初始化一个新的FincasDB实例
// dataDir参数指定数据存储的目录路径
func NewFincasDB(dataDir string) *FincasDB {
	// 存储引擎选项列表
	var bcOpts []storage.Option
	// 获取全局配置
	conf := config.Get()

	// 设置数据目录
	if conf.Base.DataDir != "" && dataDir == "" {
		// 如果配置文件中指定了数据目录且函数参数未指定，使用配置文件中的目录
		bcOpts = append(bcOpts, storage.WithDataDir(conf.Base.DataDir))
	} else if dataDir != "" {
		// 如果函数参数指定了数据目录，优先使用参数指定的目录
		bcOpts = append(bcOpts, storage.WithDataDir(dataDir))
	}

	// 配置内存索引数据结构
	if conf.MemIndex.DataStructure != "" {
		var memDS storage.MemIndexType
		switch conf.MemIndex.DataStructure {
		case "btree": // B树索引
			memDS = storage.BTree
			// 设置B树度数，最小为8
			bcOpts = append(bcOpts, storage.WithBTreeDegree(max(conf.MemIndex.BTreeDegree, 8)))
		case "skiplist": // 跳表索引
			memDS = storage.SkipList
		case "swisstable": // SwissTable哈希索引
			memDS = storage.SwissTable
			// 设置初始大小，最小为1024
			bcOpts = append(bcOpts, storage.WithBTreeDegree(max(conf.MemIndex.SwissTableInitialSize, 1024)))
		default: // 不支持的索引类型
			log.Fatal("Unsupported MemIndex data structure: " + conf.MemIndex.DataStructure)
		}
		// 设置内存索引数据结构类型
		bcOpts = append(bcOpts, storage.WithMemIndexDS(memDS))
	}
	// 设置内存索引分片数量，最小为128
	bcOpts = append(bcOpts, storage.WithMemIndexShardCount(max(conf.MemIndex.ShardCount, 128)))

	// 配置内存缓存
	if conf.MemCache.Enable { // 如果启用内存缓存
		bcOpts = append(bcOpts, storage.WithOpenMemCache(true))
		switch conf.MemCache.DataStructure {
		case "lru": // LRU缓存策略
			bcOpts = append(bcOpts, storage.WithMemCacheDS(storage.LRU))
			// 设置缓存大小，最小为1024
			bcOpts = append(bcOpts, storage.WithMemCacheSize(max(conf.MemCache.Size, 1024)))
		default: // 不支持的缓存类型
			log.Fatal("Unsupported MemCache data structure: " + conf.MemCache.DataStructure)
		}
	} else { // 如果禁用内存缓存
		bcOpts = append(bcOpts, storage.WithOpenMemCache(false))
	}

	// 配置文件管理器参数
	// 设置最大文件大小
	bcOpts = append(bcOpts, storage.WithMaxFileSize(max(storage.DefaultOptions().MaxFileSize, int64(conf.FileManager.MaxSize))))
	// 设置最大打开文件数
	bcOpts = append(bcOpts, storage.WithMaxOpenFiles(max(storage.DefaultOptions().MaxOpenFiles, conf.FileManager.MaxOpened)))
	// 设置同步间隔
	bcOpts = append(bcOpts, storage.WithSyncInterval(max(storage.DefaultOptions().SyncInterval, conf.FileManager.SyncInterval)))

	// 配置合并策略
	if conf.Merge.Auto { // 如果启用自动合并
		bcOpts = append(bcOpts, storage.WithAutoMerge(true))
		// 设置合并间隔
		bcOpts = append(bcOpts, storage.WithMergeInterval(max(storage.DefaultOptions().MergeInterval, conf.Merge.Interval)))
		// 设置最小合并比率
		bcOpts = append(bcOpts, storage.WithMinMergeRatio(max(storage.DefaultOptions().MinMergeRatio, conf.Merge.MinRatio)))
	} else { // 如果禁用自动合并
		bcOpts = append(bcOpts, storage.WithAutoMerge(false))
	}

	// 创建数据库包装器
	dw := redis2.NewBDWrapper(nil, bcOpts...)
	// 创建并返回数据库实例，初始化各种数据类型的操作接口
	return &FincasDB{
		RString: redis2.NewRString(dw), // 字符串操作接口
		//RHash:   redis2.NewRHash(dw),   // 哈希表操作接口
		//RList:   redis2.NewRList(dw),   // 列表操作接口
		//RSet:    redis2.NewRSet(dw),    // 集合操作接口
		//RZSet:   redis2.NewRZSet(dw),   // 有序集合操作接口
	}
}

// Close 关闭数据库，释放所有资源
func (db *FincasDB) Close() {
	// 释放各种数据类型的操作接口资源
	db.RString.Release() // 释放字符串操作接口
	//db.RHash.Release()   // 释放哈希表操作接口
	//db.RList.Release()   // 释放列表操作接口
	//db.RSet.Release()    // 释放集合操作接口
	//db.RZSet.Release()   // 释放有序集合操作接口
}
