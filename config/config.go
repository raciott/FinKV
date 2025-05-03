package config

import (
	"log"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/spf13/viper" // 用于识别配置文件，并且支持热更新
)

type BaseConfig struct {
	DataDir string // 数据目录
}

type NetworkConfig struct {
	Addr         string        // 地址
	IdleTimeout  time.Duration // 空闲超时
	MaxConns     int           // 最大连接数
	ReadTimeout  time.Duration // 读超时时间
	WriteTimeout time.Duration // 写超时时间
}

type MemIndexConfig struct {
	DataStructure         string // 数据结构
	ShardCount            int    // 分片数量
	BTreeDegree           int    // BTree 阶数
	SwissTableInitialSize int    // SwissTable 初始大小
}

type MemCacheConfig struct {
	Enable        bool   // 启用缓存
	DataStructure string // 数据结构
	Size          int    // 缓存大小
}

type FileManagerConfig struct {
	MaxSize      int           // 单个文件最大大小
	MaxOpened    int           // 最大打开文件数
	SyncInterval time.Duration // 同步间隔
}

type MergeConfig struct {
	Auto     bool          // 自动合并
	Interval time.Duration // 自动合并间隔
	MinRatio float64       // 最小合并比例
}

type Config struct {
	Base        BaseConfig        // 基础配置
	Network     NetworkConfig     // 网络配置
	MemIndex    MemIndexConfig    // 索引配置
	MemCache    MemCacheConfig    // 缓存配置
	FileManager FileManagerConfig // 文件管理配置
	Merge       MergeConfig       // 合并配置
}

var (
	conf     *Config      // 全局配置
	confOnce sync.Once    // 确保配置只初始化一次
	mu       sync.RWMutex // 配置读写锁
)

// Get 获取配置
func Get() *Config {
	mu.RLock()
	defer mu.RUnlock()
	return conf
}

// 加载配置文件
func loadConfig(v *viper.Viper) *Config {
	cfg := &Config{} // 创建配置实例

	cfg.Base.DataDir = v.GetString("base.data_dir")

	// 加载网络配置
	cfg.Network.Addr = v.GetString("network.addr")
	cfg.Network.IdleTimeout = v.GetDuration("network.idle_timeout")
	cfg.Network.MaxConns = v.GetInt("network.max_conns")
	cfg.Network.ReadTimeout = v.GetDuration("network.read_timeout")
	cfg.Network.WriteTimeout = v.GetDuration("network.write_timeout")

	// 加载索引配置
	cfg.MemIndex.DataStructure = v.GetString("mem_index.data_structure")
	cfg.MemIndex.ShardCount = v.GetInt("mem_index.shard_count")
	cfg.MemIndex.BTreeDegree = v.GetInt("mem_index.btree_degree")
	cfg.MemIndex.SwissTableInitialSize = v.GetInt("mem_index.swiss_table_initial_size")

	// 加载缓存配置
	cfg.MemCache.Enable = v.GetBool("mem_cache.enable")
	cfg.MemCache.DataStructure = v.GetString("mem_cache.data_structure")
	cfg.MemCache.Size = v.GetInt("mem_cache.size")

	// 加载文件管理配置
	cfg.FileManager.MaxSize = v.GetInt("file_manager.max_size")
	cfg.FileManager.MaxOpened = v.GetInt("file_manager.max_opened")
	cfg.FileManager.SyncInterval = v.GetDuration("file_manager.sync_interval")

	// 加载合并配置
	cfg.Merge.Auto = v.GetBool("merge.auto")
	cfg.Merge.Interval = v.GetDuration("merge.interval")
	cfg.Merge.MinRatio = v.GetFloat64("merge.min_ratio")

	//将 cfg 转换为 JSON 格式并输出
	//jsonData, err := json.MarshalIndent(cfg, "", "  ")
	//if err != nil {
	//	log.Printf("Failed to marshal config to JSON: %v\n", err)
	//} else {
	//	fmt.Println("config:")
	//	fmt.Println(string(jsonData))
	//}

	return cfg
}

// Init 初始化配置
func Init(configPath string) error {
	var initErr error
	confOnce.Do(func() {
		v := viper.New()
		v.SetConfigFile(configPath) // 设置配置文件路径

		if err := v.ReadInConfig(); err != nil {
			initErr = err
			log.Printf("read config file failed: %v\n", err)
			return
		}

		mu.Lock()
		conf = loadConfig(v)

		//log.Printf("配置初始完成")

		// 配置文件热更新监听
		v.WatchConfig()
		v.OnConfigChange(func(e fsnotify.Event) {
			log.Printf("config file changed: %s\n", e.Name)

			// 重新加载配置
			newV := viper.New()
			newV.SetConfigFile(configPath)

			if err := newV.ReadInConfig(); err != nil {
				log.Printf("read config file failed: %v\n", err)
				return
			}

			// 更新配置文件
			newConfig := loadConfig(newV)

			mu.Lock()
			conf = newConfig
			log.Printf("配置热更新完成")
			mu.Unlock()

			log.Printf("config file change and reloded")
		})

		mu.Unlock()
	})
	return initErr
}
