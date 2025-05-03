package redis

import (
	base2 "FinKV/database/base"
	"FinKV/storage"
	"log"
)

type DBWrapper struct {
	db *base2.DB
}

// NewBDWrapper 创建数据库包装器(用于底层操作数据库)
func NewBDWrapper(bcOpts ...storage.Option) *DBWrapper {
	db, err := base2.NewDB(bcOpts...)
	if err != nil {
		log.Fatal(err)
	}
	return &DBWrapper{db: db}
}

// GetDB 获取数据库实例
func (db *DBWrapper) GetDB() *base2.DB {
	return db.db
}
