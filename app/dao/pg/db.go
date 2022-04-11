package pg

/*
author:heqimin
purpose:pg库信息加载及链接管理
*/

import (
	"fmt"
	"go.uber.org/zap"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"gorm.io/gorm/schema"
	"hxextract/app/config"
	"hxextract/app/log"
	"sync"
)

type (
	// DB 连接信息管理
	DB struct {
		taskDB    map[string]*gorm.DB // 用dsn=>db形式存储pg的连接，以dsn为key
		mutexTask sync.Mutex
	}
)

func NewDB() (db *DB, cf func(), err error) {
	log.Log.Info("init pg connection")
	db = new(DB)
	db.taskDB = make(map[string]*gorm.DB)
	return
}

//
// connectCheck
//  @Description: 检查pg连接是否正常
//  @receiver d
//
func (d *DB) connectCheck() error {
	d.mutexTask.Lock()
	defer d.mutexTask.Unlock()
	for dsn, db := range d.taskDB {
		db_, err := db.DB()
		if err == nil && db_.Ping() == nil {
			continue
		}
		log.Log.Error("pg connection lost,try to reconnect", zap.String("dsn", dsn))
		db, err = getConn(dsn)
		if err != nil {
			log.Log.Error("reconnect pg connection failed")
			return err
		}
		d.taskDB[dsn] = db
	}
	return nil
}

func (d *DB) getDsnDb(dsn string) (*gorm.DB, error) {
	d.mutexTask.Lock()
	defer d.mutexTask.Unlock()
	if db, ok := d.taskDB[dsn]; ok {
		return db, nil
	}
	dbNew, err := getConn(dsn)
	if err != nil {
		return nil, nil
	}
	d.taskDB[dsn] = dbNew
	return dbNew, nil
}

// getConn 获取pg库连接
func getConn(dsn string) (db *gorm.DB, err error) {
	db, err = gorm.Open(postgres.Open(dsn), &gorm.Config{
		NamingStrategy: schema.NamingStrategy{
			TablePrefix:   "",
			SingularTable: true,
		},
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err == nil {
		sqlExec := fmt.Sprintf("set statement_timeout to %d", config.GetPgsql().QueryTimeout)
		db.Exec(sqlExec)
	}
	return
}
