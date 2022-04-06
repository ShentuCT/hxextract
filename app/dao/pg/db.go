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
)

type (

	// TableConfig 适配老版财务数据配置表
	TableConfig struct {
		schemaName string //信息表所属模式
		fieldInfo  string //存储财务文件字段信息的表名，默认为CJ_INDEX
		fin2table  string //存储财务文件名与mysql表对应关系的表名
	}

	sqlFlag struct {
		funcFlag  bool
		indexFlag bool
	}
)

//用于存储pg信息的包变量
var (
	dbTable = make(map[string]*gorm.DB) // 用dsn=>db形式存储pg的连接，以dsn为key
)

type DB struct {
	tables TableConfig
	baseDB *gorm.DB
	taskDB map[string]*gorm.DB
}

func NewDB() (db *DB, cf func(), err error) {
	log.Log.Info("init pg connection")
	db = new(DB)
	db.taskDB = make(map[string]*gorm.DB)
	pgCfg := config.GetPgsql()
	db.tables = TableConfig{
		pgCfg.SchemaName,
		pgCfg.FieldInfo,
		pgCfg.Fin2Table,
	}
	if db.baseDB, err = getConn(pgCfg.DefaultDSN); err != nil {
		return
	}
	return
}

//
// connectCheck
//  @Description: 检查pg连接是否正常
//  @receiver d
//
func (d *DB) connectCheck() error {
	for dsn, db := range dbTable {
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
		dbTable[dsn] = db
	}
	return nil
}

//
//  taskDbLoad
//  @Description: 初始化pg导出任务，为所有导出任务所需的db库创建连接
//  @receiver d
//  @return error
//
func (d *DB) taskDbLoad(dsn string) error {
	// 通过dsn判断是否需要新建连接，否则直接沿用
	if _, ok := dbTable[dsn]; !ok {
		db, err := getConn(dsn)
		if err != nil {
			return err
		}
		dbTable[dsn] = db
	}
	return nil
}

func (d *DB) getDsnDb(dsn string) (*gorm.DB, error) {
	if db, ok := d.taskDB[dsn]; ok {
		return db, nil
	}
	// todo make err
	return nil, nil
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
