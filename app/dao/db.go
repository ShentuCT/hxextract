package dao

import (
	"database/sql"
	"go.uber.org/zap"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"gorm.io/gorm/schema"
	"hxextract/app/config"
	"hxextract/app/log"
)

// DB mysql 连接管理
type DB struct {
	// MysqlDbs 用于存储schema=>db的映射关系
	MysqlDbs map[string]*sql.DB
	// 默认库连接，用于连接信息库
	defaultDb *sql.DB
	// 默认库gorm连接，用于处理type_describe表
	defaultOrm *gorm.DB
	// 默认库名，用于存储数据信息
	defaultSchema string
	// 库表详细信息
	gTableInfo  map[string]SchemaInfo
	financeInfo FinnameInfo
}

func NewDB() (db *DB, cf func(), err error) {
	log.Log.Info("init mysql connection")
	db = new(DB)
	db.MysqlDbs = make(map[string]*sql.DB)
	err = db.mysqlConnInit()
	cf = db.Close
	return
}

/*Close
 * @Description: 关闭所有db连接，并清空map
 */
func (d *DB) Close() {
	for _, db := range d.MysqlDbs {
		if db == nil {
			continue
		}
		err := db.Close()
		if err != nil {

		}
	}
	d.defaultDb = nil
	d.defaultSchema = ""
	d.MysqlDbs = make(map[string]*sql.DB)
}

//
//  mysqlConnInit
//  @Description: 初始化mysql连接
//  @receiver d
//  @return error
//
func (d *DB) mysqlConnInit() error {
	myCfg := config.GetMysql()
	d.defaultSchema = myCfg.DefaultDbname
	orm, err := gorm.Open(mysql.Open(makeDsn(myCfg.Address, myCfg.Params, d.defaultSchema)), &gorm.Config{
		NamingStrategy: schema.NamingStrategy{
			TablePrefix:   "",
			SingularTable: true,
		},
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err == nil {
		d.defaultOrm = orm
		d.defaultDb, _ = orm.DB()
	}
	for _, dbname := range myCfg.DbNames {
		if db, err := newDbConn(makeDsn(myCfg.Address, myCfg.Params, dbname)); err != nil {
			return err
		} else {
			d.MysqlDbs[dbname] = db
		}
	}
	d.MysqlDbs[d.defaultSchema] = d.defaultDb
	return nil
}

// 查看该数据库是否加载连接，如果未加载尝试加载
func (d *DB) inDataBases(schemaName string) (bool, error) {
	_, ok := d.MysqlDbs[schemaName]
	if !ok {
		myCfg := config.GetMysql()
		if db, err := newDbConn(makeDsn(myCfg.Address, myCfg.Params, schemaName)); err != nil {
			return false, err
		} else {
			d.MysqlDbs[schemaName] = db
			return true, nil
		}
	}
	return ok, nil
}

//
//  getConn
//  @Description: 通过库名获取对应连接
//  @receiver d
//  @param schemaName
//  @return *gorm.DB
//  @return error
//
func (d *DB) getConn(schemaName string) (*sql.DB, error) {
	ok, err := d.inDataBases(schemaName)
	if ok {
		return d.MysqlDbs[schemaName], nil
	} else {
		return nil, err
	}
}

//
//  connectCheck
//  @Description: 检查连接是否正常
//  @receiver d
//  @return error
//
func (d *DB) connectCheck() error {
	myCfg := config.GetMysql()
	if d.defaultDb.Ping() != nil {
		log.Log.Error("mysql connect lost, try to reconnect", zap.String("dbname", d.defaultSchema))
		orm, err := gorm.Open(mysql.Open(makeDsn(myCfg.Address, myCfg.Params, d.defaultSchema)), &gorm.Config{
			NamingStrategy: schema.NamingStrategy{
				TablePrefix:   "",
				SingularTable: true,
			},
			Logger: logger.Default.LogMode(logger.Silent),
		})
		if err != nil {
			log.Log.Error("reconnect mysql connection failed",
				zap.String("dbname", d.defaultSchema), zap.Error(err))
			return err
		}
		log.Log.Info("reconnect mysql connection successfully",
			zap.String("dbname", d.defaultSchema))
		d.defaultOrm = orm
		d.defaultDb, _ = orm.DB()
	}
	for dbname, db := range d.MysqlDbs {
		if db.Ping() != nil {
			log.Log.Error("mysql connection lost, try to reconnect", zap.String("dbname", dbname))
			if db, err := newDbConn(makeDsn(myCfg.Address, myCfg.Params, dbname)); err != nil {
				log.Log.Error("reconnect mysql connection failed", zap.String("dbname", dbname), zap.Error(err))
				return err
			} else {
				log.Log.Info("reconnect mysql connection successfully", zap.String("dbname", dbname))
				d.MysqlDbs[dbname] = db
			}
		}
	}
	return nil
}

/*newDbConn
 * @Description: 创建新的mysql连接
 * @param dsn
 * @return db
 * @return err
 */
func newDbConn(dsn string) (db *sql.DB, err error) {
	db, err = sql.Open("mysql", dsn)
	return
}

func makeDsn(address string, params string, dbName string) string {
	return address + dbName + "?" + params
}
