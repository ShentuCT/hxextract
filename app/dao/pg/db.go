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
	"strconv"
	"strings"
	"time"
)

type (
	// ConnInfo DNS信息结构体
	ConnInfo struct {
		host    string
		port    int
		user    string
		passwd  string
		dbname  string
		sslmode string
	}

	// TableConfig 适配老版财务数据配置表
	TableConfig struct {
		schemaName string //信息表所属模式
		taskInfo   string //存储任务信息的表名，默认为taskitems
		finInfo    string //存储财务文件信息的表名，默认为tableinfo
		fieldInfo  string //存储财务文件字段信息的表名，默认为CJ_INDEX
		fin2table  string //存储财务文件名与mysql表对应关系的表名
	}

	sqlFlag struct {
		funcFlag  bool
		indexFlag bool
	}

	// procSQLs 五种导出sql
	procSQLs [5]string
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
		pgCfg.TaskInfo,
		pgCfg.FinInfo,
		pgCfg.FieldInfo,
		pgCfg.Fin2Table,
	}
	if db.baseDB, err = getConn(pgCfg.DefaultDSN); err != nil {
		return
	}
	if err = db.taskDbLoad(); err != nil {
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
func (d *DB) taskDbLoad() error {
	log.Log.Info("init pg task")
	querySql := fmt.Sprintf("SELECT distinct taskname,server,username,passwd,database from %s.%s where export = 2;",
		d.tables.schemaName, d.tables.taskInfo)
	rows, err := d.baseDB.Raw(querySql).Rows()
	if err != nil {
		return err
	}
	//逐行处理查询结果
	for rows.Next() {
		var taskName, server, username, passwd, database string
		err = rows.Scan(&taskName, &server, &username, &passwd, &database)
		if err != nil {
			return err
		} else {
			dsn := makeDSN(getInfo(server, username, passwd, database))
			// 通过dsn判断是否需要新建连接，否则直接沿用
			if db, ok := dbTable[dsn]; ok {
				d.taskDB[taskName] = db
			} else {
				db, err = getConn(dsn)
				if err != nil {
					return err
				}
				dbTable[dsn] = db
				d.taskDB[taskName] = db
			}
		}
	}
	return nil
}

func (d *DB) getFinDb(finName string) (*gorm.DB, error) {
	taskName, err := d.getTaskName(finName)
	if err != nil {
		return nil, err
	}
	return d.getTaskDb(taskName)
}

func (d *DB) getTaskDb(taskName string) (*gorm.DB, error) {
	if db, ok := d.taskDB[taskName]; ok {
		return db, nil
	}
	// todo make err
	return nil, nil
}

func (d *DB) GetTaskFins(taskName string) ([]string, error) {
	// 仅限自营，isvalid=2
	querySql := fmt.Sprintf("SELECT finname from %s.%s where taskname = '%s' and isvalid = 2;",
		d.tables.schemaName, d.tables.finInfo, taskName)
	rows, err := d.baseDB.Raw(querySql).Rows()
	if err != nil {
		return nil, err
	}
	var fins []string
	var fin string
	for rows.Next() {
		err = rows.Scan(&fin)
		if err != nil {
			log.Log.Error(err.Error())
		} else {
			fins = append(fins, fin)
		}
	}
	return fins, err
}

/*GetTableName
 * @Description: 通过财务文件名获取mysql表名
 * @param finName 财务文件名
 * @return tableName 表名
 */
func (d *DB) GetTableName(finName string) (tableName string, err error) {
	querySql := fmt.Sprintf("select tablename from %s.%s where finname = '%s'",
		d.tables.schemaName, d.tables.fin2table, finName)
	row := d.baseDB.Raw(querySql).Row()
	err = row.Scan(&tableName)
	return
}

// GetSchema
//  @Description: 获取财务文件对应表所在的数据库
//  @receiver d
//  @param finName
//  @return schemaName
//  @return err
//
func (d *DB) GetSchema(finName string) (schemaName string, err error) {
	querySql := fmt.Sprintf("select schema from %s.%s where finname = '%s'",
		d.tables.schemaName, d.tables.fin2table, finName)
	row := d.baseDB.Raw(querySql).Row()
	err = row.Scan(&schemaName)
	return
}

//GetTableInfo
//  @Description: 通过财务文件名同时获取对应表名及表所在数据库名，相对于分别获取该两项能减少一次sql查询
//  @receiver d
//  @param finName
//  @return tableName
//  @return schemaName
//  @return err
//
func (d *DB) GetTableInfo(finName string) (tableName string, schemaName string, err error) {
	querySql := fmt.Sprintf("select tablename,schema from %s.%s where finname = '%s'",
		d.tables.schemaName, d.tables.fin2table, finName)
	row := d.baseDB.Raw(querySql).Row()
	err = row.Scan(&tableName, &schemaName)
	return
}

/*GetTaskName
 * @Description: 从文件信息表中获取财务文件所在任务名
 * @param finaName 财务文件名
 * @return taskName 任务名
 */
func (d *DB) getTaskName(finaName string) (taskName string, err error) {
	querySql := fmt.Sprintf("SELECT taskname from %s.%s where finname = '%s' and isvalid = 2;",
		d.tables.schemaName, d.tables.finInfo, finaName)
	row := d.baseDB.Raw(querySql).Row()
	err = row.Scan(&taskName)
	if err != nil {
		return
	}
	return
}

func (d *DB) getFinSqls(finName string) (sqls procSQLs, err error) {
	querySql := fmt.Sprintf("SELECT allproc,repproc,finproc,realproc,codeproc from %s.%s where"+
		" finname = '%s' and isvalid = 2", d.tables.schemaName, d.tables.finInfo, finName)
	row := d.baseDB.Raw(querySql).Row()
	if d.baseDB.Error != nil {
		return
	}
	var allSql, bbrqSql, rtimeSql, realSql, codeSql string
	err = row.Scan(&allSql, &bbrqSql, &rtimeSql, &realSql, &codeSql)
	if err != nil {
		return
	}
	sqls[OpAll], sqls[OpBbrq], sqls[OpRtime], sqls[OpReal], sqls[OpCode] =
		allSql, bbrqSql, rtimeSql, realSql, codeSql
	return
}

func (d *DB) getProc(finName string, para QueryParam) (string, sqlFlag, error) {
	flags := sqlFlag{}
	sqls, err := d.getFinSqls(finName)
	if err != nil {
		return "", flags, err
	}
	sql := sqls[para.ProcType]
	if para.ProcType == OpBbrq || para.ProcType == OpRtime {
		sql = strings.Replace(sql, "[start]", int2Date(para.StartDate), 1)
		sql = strings.Replace(sql, "[end]", int2Date(para.EndDate), 1)
	} else if para.ProcType == OpCode {
		codes := strings.Split(para.CodeList, ",")
		var codelist string
		for _, v := range codes {
			codelist += "'" + v + "',"
		}
		sql = strings.Replace(sql, "[codelist]", strings.TrimRight(codelist, ","), 1)
	}
	// 处理存储过程
	if strings.Contains(sql, "{") && strings.Contains(sql, "}") {
		// 财务数据中使用存储过程的sql均为用{}包围且缺select，需要进行处理
		sql = strings.Replace(sql, "{", "select ", 1)
		sql = strings.Replace(sql, "}", ";", 1)
		flags.funcFlag = true
	} else {
		// 非存储过程 适配部分sql中有set enable_nestloop=on
		sqlSplit := strings.Split(sql, ";")
		for _, unitSql := range sqlSplit {
			// 去掉首尾冗余空格
			unitSql = strings.Trim(unitSql, " ")
			if strings.Contains(unitSql, "select") || strings.Contains(unitSql, "SELECT") {
				sql = unitSql + ";"
				break
			}
			if unitSql != "" {
				// 说明存在需要开启索引开关的操作
				flags.indexFlag = true
			}
		}
	}
	return sql, flags, err
}

/*getInfo
 * @Description: 获取数据库连接信息
 * @param server
 * @param username
 * @param passwd
 * @param database
 * @return info
 */
func getInfo(server string, username string, passwd string, database string) (info ConnInfo) {
	tmp := strings.Split(server, ":")
	info.host = tmp[0]
	info.port, _ = strconv.Atoi(tmp[1])
	info.user = username
	info.passwd = passwd
	info.dbname = database
	return
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

/**
 * @Description: 将pg库信息转化为相应dsn
 * @param info
 * @return string
 */
func makeDSN(info ConnInfo) string {
	var mode string
	if info.sslmode != "" {
		mode = fmt.Sprintf(" sslmode=%s", info.sslmode)
	}
	pgsqlDSN := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s%s",
		info.host, info.port, info.user, info.passwd, info.dbname, mode)
	return pgsqlDSN
}

/*int2Date
 * @Description: 将整数（YYYYMMDD）形式的日志转化为字符串（YYYYMMDD）形式，如果输入为0则转化为当天日期
 * @param dateInt
 * @return string
 */
func int2Date(dateInt int) string {
	var dayInt, monInt, yearInt int
	if dateInt == 0 {
		var mon time.Month
		dayInt, mon, yearInt = time.Now().Date()
		monInt = int(mon)
	} else {
		dayInt = dateInt % 100
		dateInt /= 100
		monInt = dateInt % 100
		yearInt = dateInt / 100
	}
	return fmt.Sprintf("%02d%02d%02d", yearInt, monInt, dayInt)
}
