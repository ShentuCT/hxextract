package dao

import (
	"errors"
	"fmt"
	"go.uber.org/zap"
	"hxextract/app/cron"
	"hxextract/app/dao/orm"
	"hxextract/app/dao/pg"
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
	TableInfo struct {
		tableName  string
		schemaName string
		finName    string
		allProc    string
		repProc    string
		finProc    string
		codeProc   string
		dsnInfo    string
	}
	TaskItem struct {
		tableName  string
		schemaName string
		opType     int
	}

	SchemaInfo  map[string]TableInfo
	FinnameInfo map[string]TableInfo
)

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
		yearInt, mon, dayInt = time.Now().Date()
		monInt = int(mon)
	} else {
		dayInt = dateInt % 100
		dateInt /= 100
		monInt = dateInt % 100
		yearInt = dateInt / 100
	}
	return fmt.Sprintf("%02d%02d%02d", yearInt, monInt, dayInt)
}

func (t *TableInfo) getSql(op int) string {
	if op == pg.OpAll {
		return t.allProc
	} else if op == pg.OpBbrq {
		return t.repProc
	} else if op == pg.OpRtime {
		return t.finProc
	} else if op == pg.OpCode {
		return t.codeProc
	}
	return ""
}

// 加载财务表信息
func (d *dao) tableinfoDbLoad() error {
	log.Log.Info("init table info")
	var result []orm.TableInfo
	d.DB.defaultOrm.Table("TableInfo").Find(&result)
	if d.DB.defaultOrm.Error != nil {
		return d.DB.defaultOrm.Error
	}
	if len(result) == 0 {
		return errors.New("no table info found")
	}
	// 每个表缓存其对应的连接信息，要获取pg连接时，使用连接信息去map中查找
	d.DB.financeInfo = make(FinnameInfo)
	d.DB.gTableInfo = make(map[string]SchemaInfo)
	for _, v := range result {
		dsn := makeDSN(getInfo(v.Server, v.User, v.Passwd, v.Database))
		tableinfo := TableInfo{
			tableName:  v.TableName,
			schemaName: v.SchemaName,
			finName:    v.FinName,
			dsnInfo:    dsn,
			allProc:    v.AllProc,
			repProc:    v.RepProc,
			finProc:    v.FinProc,
			codeProc:   v.CodeProc,
		}
		if _, ok := d.DB.gTableInfo[v.SchemaName]; !ok {
			d.DB.gTableInfo[v.SchemaName] = make(SchemaInfo)
		}
		d.DB.gTableInfo[v.SchemaName][v.TableName] = tableinfo
		d.DB.financeInfo[v.FinName] = tableinfo
	}
	return nil
}

// 加载定时任务信息
func (d *dao) taskitemsDbLoad() error {
	log.Log.Info("init task items")
	var result []orm.TaskItems
	d.DB.defaultOrm.Table("TaskItems").Find(&result)
	if d.DB.defaultOrm.Error != nil {
		return d.DB.defaultOrm.Error
	}
	if len(result) == 0 {
		return errors.New("no task items found")
	}
	tasks := 0
	cron.InitCron()
	for _, v := range result {
		taskitem := TaskItem{
			tableName:  v.TableName,
			schemaName: v.SchemaName,
			opType:     v.Export,
		}
		croninfo := CronTaskInfo{
			taskinfo:    taskitem,
			processFunc: d.exportFinCron,
		}
		// 获取所有时间
		times := strings.Split(v.Cron, ";")
		for _, val := range times {
			if val == "" {
				continue
			}
			taskname := taskitem.schemaName + taskitem.tableName
			err := cron.AddTask(taskname, val, croninfo.CronTasksExport)
			if err != nil {
				log.Log.Warn(fmt.Sprintf("add task failed"),
					zap.String("table", taskitem.tableName),
					zap.String("schema", taskitem.schemaName),
					zap.String("crontime", val))
				continue
			}
			tasks++
		}
	}
	cron.Start()
	log.Log.Info("cronjob load finished", zap.Int("tasks", tasks))
	return nil
}

func (d *dao) getProc(para pg.QueryParam) (string, int, error) {
	flag := pg.SqlNormal
	schema := make(SchemaInfo)
	var ok bool
	if schema, ok = d.DB.gTableInfo[para.SchemaName]; !ok {
		return "", flag, errors.New("can't find schema")
	}
	var table TableInfo
	if table, ok = schema[para.TableName]; !ok {
		return "", flag, errors.New("can't find table")
	}
	sql := table.getSql(para.ProcType)

	if para.ProcType == pg.OpBbrq || para.ProcType == pg.OpRtime {
		sql = strings.Replace(sql, "[start]", int2Date(para.StartDate), 1)
		sql = strings.Replace(sql, "[end]", int2Date(para.EndDate), 1)
	} else if para.ProcType == pg.OpCode {
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
		flag = pg.SqlStoredProcedure
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
				flag = pg.SqlIndex
			}
		}
	}
	return sql, flag, nil
}
