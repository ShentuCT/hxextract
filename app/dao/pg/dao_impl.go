package pg

import (
	"database/sql"
	"fmt"
	"gorm.io/gorm"
	"hxextract/app/log"
)

type (
	FinanceInfo struct {
		// 待导出财务文件基本信息
		SchemaName string //模式名，和mysql中库名对应
		TableName  string //mysql库表名，和财务文件名一一对应
	}

	// QueryParam 适配老板财务数据导出模式的参数
	QueryParam struct {
		//ProcMethod int    //导出方式 0为直接入库，1为导出.sql文件
		TableName   string
		SchemaName  string
		FinName     string
		StartDate   int
		EndDate     int
		CodeList    string
		ProcType    int
		TriggerType int //触发方式，详见：pg.Trig*
		DsnInfo     string
		ProcSql     string
		SqlType     int //sql类型，详见：pg.Sql
	}
	ExportParam struct {
		FinName string
		QP      QueryParam
	}
)

func (d *pgDao) GetRows(param QueryParam) (*sql.Rows, error) {
	db, err := d.getDsnDb(param.DsnInfo)
	if err != nil {
		return nil, err
	}
	return execFinSql(db, param.ProcSql, param.SqlType)
}

func execFinSql(db *gorm.DB, sql string, flag int) (*sql.Rows, error) {
	log.Log.Info(fmt.Sprintf("exec sql: %s", sql))
	// 处理存储过程
	if flag == SqlStoredProcedure {
		// 多段连续要BEGIN END
		// 获取存储过程的数据需要先执行生成临时缓存之后再通过fetch进一步获取数据
		db.Exec("BEGIN;")
		defer db.Exec("END;") //返回前先END
		row := db.Raw(sql).Row()
		if err := db.Error; err != nil {
			return nil, err
		}
		var strFetchSql string
		err := row.Scan(&strFetchSql)
		if err != nil {
			return nil, err
		}
		// fetch all in "strFetchSql"
		strFetchSql = fmt.Sprintf("fetch all in %q", strFetchSql)
		return db.Raw(strFetchSql).Rows()
	}
	// 处理 非存储过程 的select语句
	if flag == SqlIndex {
		// sql需要开启索引
		db.Exec("set enable_nestloop = on;")        //开启索引
		defer db.Exec("set enable_nestloop = off;") //返回前关闭索引
	}
	return db.Raw(sql).Rows()
}
