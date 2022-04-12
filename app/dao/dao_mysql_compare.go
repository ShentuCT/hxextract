package dao

import (
	"bytes"
	"errors"
	"fmt"
	"go.uber.org/zap"
	"hxextract/app/dao/pg"
	"hxextract/app/log"
	"strconv"
	"strings"
)

const (
	CmpAndDelete = 1 //对比完并删除不一致数据
	CmpAndAdd    = 2 //对比后补全缺失的数据
)

// 需要重点考虑请求pg与mysql超时、写mysql对比表超时，可能发生的删除不该删除数据的场景
func (d *dao) CompareAndUpdateMysql(schemaName string, tableName string, operation int) (int, int, error) {
	err := d.CreateCompareTable(schemaName, tableName)
	if err != nil {
		return 0, 0, err
	}
	// 进行对照操作
	deleteRows := 0
	insertRows := 0
	zqdmProd, zqdmCmp, zqdmCommon, err := d.GetZqdmDiffer(tableName, schemaName)
	if err != nil {
		return 0, 0, err
	}
	// 生产库中比对比库多的证券代码
	if zqdmProd != nil && len(*zqdmProd) > 0 {
		// 先只输出到日志，不删除
		log.Log.Info(fmt.Sprintf("zqdm compare need delete: zqdm=%v", *zqdmProd),
			zap.String("schema", schemaName),
			zap.String("table", tableName))

		if operation&CmpAndDelete != 0 {
			for _, zqdm := range *zqdmProd {
				var emptySlice []int32
				deleteRows += d.DeleteMysqlRecord(schemaName, tableName, zqdm, emptySlice)
			}
		}
	}
	// 生产库中比对比库少的证券代码
	if zqdmCmp != nil && len(*zqdmCmp) > 0 {
		log.Log.Info(fmt.Sprintf("zqdm compare need add: zqdm=%v", *zqdmCmp),
			zap.String("schema", schemaName),
			zap.String("table", tableName))
		// todo：补全数据
		if operation&CmpAndAdd != 0 {
			for _, val := range *zqdmCmp {
				insert, err := d.InsertMysqlRecordFromCompare(schemaName, tableName, val, nil)
				if err != nil {
					log.Log.Warn("compare insert failed",
						zap.String("schema", schemaName),
						zap.String("table", tableName),
						zap.String("err", err.Error()))
				}
				insertRows += int(insert)
			}
		}
	}
	// 生产库与对比库一致的证券代码，需要对比判断报表日期
	if zqdmCommon == nil {
		return deleteRows, insertRows, nil
	}
	zqdmCommonCnt := len(*zqdmCommon)
	if zqdmCommonCnt <= 0 {
		return deleteRows, insertRows, nil
	}
	mapBbrqProd, mapBbrqCmp, err := d.GetBbrqDiffer(tableName, schemaName, zqdmCommon)
	if err != nil {
		return deleteRows, insertRows, err
	}
	if len(*mapBbrqProd) > 0 {
		for key, val := range *mapBbrqProd {
			log.Log.Info(fmt.Sprintf("bbrq compare need delete: zqdm=%s, bbrq=%v", key, val),
				zap.String("schema", schemaName),
				zap.String("table", tableName))
			if operation&CmpAndDelete != 0 {
				deleteRows += d.DeleteMysqlRecord(schemaName, tableName, key, val)
			}
		}
	}
	if len(*mapBbrqCmp) > 0 {
		for key, val := range *mapBbrqCmp {
			log.Log.Info(fmt.Sprintf("bbrq compare need add: zqdm=%s, bbrq=%v", key, val),
				zap.String("schema", schemaName),
				zap.String("table", tableName))
			// todo：补全数据
			if operation&CmpAndAdd != 0 {
				insert, err := d.InsertMysqlRecordFromCompare(schemaName, tableName, key, val)
				if err != nil {
					log.Log.Warn("compare insert failed",
						zap.String("schema", schemaName),
						zap.String("table", tableName),
						zap.String("err", err.Error()))
				}
				insertRows += int(insert)
			}
		}
	}
	return deleteRows, insertRows, nil
}

// 这里只负责往已经存在的表里塞入数据，不负责表的创建
func (d *dao) CreateCompareTable(schemaName string, tableName string) error {
	// 获取mysql连接，schema需要提前手动创建好
	schemaCmp := "compare_" + schemaName
	db, err := d.DB.getConn(schemaCmp)
	if err != nil {
		return err
	}
	// 先把对照表的数据删除
	sqlDel := fmt.Sprintf("delete from %s", tableName)
	_, err = db.Exec(sqlDel)

	// 生成对照表的记录
	pgParam := pg.QueryParam{
		SchemaName: schemaName,
		TableName:  tableName,
		ProcType:   pg.OpAll,
		StartDate:  0,
		EndDate:    0,
		CodeList:   "",
	}
	// 找到对应的pg数据库信息
	schema := make(SchemaInfo)
	var ok bool
	if schema, ok = d.DB.gTableInfo[schemaName]; !ok {
		return errors.New("can't find dsn")
	}
	var table TableInfo
	if table, ok = schema[tableName]; !ok {
		return errors.New("can't find dsn")
	}
	pgParam.DsnInfo = table.dsnInfo
	// 生成sql
	sql, flag, err := d.getProc(pgParam)
	if err != nil {
		return err
	}
	pgParam.ProcSql = sql
	pgParam.SqlType = flag
	// 导出数据
	rows, err := pgDao.GetRows(pgParam)
	if err != nil {
		return err
	}
	sqlList, err := d.rows2sqls(pg.FinanceInfo{SchemaName: schemaName, TableName: tableName}, rows, false)
	if err != nil {
		return err
	}
	// 通过sql语句更新mysql，这里不创建多个go routine，避免影响mysql的性能
	for _, sqlBytes := range sqlList {
		sqlStr := sqlBytes.String()
		result, unitErr := db.Exec(sqlStr)
		if unitErr != nil {
			log.Log.Error(unitErr.Error())
		} else {
			lastInsertId, _ := result.LastInsertId()
			affectRows, _ := result.RowsAffected()
			log.Log.Info("", zap.Int64("Id", lastInsertId), zap.Int64("affected rows", affectRows))
		}
	}
	return nil
}

func (d *dao) DeleteMysqlRecord(schemaName string, tableName string, zqdm string, bbrq []int32) int {
	// 获取连接
	deleteRow := 0
	db, err := d.DB.getConn(schemaName)
	if err != nil {
		return deleteRow
	}
	// 创建sql
	sqlDelete := fmt.Sprintf("delete from %s where zqdm = \"%s\"", tableName, zqdm)
	lenBbrq := len(bbrq)
	if lenBbrq > 0 {
		querySql := bytes.Buffer{}
		for index, val := range bbrq {
			querySql.WriteString(strconv.Itoa(int(val)))
			if index < (lenBbrq - 1) {
				querySql.WriteByte(',')
			}
		}
		sqlDelete = fmt.Sprintf("%s and bbrq in (%s)", sqlDelete, querySql.String())
	}
	// 执行删除操作
	result, unitErr := db.Exec(sqlDelete)
	if unitErr != nil {
		log.Log.Error(unitErr.Error())
	} else {
		lastInsertId, _ := result.LastInsertId()
		deleteRow, _ := result.RowsAffected()
		log.Log.Info("delete table succeed", zap.String("schema", schemaName), zap.String("table", tableName), zap.String("zqdm", zqdm), zap.Int64("Id", lastInsertId), zap.Int64("affected rows", deleteRow))
	}
	return deleteRow
}

// 补全对比后缺失的数据
func (d *dao) InsertMysqlRecordFromCompare(schemaName string, tableName string, zqdm string, bbrq []int32) (int64, error) {
	// 从对比表获取待补全的数据
	schemaCmp := "compare_" + schemaName
	dbCmpHandler, err := d.DB.getConn(schemaCmp)
	if err != nil {
		return 0, err
	}
	sqlSrc := fmt.Sprintf("select * from %s where zqdm = '%s'", tableName, zqdm)
	if bbrq != nil && len(bbrq) > 0 {
		var bbrqList string
		for _, val := range bbrq {
			bbrqList += fmt.Sprintf("%d,", val)
		}
		bbrqList = strings.TrimRight(bbrqList, ",")
		sqlSrc += fmt.Sprintf(" and bbrq in (%s)", bbrqList)
	}
	rowsSrc, err := dbCmpHandler.Query(sqlSrc)
	if err != nil {
		return 0, err
	}
	defer rowsSrc.Close()
	sqlList, err := d.rows2sqls(pg.FinanceInfo{SchemaName: schemaName, TableName: tableName}, rowsSrc, false)
	// 将待补全的数据写入生产表
	dbProdHandler, err := d.DB.getConn(schemaName)
	if err != nil {
		return 0, err
	}
	var rowCnt int64
	rowCnt = 0
	for _, sqlBytes := range sqlList {
		sqlStr := sqlBytes.String()
		result, unitErr := dbProdHandler.Exec(sqlStr)
		if unitErr != nil {
			log.Log.Error(unitErr.Error())
		} else {
			lastInsertId, _ := result.LastInsertId()
			affectRows, _ := result.RowsAffected()
			rowCnt += affectRows
			log.Log.Info("InsertMysqlRecordFromCompare",
				zap.String("schema", schemaName),
				zap.String("table", tableName),
				zap.String("zqdm", zqdm),
				zap.Int64("Id", lastInsertId),
				zap.Int64("affected rows", affectRows))
		}
	}
	return rowCnt, nil
}
