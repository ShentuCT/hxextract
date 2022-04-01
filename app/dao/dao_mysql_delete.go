package dao

import (
	"bytes"
	"errors"
	"fmt"
	"go.uber.org/zap"
	"hxextract/app/dao/pg"
	"hxextract/app/log"
	"strconv"
)

const (
	CmpNoDelete  = 1 //对比完不删除数据
	CmpAndDelete = 2 //对比完并删除不一致数据
)

// 删除mysql中的过期数据
// 需要重点考虑请求pg与mysql超时、写mysql对比表超时，可能发生的删除不该删除数据的场景
func (d *dao) CompareAndDeleteMysqlRecord(finName string, operation int) (int, error) {
	// 必须传入schema才能执行检查与删除操作
	if finName == "" {
		return 0, errors.New("no finname recvd!")
	}
	// 全量更新对照表的数据
	tableName, schemaName, err := pgDao.GetTableInfo(finName)
	if err != nil {
		return 0, errors.New("cannot find tableinfo!")
	}
	err = d.CreateCompareTable(finName, schemaName, tableName)
	if err != nil {
		return 0, err
	}
	// 进行对照操作
	deleteRows := 0
	zqdmProd, zqdmCmp, zqdmCommon := d.GetZqdmDiffer(tableName, schemaName)
	// 生产库中比对比库多的证券代码
	if zqdmProd != nil && len(*zqdmProd) > 0 {
		// 先只输出到日志，不删除
		log.Log.Info(fmt.Sprintf("zqdm compare need delete: zqdm=%v", *zqdmProd),
			zap.String("finname", finName))

		if operation == CmpAndDelete {
			for _, zqdm := range *zqdmProd {
				var emptySlice []int32
				deleteRows += d.DeleteMysqlRecord(schemaName, tableName, zqdm, emptySlice)
			}
		}
	}
	// 生产库中比对比库少的证券代码
	if zqdmCmp != nil && len(*zqdmCmp) > 0 {
		log.Log.Info(fmt.Sprintf("zqdm compare need add: zqdm=%v", *zqdmCmp),
			zap.String("finname", finName))
	}
	// 生产库与对比库一致的证券代码，需要对比判断报表日期
	if zqdmCommon == nil {
		return deleteRows, nil
	}
	zqdmCommonCnt := len(*zqdmCommon)
	if zqdmCommonCnt <= 0 {
		return deleteRows, nil
	}
	mapBbrqProd, mapBbrqCmp := d.GetBbrqDiffer(tableName, schemaName, zqdmCommon)
	if len(*mapBbrqProd) > 0 {
		for key, val := range *mapBbrqProd {
			log.Log.Info(fmt.Sprintf("bbrq compare need delete: zqdm=%s, bbrq=%v", key, val),
				zap.String("finname", finName))
			if operation == CmpAndDelete {
				deleteRows += d.DeleteMysqlRecord(schemaName, tableName, key, val)
			}
		}
	}
	if len(*mapBbrqCmp) > 0 {
		for key, val := range *mapBbrqCmp {
			log.Log.Info(fmt.Sprintf("bbrq compare need add: zqdm=%s, bbrq=%v", key, val),
				zap.String("finname", finName))
		}
	}
	return deleteRows, nil
}

// 这里只负责往已经存在的表里塞入数据，不负责表的创建
func (d *dao) CreateCompareTable(finName string, schemaName string, tableName string) error {
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
		ProcType:  pg.OpAll,
		StartDate: 0,
		EndDate:   0,
		CodeList:  "",
	}
	rows, err := pgDao.GetRows(finName, pgParam)
	if err != nil {
		return err
	}
	sqlList, err := d.rows2sqls(pg.FinanceInfo{SchemaName: schemaName, FinName: finName, TableName: tableName}, rows, false)
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
