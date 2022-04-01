package dao

import (
	"bytes"
	"database/sql"
	"fmt"
	"gorm.io/gorm"
	"hxextract/app/dao/orm"
	"hxextract/app/log"
)

// 导入
func (d *dao) executeImportSql(db *sql.DB, sqlBytes *bytes.Buffer) error {
	_, err := db.Exec(sqlBytes.String())
	return err
}

func (d *dao) executeImportSqls(db *sql.DB, sqlList []*bytes.Buffer) (int64, error) {
	rCh := make(chan sql.Result)
	eCh := make(chan error)
	cnt := 0
	var nRowsAffected int64 = 0
	for cnt < len(sqlList) {
		select {
		case result := <-rCh:
			num, _ := result.RowsAffected()
			nRowsAffected += num
			cnt++
		}
	}
	err := <-eCh
	return nRowsAffected, err
}

func (d *dao) executeSqls(db *sql.DB, sqlList []*bytes.Buffer, rCh chan sql.Result, eCh chan error) {
	for _, sqlBytes := range sqlList {
		sqlStr := sqlBytes.String()
		go func() {
			res, err := db.Exec(sqlStr)
			if err != nil {
				log.Log.Error(err.Error())
				eCh <- err
				rCh <- res
			} else {
				rCh <- res
			}
		}()
	}
}

func (d *dao) dealSqlResults(rCh chan sql.Result) {

}

// DataQuery  TODO
func DataQuery(db *gorm.DB, table interface{}) (interface{}, error) {
	db.Where("code in ?").Find(&table)
	return table, db.Error
}

/*DataUpdate
 * @Description: 逐行向Mysql更新数据，用于少量数据更新
 * @param schemaName
 * @param tableName
 * @param dataRows
 */
func (d *dao) DataUpdate(schemaName string, tableName string, dataRows []interface{}) {
	// TODO
}

/*DataChangeValid
 * @Description: 修改数据置否信息
 * @param schemaName
 * @param tableName
 * @param finKey
 * @param flg
 */
func (d *dao) DataChangeValid(schemaName string, tableName string, finKey orm.FinPrimaryKey, flg bool) {
	isvalid := 0
	if flg {
		isvalid = 1
	}
	db, err := d.DB.getConn(schemaName)
	if err != nil {
		// TODO
	}
	sqlQuery := fmt.Sprintf("UPDATE table %s set `isvalid` = %d  where code = '%s' and datetime = %d;",
		tableName, isvalid, finKey.Code, finKey.Datetime)
	db.Exec(sqlQuery)
}

/*DataDelete
 * @Description: 删除单条数据，谨慎使用，如无必要请使用置否
 * @param schemaName
 * @param tableName
 * @param finKey
 */
func (d *dao) DataDelete(schemaName string, tableName string, finKey orm.FinPrimaryKey) {
	db, err := d.DB.getConn(schemaName)
	if err != nil {
		// TODO
	}
	sqlQuery := fmt.Sprintf("DELETE from %s where zqdm = '%s' and bbrq = %d;",
		tableName, finKey.Code, finKey.Datetime)
	db.Exec(sqlQuery)
}
