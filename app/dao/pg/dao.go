package pg

import (
	"bytes"
	"database/sql"
	"github.com/google/wire"
)

var Provider = wire.NewSet(New, NewDB)

type Dao interface {
	Close()
	GetRows(param QueryParam) (*sql.Rows, error)
	GetTableName(finName string) (string, error)
	GetSchema(finName string) (string, error)
	GetTableInfo(finName string) (tableName string, schemaName string, err error)
	GetTaskFins(taskName string) ([]string, error)
	HealthCheck() error
}

type pgDao struct {
	*DB
}

// New new a dao and return.
func New(db *DB) (d Dao, cf func(), err error) {
	return newDao(db)
}

func newDao(db *DB) (d *pgDao, cf func(), err error) {
	d = &pgDao{
		db,
	}
	cf = d.Close
	return
}

func (d *pgDao) Close() {

}

func (d *pgDao) StartCronTask() {

}

func (d *pgDao) HealthCheck() error {
	return d.connectCheck()
}

func (d *pgDao) ExportSql(fin FinanceInfo, param QueryParam) ([]*bytes.Buffer, error) {
	//rows ,err:= d.GetRows(fin.FinName,param)
	//if err != nil {
	//	return nil, err
	//}
	//if fin.TableName == ""{
	//	fin.TableName,err = d.DB.GetTableName(fin.FinName)
	//	if err != nil {
	//		return nil, err
	//	}
	//}

	//sqls,err:= rows2sqls(fin,rows)
	return nil, nil
}

func (d *pgDao) ExportData() {

}
