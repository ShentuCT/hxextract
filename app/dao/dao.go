package dao

import (
	"github.com/google/wire"
	"github.com/pkg/errors"
	"hxextract/app/cron"
	"hxextract/app/dao/pg"
)

var Provider = wire.NewSet(New, NewDB)

var pgDao pg.Dao

// Dao dao interface
type Dao interface {
	Start() error
	Export(finName string, param pg.QueryParam) error
	Close()
	HealthCheck() error
	// Ping(ctx context.Context) (err error)
	CompareTable(finName string, operation int) (int, int, error)
}

type dao struct {
	DB *DB
}

// New new a dao and return.
func New(db *DB) (d Dao, cf func(), err error) {
	var cleanupPg, cleanupDao func()
	if pgDao == nil {
		pgDao, cleanupPg, err = pg.NewPg()
	}
	if err != nil {
		return
	}
	d, cleanupDao, err = newDao(db)
	cf = func() {
		cleanupPg()
		cleanupDao()
	}
	return
}

func newDao(db *DB) (d *dao, cf func(), err error) {
	d = &dao{
		db,
	}
	cf = d.Close
	return
}

func (d *dao) Start() error {
	// 先开启拓展数据导出后开启定时任务
	d.repExtraExportStart()
	return d.pgCronInit()
}

func (d *dao) Export(finName string, param pg.QueryParam) error {
	// 现根据finname找到对应schema和table
	var table TableInfo
	ok := false
	if table, ok = d.DB.financeInfo[finName]; !ok {
		return errors.New("cant find finance by name")
	}
	param.TableName = table.tableName
	param.SchemaName = table.schemaName
	return d.ExportPgData(param)
}

func (d *dao) Close() {
	cron.Stop()
}

func (d *dao) HealthCheck() error {
	err := d.DB.connectCheck()
	if err != nil {
		return err
	}
	return pgDao.HealthCheck()
}

func (d *dao) CompareTable(finName string, operation int) (int, int, error) {
	var table TableInfo
	ok := false
	if table, ok = d.DB.financeInfo[finName]; !ok {
		return 0, 0, errors.New("cant find finance by name")
	}
	return d.CompareAndUpdateMysql(table.schemaName, table.tableName, operation)
}
