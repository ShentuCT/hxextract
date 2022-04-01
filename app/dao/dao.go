package dao

import (
	"github.com/google/wire"
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
	CompareTable(finName string, operation int) (int, error)
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
	return d.ExportPgData(finName, param)
}

func (d *dao) Close() {
}

func (d *dao) HealthCheck() error {
	err := d.DB.connectCheck()
	if err != nil {
		return err
	}
	return pgDao.HealthCheck()
}

func (d *dao) CompareTable(finName string, operation int) (int, error) {
	return d.CompareAndDeleteMysqlRecord(finName, operation)
}
