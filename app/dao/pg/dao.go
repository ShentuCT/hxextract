package pg

import (
	"database/sql"
	"github.com/google/wire"
)

var Provider = wire.NewSet(New, NewDB)

type Dao interface {
	Close()
	GetRows(param QueryParam) (*sql.Rows, error)
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

func (d *pgDao) HealthCheck() error {
	return d.connectCheck()
}
