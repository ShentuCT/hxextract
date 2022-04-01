// Code generated by Wire. DO NOT EDIT.

//go:generate go run github.com/google/wire/cmd/wire
//+build !wireinject

package pg

// Injectors from wire.go:

//go:generate wire
func NewPg() (*pgDao, func(), error) {
	db, cleanup, err := NewDB()
	if err != nil {
		return nil, nil, err
	}
	pgPgDao, cleanup2, err := newDao(db)
	if err != nil {
		cleanup()
		return nil, nil, err
	}
	return pgPgDao, func() {
		cleanup2()
		cleanup()
	}, nil
}