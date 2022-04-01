// +build wireinject
// The build tag makes sure the stub is not built in the final build.

package pg

import "github.com/google/wire"

//go:generate wire
func NewPg() (*pgDao, func(), error) {
	panic(wire.Build(newDao, NewDB))
}
