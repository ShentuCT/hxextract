package api

import (
	"context"
	"hxextract/app/dao/pg"
)

// NegtServer 对外接口
type NegtServer interface {
	Ping(ctx context.Context) error
	Export(finName string, param pg.QueryParam) error
	HealthCheck() error
	CompareTable(finName string, operation int) (int, int, error)
}
