package service

import (
	"context"
	"github.com/google/wire"
	"hxextract/api"
	"hxextract/app/dao"
	"hxextract/app/dao/pg"
)

var Provider = wire.NewSet(New, wire.Bind(new(api.NegtServer), new(*Service)))

// Service 服务层接口
type Service struct {
	dao dao.Dao // 数据层接口
}

func New(d dao.Dao) (s *Service, cf func(), err error) {
	s = &Service{
		dao: d,
	}
	return
}

func (s *Service) Start() error {

	return s.dao.Start()
}

func (s *Service) Close() {
	s.dao.Close()
}

func (s *Service) Ping(ctx context.Context) error {
	return nil
}

func (s *Service) Export(finName string, param pg.QueryParam) error {
	return s.dao.Export(finName, param)
}

func (s *Service) HealthCheck() error {
	return s.dao.HealthCheck()
}

func (s *Service) CompareTable(finName string, operation int) (int, int, error) {
	return s.dao.CompareTable(finName, operation)
}
