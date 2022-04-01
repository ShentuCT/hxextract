package cron

import (
	"github.com/robfig/cron/v3"
)

func AddFunc(spec string, cronFunc func()) error {
	crontab := cron.New(cron.WithSeconds())
	_, err := crontab.AddFunc(spec, cronFunc)
	if err != nil {
		return err
	}
	crontab.Start()
	return nil
}
