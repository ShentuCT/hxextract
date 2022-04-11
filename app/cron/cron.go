package cron

import (
	"github.com/robfig/cron/v3"
	"time"
)

/**
  本接口非线程安全，注意使用场景
*/
type (
	CronEntryInfo map[string]cron.EntryID
	Manager       struct {
		cron           *cron.Cron
		scheduleDetail map[string]CronEntryInfo
	}
)

var manager Manager

func InitCron() {
	loc, _ := time.LoadLocation("Asia/Shanghai")
	manager.cron = cron.New(cron.WithLocation(loc))
	manager.scheduleDetail = make(map[string]CronEntryInfo)
}

func Start() {
	manager.cron.Start()
}

func Stop() {
	manager.cron.Stop()
}

func AddTask(task string, schedule string, cronFunc func()) error {
	_, ok := manager.scheduleDetail[task]
	if !ok {
		manager.scheduleDetail[task] = make(CronEntryInfo)
	}
	_, ok = manager.scheduleDetail[task][schedule]
	if ok {
		return nil
	}
	id, err := manager.cron.AddFunc(schedule, cronFunc)
	if err != nil {
		return err
	}
	manager.scheduleDetail[task][schedule] = id
	return nil
}

func RemoveTask(task string) {
	taskDetail, ok := manager.scheduleDetail[task]
	if !ok {
		return
	}
	for _, v := range taskDetail {
		manager.cron.Remove(v)
	}
	delete(manager.scheduleDetail, task)
}
