package dao

import (
	"fmt"
	"go.uber.org/zap"
	"hxextract/app/dao/pg"
	"hxextract/app/log"
	"time"
)

type CronTaskInfo struct {
	taskinfo    TaskItem
	processFunc func(pg.QueryParam, int)
}

var (
	// 最多重试三次，先拍个脑袋
	retryCnt = 3
)

func (d *CronTaskInfo) CronTasksExport() {
	log.Log.Info(fmt.Sprintf("start to export finance"),
		zap.String("table", d.taskinfo.tableName),
		zap.String("schema", d.taskinfo.schemaName))
	param := pg.QueryParam{
		TableName:   d.taskinfo.tableName,
		SchemaName:  d.taskinfo.schemaName,
		ProcType:    d.taskinfo.opType,
		StartDate:   0,
		EndDate:     0,
		TriggerType: pg.TrigCron,
	}
	d.processFunc(param, retryCnt)
	// 部分sql问题，通过bbrq再导一次
	// d.processFunc(fin, cronParamBbrq, retryCnt)
}

func (d *dao) exportFinCron(param pg.QueryParam, retry int) {
	for i := 0; i < retry; i++ {
		err := d.ExportPgData(param)
		if err == nil {
			log.Log.Info(fmt.Sprintf("export data successfully"),
				zap.String("finname", param.FinName),
				zap.String("type", "cron"),
				zap.Int("retry", i))
			break
		}
		if i == retry-1 {
			log.Log.Error(fmt.Sprintf("export data failed: %s", err.Error()),
				zap.String("finname", param.FinName),
				zap.String("type", "cron"),
				zap.Int("retry", i))
		} else {
			log.Log.Error(fmt.Sprintf("export data failed: %s,start to retry", err.Error()),
				zap.String("finname", param.FinName),
				zap.String("type", "cron"))
		}
		time.Sleep(time.Duration(5) * time.Second)
	}
}
