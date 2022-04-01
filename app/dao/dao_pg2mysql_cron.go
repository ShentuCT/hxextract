package dao

import (
	"fmt"
	"go.uber.org/zap"
	"hxextract/app/cron"
	"hxextract/app/dao/pg"
	"hxextract/app/log"
	"sort"
	"strings"
	"time"
)

var (
	cronJob     *pg.CronJob
	lastJobTime int
)

var (
	cronParamRtime = pg.QueryParam{
		ProcType:    pg.OpRtime,
		TriggerType: pg.TrigCron,
	}
	cronParamBbrq = pg.QueryParam{
		ProcType:    pg.OpBbrq,
		TriggerType: pg.TrigCron,
	}
	// 最多重试三次，先拍个脑袋
	retryCnt = 3
)

// 初始化日期，定时任务执行前一天到当天
func cronInit() {
	tToday := time.Now()
	tYesterday := tToday.AddDate(0, 0, -1)
	y, m, d := tToday.Date()
	cronParamRtime.EndDate = d + 100*int(m) + 10000*y
	cronParamBbrq.EndDate = d + 100*int(m) + 10000*y
	y, m, d = tYesterday.Date()
	cronParamRtime.StartDate = d + 100*int(m) + 10000*y
	cronParamBbrq.StartDate = d + 100*int(m) + 10000*y
}

func (d *dao) pgCronInit() error {
	if err := pgDao.TaskCronLoad(); err != nil {
		return err
	}
	cronJob = pgDao.GetCronJob()
	h, m, s := time.Now().Clock()
	lastJobTime = h*10000 + m*100 + s
	spec := "0 * * * * ?" //cron表达式，每分钟触发一次
	return cron.AddFunc(spec, d.exportCron)
}

func (d *dao) exportCron() {
	h, m, s := time.Now().Clock()
	nowClock := h*10000 + m*100 + s
	if lastJobTime < 0 {
		lastJobTime = nowClock
	}
	cronKeys := cronJob.Keys()
	if len(cronKeys) == 0 {
		return
	}
	sort.Ints(cronKeys)
	// 当前时间已小于最小时间，但是上次的记录却大于当前时间，说明已跨天，需要重置任务记录
	if nowClock < cronKeys[0] && lastJobTime > cronKeys[0] {
		// 正常来说跨天之后上一次记录应该是最后的记录，若仍有未执行任务，则将其一起执行再
		if lastJobTime < cronKeys[len(cronKeys)-1] {
			for _, key := range cronKeys {
				if nowClock > lastJobTime {
					taskNames := cronJob.Get(key)
					go d.TasksExport(taskNames)
				}
			}
		}
		// 重置任务记录
		lastJobTime = nowClock
	}
	for _, key := range cronKeys {
		if key <= lastJobTime {
			continue
		}
		if key > nowClock {
			break
		}
		if cronJob.Get(key) == "" {
			continue
		}
		taskNames := cronJob.Get(key)
		lastJobTime = key
		go d.TasksExport(taskNames)
	}
}

func (d *dao) TasksExport(taskNames string) {
	tasks := strings.Split(taskNames, ",")
	// 去重去空
	tasks = removeDuplicateElement(tasks)
	cronInit()
	for _, taskName := range tasks {
		log.Log.Info(fmt.Sprintf("start to export finance of task:%s", taskName),
			zap.String("taskname", taskName))
		fins, err := pgDao.GetTaskFins(taskName)
		if err != nil {
			log.Log.Error(fmt.Sprintf("get finnames from task failed: %s", err.Error()),
				zap.String("taskname", taskName))
			continue
		}
		for _, fin := range fins {
			d.exportFinCron(fin, cronParamRtime, retryCnt)
			// 部分sql问题，通过bbrq再导一次
			d.exportFinCron(fin, cronParamBbrq, retryCnt)
		}
	}
}

func (d *dao) exportFinCron(finName string, param pg.QueryParam, retry int) {
	for i := 0; i < retry; i++ {
		err := d.ExportPgData(finName, param)
		if err == nil {
			log.Log.Info(fmt.Sprintf("export data successfully"),
				zap.String("finname", finName),
				zap.String("type", "cron"),
				zap.Int("retry", i))
			break
		}
		if i == retry-1 {
			log.Log.Error(fmt.Sprintf("export data failed: %s", err.Error()),
				zap.String("finname", finName),
				zap.String("type", "cron"),
				zap.Int("retry", i))
		} else {
			log.Log.Error(fmt.Sprintf("export data failed: %s,start to retry", err.Error()),
				zap.String("finname", finName),
				zap.String("type", "cron"))
		}
		time.Sleep(time.Duration(5) * time.Second)
	}
}

/*removeDuplicateElement
 * @Description: 去重去空
 * @param stringSlice
 * @return []string
 */
func removeDuplicateElement(stringSlice []string) []string {
	result := make([]string, 0, len(stringSlice))
	temp := map[string]struct{}{}
	for _, item := range stringSlice {
		if item == "" { // 去空
			continue
		}
		if _, ok := temp[item]; !ok { // 去重
			temp[item] = struct{}{}
			result = append(result, item)
		}
	}
	return result
}
