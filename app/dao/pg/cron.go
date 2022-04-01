package pg

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
)

var (
	cronTab = make(map[int]string) // 用于存储所有任务的定时任务时间
	cronJob = &CronJob{
		jobs: make(map[int]string),
		lock: new(sync.RWMutex),
	}
)

type CronJob struct {
	jobs map[int]string
	lock *sync.RWMutex
}

func (c *CronJob) Get(key int) string {
	c.lock.RLock()
	defer c.lock.RUnlock()
	return c.jobs[key]
}

func (c *CronJob) Set(key int, value string) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.jobs[key] = value
}

func (c *CronJob) Delete(key int) {
	c.lock.Lock()
	defer c.lock.Unlock()
	delete(c.jobs, key)
}

func (c *CronJob) Len() int {
	c.lock.RLock()
	defer c.lock.RUnlock()
	return len(c.jobs)
}

func (c *CronJob) Keys() []int {
	c.lock.RLock()
	defer c.lock.RUnlock()
	keys := make([]int, 0, len(c.jobs))
	for k := range c.jobs {
		keys = append(keys, k)
	}
	return keys
}

func (c *CronJob) Values() []string {
	c.lock.RLock()
	defer c.lock.RUnlock()
	values := make([]string, 0, len(c.jobs))
	for _, v := range c.jobs {
		values = append(values, v)
	}
	return values
}

func (c *CronJob) Items() map[int]string {
	c.lock.RLock()
	defer c.lock.RUnlock()
	items := make(map[int]string, len(c.jobs))
	for k, v := range c.jobs {
		items[k] = v
	}
	return items
}

func (c *CronJob) Clear() {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.jobs = make(map[int]string)
}

func (d *pgDao) TaskCronLoad() error {
	cronJob.Clear()
	querySql := fmt.Sprintf("SELECT distinct taskname,cron from %s.%s where export = 2;",
		d.DB.tables.schemaName, d.DB.tables.taskInfo)
	rows, err := d.DB.baseDB.Raw(querySql).Rows() // 使用原生sql进行查询
	if err != nil {
		return err
	}
	var taskName, cron string
	//逐行处理查询结果
	for rows.Next() {
		err = rows.Scan(&taskName, &cron)
		if err != nil {
			return err
		} else {
			appendCron(taskName, cron)
		}
	}
	fmt.Println("cronTab:", cronJob)
	return nil
}

func (d *pgDao) GetCronJob() *CronJob {
	return cronJob
}

/*appendCron
 * @Description: 将数据库获得的map
 * @param crons
 */
func appendCron(taskName string, cron string) {
	crons := splitCron(cron)
	for _, v := range crons {
		cronJob.Set(v, cronJob.Get(v)+taskName+",")
		cronTab[v] += taskName + ","
	}
}

/*splitCron
 * @Description: 将数据库中记录的日期字符串转化为整数切片
 * @param crons: 财务文件定时任务格式一般为yy:mm;yy:mm;yy:mm
 * @return cronTab
 */
func splitCron(cron string) []int {
	var crons []int
	times := strings.Split(cron, ";")
	for _, v := range times {
		if v == "" {
			continue
		}
		ti := getTime(v)
		crons = append(crons, ti)
	}
	return crons
}

/*getTime
 * @Description: 将时分转化成整数形式
 * @param ti yy:mm格式的字符串（也支持yy:mm:ss
 * @return T 时间
 */
func getTime(ti string) (T int) {
	hms := strings.Split(ti, ":")
	hour, _ := strconv.Atoi(hms[0])
	minute, second := 0, 0
	//防止瞎写
	if len(hms) == 3 { //时分或时分秒
		minute, _ = strconv.Atoi(hms[1])
		second, _ = strconv.Atoi(hms[2])
	} else if len(hms) == 2 {
		minute, _ = strconv.Atoi(hms[1])
	}
	// 防止有人瞎写
	hour %= 24
	minute %= 60
	second %= 60
	T = hour*10000 + minute*100 + second
	return
}
