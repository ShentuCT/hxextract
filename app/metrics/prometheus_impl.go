package metrics

import (
	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"hxextract/app/dao/pg"
)

// 导出方式export
var exportTypeDict = map[int]string{
	pg.OpAll:   "all",   //全量导出
	pg.OpBbrq:  "bbrq",  //按日期导出
	pg.OpRtime: "rtime", //按rtime导出
	pg.OpReal:  "real",  //按实时更新导出
	pg.OpCode:  "code",  //按代码导出
}

// 触发方式
var trigTypeDict = map[int]string{
	pg.TrigCron:   "cron",   //全量导出
	pg.TrigManual: "manual", //按日期导出
}

// 错误阶段
const (
	ErrorDefault  = "default"          //完整阶段
	ErrorTablenfo = "tableinfo"        //获取表信息
	ErrorConn     = "mysql_connection" //与mysql建立连接
	ErrorPg       = "postgres_req"     //与pg建立连接并请求数据
	ErrorSink     = "sink"             //数据持久化
)

// 任务阶段stage
const (
	StageAll       = "all"       //完整阶段
	StageExtract   = "extract"   //抽取
	StageTransform = "transform" //转换
	StageLoad      = "load"      //持久化
)

// 数据源source
const (
	SourcePg      = "postgresql" //pg数据库
	SourceArsenal = "arsenal"    //中台
)

// 任务触发方式type
const (
	TypeCron   = "cron"   //定时任务
	TypeManual = "manual" //手动触发
)

var (
	// 各队列任务数
	// stage: extract proccess load
	queueGaugeVec = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "queue_number",
			Help: "Current number of queues.",
		},
		[]string{"stage"},
	)

	// 接收流量
	// source: pg cbas arsenal
	rxTrafficConterVec = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "recv_data_kb_total",
			Help: "Size of data received.",
		},
		[]string{"source"},
	)

	// qps
	// trigger: cron manual
	qpsConterVec = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "qps",
			Help: "Request reseived.",
		},
		[]string{"schema", "table", "trigger", "export"},
	)

	// 处理耗时分布
	// trigger: cron manual
	// stage: extract proccess load
	// export: all bbrq rtime real code
	perfReqBucketVec = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "perf_req",
			Help:    "Cost of time for each request.",
			Buckets: []float64{50, 500, 3000, 10000, 30000, 180000, 600000},
		},
		[]string{"schema", "table", "trigger", "stage", "export"},
	)

	// 错误数
	errorConterVec = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "errors",
			Help: "Errors occured.",
		},
		[]string{"trigger", "schema", "table", "export", "statuscode"},
	)

	// 处理函数
	promHttpHandler = gin.WrapH(promhttp.Handler())
)

func GetExportType(export int) string {
	val, ok := exportTypeDict[export]
	if !ok {
		return ""
	}
	return val
}

func GetTriggerType(trigger int) string {
	val, ok := trigTypeDict[trigger]
	if !ok {
		return ""
	}
	return val
}

// 指标初始化
func InitMetrics() {
	prometheus.MustRegister(queueGaugeVec)
	prometheus.MustRegister(rxTrafficConterVec)
	prometheus.MustRegister(qpsConterVec)
	prometheus.MustRegister(perfReqBucketVec)
	prometheus.MustRegister(errorConterVec)
}

// 指标结果获取接口
func GetMetrics(c *gin.Context) {
	promHttpHandler(c)
}

// 队列指标统计接口
func QueueMetricsInc(stage string) {
	queueGaugeVec.WithLabelValues(stage).Inc()
}

func QueueMetricsDec(stage string) {
	queueGaugeVec.WithLabelValues(stage).Dec()
}

// 流量指标统计接口
func TrafficMetricsAdd(source string, kb float64) {
	rxTrafficConterVec.WithLabelValues(source).Add(kb)
}

// qps统计接口
func QpsMetricsInc(schema string, table string, trigger string, export string) {
	qpsConterVec.WithLabelValues(schema, table, trigger, export).Inc()
}

// 处理耗时统计接口
func PerfBucketMetricsObserve(schema string, table string, trigger string, stage string, export string, costMs float64) {
	perfReqBucketVec.WithLabelValues(schema, table, trigger, stage, export).Observe(costMs)
}

// 错误统计接口
func ErrorMetricsInc(trigger string, schema string, table string, export string, errorcode string) {
	errorConterVec.WithLabelValues(trigger, schema, table, export, errorcode).Inc()
}
