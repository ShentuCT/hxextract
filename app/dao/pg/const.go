package pg

//pg相关需要导出的常数

// 请求相关参数
const (
	FINNAME   = "finname" //财务文件名
	STARTDATE = "startdate"
	ENDDATE   = "enddate"
	CODELIST  = "codelist"
	TYPE      = "type"
)

// 导出方式，从0-5分别如下
const (
	OpAll   = iota //全量导出
	OpBbrq         //按日期导出
	OpRtime        //按rtime导出
	OpReal         //按实时更新导出
	OpCode         //按代码导出
)

// 触发方式，从0-1分别如下
const (
	TrigCron   = iota //定时任务
	TrigManual        //手动触发
)

// 部分特殊字段名
const (
	ZQDM   = "zqdm"
	BBRQ   = "bbrq"
	RTIME  = "rtime"
	MARKET = "market"
)

// sql类型
const (
	SqlNormal = iota
	SqlStoredProcedure
	SqlIndex
)
