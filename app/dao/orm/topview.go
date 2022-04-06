package orm

import (
	"time"
)

// topview库为

// 表数据分市场说明：
// mysql数据同步本身是不需要分市场的
// hxfinance业务层要用的，根据市场查找
// 考虑到万一以后可能有某些市场数据量比较大，而小机房用不到这个市场的场景，mysql可以配置这个db不同步
type (
	// TypeDescribe  字段信息：包含字段id，字段类型，字段长度，字段所属表/财务文件和所属数据库（用于区分市场）
	TypeDescribe struct {
		FieldName     string `gorm:"type:varchar(20);column:field_name"`               //字段名
		FieldId       int    `gorm:"type:int;column:field_id;primary_key"`             //字段id，对应行情中的datatype，本表主键
		FieldType     int    `gorm:"type:int;column:field_type"`                       //字段类型
		FieldLen      int    `gorm:"type:int;column:field_len"`                        //字段长度
		FieldTable    string `gorm:"type:varchar(20);column:field_table"`              //字段所属表名 -- 对应一个财务文件
		FieldSchema   string `gorm:"type:varchar(20);column:field_schema;primary_key"` //所属库database -- 对应一个大市场
		FieldDescribe string `gorm:"type:varchar(64);column:field_describe"`           //字段描述
	}
	// FinPrimaryKey 财务数据主键，包含代码和日期
	FinPrimaryKey struct {
		Code     string
		Datetime int
	}
	// TaskItems
	TaskItems struct {
		TaskId     int       `gorm:"type:int;column:id;primary_key"`
		TableName  string    `gorm:"type:varchar(64);column:table_name"`
		SchemaName string    `gorm:"type:varchar(20);column:schema_name"`
		Export     int       `gorm:"type:int;column:export"`
		Mtime      time.Time `gorm:"type:timestamp;column:mtime"`
		Cron       string    `gorm:"type:text;column:cron"`
	}
	// TableInfo
	TableInfo struct {
		TableId    int       `gorm:"type:int;column:id;primary_key"`
		TableName  string    `gorm:"type:varchar(64);column:table_name"`
		SchemaName string    `gorm:"type:varchar(20);column:schema_name"`
		Mtime      time.Time `gorm:"type:timestamp;column:mtime"`
		FinName    string    `gorm:"type:varchar(64);column:fin_name"`
		FinProc    string    `gorm:"type:text;column:fin_proc"`
		AllProc    string    `gorm:"type:text;column:all_proc"`
		RepProc    string    `gorm:"type:text;column:rep_proc"`
		RealProc   string    `gorm:"type:text;column:real_proc"`
		CodeProc   string    `gorm:"type:text;column:code_proc"`
		Server     string    `gorm:"type:text;column:server"`
		User       string    `gorm:"type:text;column:user_name"`
		Passwd     string    `gorm:"type:text;column:passwd"`
		Database   string    `gorm:"type:text;column:database"`
	}
)

// mysql type_describe 中类型
const (
	_ = iota
	TypeINT
	TypeUINT
	_
	_
	TypeDOUBLE
	TypeFLOAT
	TypeSTRING
	TypeTIMESTAMP
)
