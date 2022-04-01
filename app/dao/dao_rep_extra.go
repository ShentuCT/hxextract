package dao

import (
	"fmt"
	"go.uber.org/zap"
	"hxextract/app/config"
	"hxextract/app/dao/orm"
	"hxextract/app/log"
	"strconv"
	"strings"
)

const (
	season1 = 331
	half    = 630
	season3 = 931
	year    = 1231

	typeMask    = 0x00070000 //报表类型的掩码
	typeSeason1 = 0x00010000 //一季报
	typeHalf    = 0x00020000 //中报
	typeSeason3 = 0x00030000 //三季报
	typeYear    = 0x00040000 //年报
)

var (
	repDate        map[int]int
	extraChannel   = make(chan string, 5)      //接收导出的表名，格式为 schema.tablename
	extraExportSql = make(map[string][]string) //存储基础表对应的导出到额外表的sql
)

func init() {
	//repDate = make(map[int]int)
	repDate = map[int]int{
		typeSeason1: season1,
		typeHalf:    half,
		typeSeason3: season3,
		typeYear:    year,
	}
}

func (d *dao) repExtraExportStart() {
	d.loadExtraTable()
	go d.watchExtraTableExport(extraChannel)
}

//
//  loadExtraTable
//  @Description: 加载财务数据拓展id相关表格字段信息
//  @receiver d
//
func (d *dao) loadExtraTable() {
	datatypes := strings.Split(config.GetMysql().ExtraDatatype, ",")
	for _, datatype := range datatypes {
		if datatype == "" {
			continue
		}
		dType, err := strconv.Atoi(datatype)
		if err != nil {
			log.Log.Error(err.Error())
			continue
		}
		// 和掩码按位求与 判断报表类型是一季报中报三季报还是年报
		repType := dType & typeMask
		if _, ok := repDate[repType]; !ok {
			log.Log.Error(fmt.Sprintf("%d is not a valid report type", repType))
			continue
		}
		// 按位异或 获取原ID
		originalDatatype := dType ^ repType
		var result []orm.TypeDescribe
		var res orm.TypeDescribe
		d.DB.defaultOrm.Table("type_describe").Where("field_id = ?", dType).Find(&result)
		if d.DB.defaultOrm.Error != nil {
			err = d.DB.defaultOrm.Error
			log.Log.Error(err.Error())
			continue
		}
		for _, field := range result {
			d.DB.defaultOrm.Table("type_describe").Where("field_id = ? and field_schema = ?",
				originalDatatype, field.FieldSchema).Find(&res)
			if d.DB.defaultOrm.Error != nil {
				log.Log.Error(err.Error())
				continue
			}
			sql := fmt.Sprintf("replace into %s select code,datetime,isvalid,`src-time`,`master-time`,%s "+
				"as `%s` from %s where datetime%%10000 = %d;", field.FieldTable, res.FieldName, field.FieldName,
				res.FieldTable, repDate[repType])
			fullTableName := fmt.Sprintf("%s.%s", field.FieldSchema, res.FieldTable)
			sqls, ok := extraExportSql[fullTableName]
			if !ok {
				sqls = make([]string, 0)
			}
			extraExportSql[fullTableName] = append(sqls, sql)
		}
	}
}

func (d *dao) watchExtraTableExport(ch chan string) {
	for {
		select {
		case tableName := <-ch:
			if _, ok := extraExportSql[tableName]; ok {
				d.exportExtraTable(tableName)
			}
		}
	}
}

func (d *dao) exportExtraTable(tableName string) {
	log.Log.Info("start export extra table", zap.String("tablename", tableName))
	sqls := extraExportSql[tableName]
	schemaName := strings.Split(tableName, ".")[0]
	db, err := d.DB.getConn(schemaName)
	if err != nil {
		log.Log.Error("getConn error:" + err.Error())
		return
	}
	for _, sql := range sqls {
		if sql == "" {
			continue
		}
		if _, err = db.Exec(sql); err != nil {
			log.Log.Error(err.Error())
		}
	}
}
