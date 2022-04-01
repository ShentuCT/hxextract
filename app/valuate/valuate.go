package valuate

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/Knetic/govaluate"
	"log"
	"reflect"
	"strconv"
	"time"
)

// 记录校验失败操作
const SkipNoRow uint32 = 0   // 继续写入
const SkipThisRow uint32 = 1 // 本条记录不写入
const SkipAllRows uint32 = 2 // 所有记录不写入

type (
	CheckConfig struct {
		CheckId         int32  `gorm:"type:int unsigned;column:check_id"`
		CheckSchema     string `gorm:"type:varchar(20);column:check_schema"`
		CheckTable      string `gorm:"type:varchar(64);column:check_table"`
		CheckFormula    string `gorm:"type:varchar(512);column:check_formula"`
		FailedOperation uint32 `gorm:"type:int unsigned;column:failed_operation"`
	}

	// 校验规则
	CheckRule struct {
		Rule   string
		Action uint32
	}

	CachedData struct {
		mapData map[string]interface{}
		rules   []CheckRule
	}
)

func GetCheckInst(rulesCheck *[]CheckRule) *CachedData {
	if rulesCheck == nil || len(*rulesCheck) == 0 {
		return nil
	}
	checkData := CachedData{
		rules: *rulesCheck,
	}
	checkData.mapData = make(map[string]interface{})
	return &checkData
}

// 获取校验规则
func GetValuateRules(db *sql.DB, tablename string, schemaname string) *[]CheckRule {
	if db == nil {
		return nil
	}

	querySql := fmt.Sprintf("select check_id, check_schema, check_table, check_formula, failed_operation from topview.UpdateCheckRule where check_table = '%s' and check_schema = '%s'", tablename, schemaname)
	result, err := db.Query(querySql)
	if err != nil {
		log.Println(result, err)
		return nil
	}
	defer result.Close()

	var sliceRule []CheckRule
	for result.Next() {
		var row CheckConfig
		result.Scan(&row.CheckId, &row.CheckSchema, &row.CheckTable, &row.CheckFormula, &row.FailedOperation)
		checkrule := CheckRule{
			Rule:   row.CheckFormula,
			Action: row.FailedOperation,
		}
		sliceRule = append(sliceRule, checkrule)
	}
	return &sliceRule
}

func FuncTest() (bool, error) {
	mapData := make(map[string]interface{})
	rule := "a != 'NULL' && a > 0"
	mapData["a"] = -11
	return EvaluateJudgeOne(rule, mapData)
}

// 条件判断
func EvaluateJudgeOne(rule string, row_data map[string]interface{}) (bool, error) {
	expression, _ := govaluate.NewEvaluableExpression(rule)
	result, err := expression.Evaluate(row_data)
	//fmt.Println("result: ", result, err)
	if result == nil {
		return false, err
	}
	return result.(bool), err
}

func (c *CachedData) EvaluateJudgeRules() (uint32, error) {
	// 遍历校验规则执行校验
	var reterr error
	reterr = nil
	for _, rule := range c.rules {
		ret, err := EvaluateJudgeOne(rule.Rule, c.mapData)
		if ret == false {
			if err == nil {
				err = errors.New(rule.Rule)
			}
			switch rule.Action {
			case SkipAllRows:
				return SkipAllRows, err
			case SkipThisRow:
				return SkipThisRow, err
			default: //SkipNoRow
				reterr = err
			}
		}
	}
	return SkipNoRow, reterr
}

func (c *CachedData) TransformData(colname string, dataType reflect.Type, data interface{}) {
	// todo 时间暂不支持校验
	if dataType == reflect.TypeOf(time.Time{}) {
		c.mapData[colname] = data
	}
	// 为空直接退出
	if data == "NULL" {
		c.mapData[colname] = data
		return
	}
	// 数字类型需要先转，否则无法进行
	kind := dataType.Kind()
	if kind == reflect.Int || kind == reflect.Int8 || kind == reflect.Int16 || kind == reflect.Int32 || kind == reflect.Int64 {
		dataNum, err := strconv.ParseInt(data.(string), 10, 64)
		if err == nil {
			c.mapData[colname] = dataNum
		} else {
			c.mapData[colname] = "NULL"
		}
	} else if kind == reflect.Uint || kind == reflect.Uint8 || kind == reflect.Uint16 || kind == reflect.Uint32 || kind == reflect.Uint64 {
		dataNum, err := strconv.ParseUint(data.(string), 10, 64)
		if err == nil {
			c.mapData[colname] = dataNum
		} else {
			c.mapData[colname] = "NULL"
		}
	} else if kind == reflect.Float32 || kind == reflect.Float64 {
		dataNum, err := strconv.ParseFloat(data.(string), 64)
		if err == nil {
			c.mapData[colname] = dataNum
		} else {
			c.mapData[colname] = "NULL"
		}
	} else {
		c.mapData[colname] = data
	}
}
