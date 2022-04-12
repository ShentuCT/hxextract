package dao

import (
	"fmt"
	"github.com/pkg/errors"
	"sort"
	"time"
)

type (
	MapZqdmBbrq map[string][]int32
)

func (d *dao) GetZqdmDiffer(tableName string, schemaName string) (*[]string, *[]string, *[]string, error) {
	// 获取对比库证券代码
	schemaCompare := "compare_" + schemaName
	listZqdmCmp, err := d.GetZqdmList(tableName, schemaCompare)
	if err != nil {
		return nil, nil, nil, err
	}
	// 获取生产库证券代码
	listZqdmProd, err := d.GetZqdmList(tableName, schemaName)
	if err != nil {
		return nil, nil, nil, err
	}
	return CompareTwoStringSlices(listZqdmProd, listZqdmCmp)
}

func (d *dao) GetBbrqDiffer(tableName string, schemaName string, codelist *[]string) (*MapZqdmBbrq, *MapZqdmBbrq, error) {
	// 获取库证券代码
	schemaCompare := "compare_" + schemaName
	mapProd, err := d.GetZqdmBbrqList(tableName, codelist, schemaName)
	if err != nil {
		return nil, nil, err
	}
	mapCmp, err := d.GetZqdmBbrqList(tableName, codelist, schemaCompare)
	if err != nil {
		return nil, nil, err
	}
	// 执行对比操作
	// 对比得到不一致的数据
	mapProdMore := make(MapZqdmBbrq)
	mapCmpMore := make(MapZqdmBbrq)
	for k, valProd := range *mapProd {
		// 查找对比库中是否有相同zqdm
		valCmp, ok := (*mapCmp)[k]
		if !ok {
			continue
		}
		bbrq1, bbrq2 := CompareTwoIntSlices(&valProd, &valCmp)
		if bbrq1 != nil && len(*bbrq1) > 0 {
			mapProdMore[k] = *bbrq1
		}

		if bbrq2 != nil && len(*bbrq2) > 0 {
			mapCmpMore[k] = *bbrq2
		}
	}
	return &mapProdMore, &mapCmpMore, nil
}

// 获取表内所有zqdm证券代码
func (d *dao) GetZqdmList(tableName string, schemaName string) (*[]string, error) {
	dbHandler, err := d.DB.getConn(schemaName)
	if err != nil {
		return nil, err
	}
	sqlProdZqdm := fmt.Sprintf("select zqdm from %s group by zqdm", tableName)
	var listZqdm []string
	for i := 0; i < 3; i++ {
		rowsZqdm, err := dbHandler.Query(sqlProdZqdm)
		if err != nil {
			return nil, err
		}
		defer rowsZqdm.Close()
		for rowsZqdm.Next() {
			var zqdm string
			rowsZqdm.Scan(&zqdm)
			listZqdm = append(listZqdm, zqdm)
		}
		if len(listZqdm) > 0 {
			sort.Strings(listZqdm) //升序排序
			break
		}
		time.Sleep(time.Duration(3) * time.Second)
	}
	if len(listZqdm) == 0 {
		return nil, errors.New(fmt.Sprintf("get 0 rows of zqdm, schema=%s, table=%s",
			schemaName, tableName))
	}
	return &listZqdm, nil
}

// 获取zqdm证券代码对应报表日期bbrq
func (d *dao) GetZqdmBbrqList(tableName string, sliceZqdm *[]string, schemaName string) (*MapZqdmBbrq, error) {
	dbHandler, err := d.DB.getConn(schemaName)
	if err != nil {
		return nil, err
	}
	mapZqdmBbrq := make(MapZqdmBbrq)
	cntZqdm := len(*sliceZqdm)
	var strZqdm string
	strZqdm = ""
	for offset, zqdm := range *sliceZqdm {
		strZqdm = strZqdm + "'" + zqdm + "'"
		// 避免全表请求，所以每次请求制定个数的zqdm
		if offset == 500 || offset == cntZqdm-1 {
			// 从数据库请求记录
			sqlZqdmBbrq := fmt.Sprintf("select zqdm, bbrq from %s where zqdm in (%s)", tableName, strZqdm)
			recordCnt := 0
			for i := 0; i < 3; i++ {
				rowsZqdm, err := dbHandler.Query(sqlZqdmBbrq)
				if err != nil {
					return nil, err
				}
				defer rowsZqdm.Close()
				// 把每个代码的报表日期存入map
				for rowsZqdm.Next() {
					recordCnt++
					var zqdm string
					var bbrq int32
					rowsZqdm.Scan(&zqdm, &bbrq)
					if _, ok := mapZqdmBbrq[zqdm]; ok {
						mapZqdmBbrq[zqdm] = append(mapZqdmBbrq[zqdm], bbrq)
					} else {
						sliceBbrq := make([]int32, 0)
						sliceBbrq = append(sliceBbrq, bbrq)
						mapZqdmBbrq[zqdm] = sliceBbrq
					}
				}
				if recordCnt > 0 {
					break
				}
				time.Sleep(time.Duration(3) * time.Second)
			}
			if recordCnt == 0 {
				return nil, errors.New("Get zqdm bbrq failed")
			}
			strZqdm = ""
		} else {
			strZqdm = strZqdm + ", "
		}
	}

	return &mapZqdmBbrq, nil
}

// 对比两个slice，获取两者中不一致的元素
func CompareTwoStringSlices(a, b *[]string) (*[]string, *[]string, *[]string, error) {
	var slice1 []string
	var slice2 []string
	var slice3 []string
	i, j := 0, 0
	for i < len(*a) && j < len(*b) {
		if (*a)[i] > (*b)[j] {
			slice2 = append(slice2, (*b)[j])
			j++
		} else if (*a)[i] < (*b)[j] {
			slice1 = append(slice1, (*a)[i])
			i++
		} else {
			slice3 = append(slice3, (*a)[i])
			i++
			j++
		}
	}
	for i < len(*a) {
		slice1 = append(slice1, (*a)[i])
		i++
	}
	for j < len(*b) {
		slice2 = append(slice2, (*b)[j])
		j++
	}

	return &slice1, &slice2, &slice3, nil
}

// 对比两个slice，获取两者中不一致的元素
func CompareTwoIntSlices(a, b *[]int32) (*[]int32, *[]int32) {
	var slice1 []int32
	var slice2 []int32
	i, j := 0, 0
	for i < len(*a) && j < len(*b) {
		if (*a)[i] > (*b)[j] {
			slice2 = append(slice2, (*b)[j])
			j++
		} else if (*a)[i] < (*b)[j] {
			slice1 = append(slice1, (*a)[i])
			i++
		} else {
			i++
			j++
		}
	}
	for i < len(*a) {
		slice1 = append(slice1, (*a)[i])
		i++
	}
	for j < len(*b) {
		slice2 = append(slice2, (*b)[j])
		j++
	}

	return &slice1, &slice2
}
