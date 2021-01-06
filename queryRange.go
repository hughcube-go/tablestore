package tablestore

import (
	aliTableStore "github.com/aliyun/aliyun-tablestore-go-sdk/tablestore"
	"github.com/hughcube-go/tablestore/schema"
	"github.com/hughcube-go/utils/msslice"
	"reflect"
)

type QueryRangeResponse struct {
	Error               error
	Response            *aliTableStore.GetRangeResponse
	NextStartPrimaryKey *aliTableStore.PrimaryKey
	HasNext             bool
	RowCount            int
}

func (t *TableStore) QueryRange(list interface{}, start interface{}, end interface{}, limit int, options ...func(*aliTableStore.GetRangeRequest)) QueryRangeResponse {
	listValue := reflect.ValueOf(list)
	if listValue.Kind() != reflect.Ptr {
		return QueryRangeResponse{Error: schema.CannotConvertTablerPointerSlice}
	}

	// 获取数组元素类型
	rowType, err := msslice.GetElemType(list, true)
	if err != nil {
		return QueryRangeResponse{Error: schema.CannotConvertTablerPointerSlice}
	}

	// 转换为Tabler类型, 为了获得表名
	dest, ok := reflect.New(rowType).Interface().(schema.Tabler)
	if !ok {
		return QueryRangeResponse{Error: schema.CannotConvertTablerPointerSlice}
	}

	// Schema结构
	tableSchema, err := t.ParseSchema(dest)
	if err != nil {
		return QueryRangeResponse{Error: err}
	}

	startPrimaryKey, startIsMin, err := tableSchema.BuildRequestRangePrimaryKey(start)
	if err != nil {
		return QueryRangeResponse{Error: err}
	}

	endPrimaryKey, _, err := tableSchema.BuildRequestRangePrimaryKey(end)
	if err != nil {
		return QueryRangeResponse{Error: err}
	}

	// 根据给出的key, 判断倒序还是顺序
	direction := aliTableStore.FORWARD
	if !startIsMin {
		direction = aliTableStore.BACKWARD
	}

	request := new(aliTableStore.GetRangeRequest)
	request.RangeRowQueryCriteria = new(aliTableStore.RangeRowQueryCriteria)
	request.RangeRowQueryCriteria.MaxVersion = 1
	request.RangeRowQueryCriteria.Limit = int32(limit)
	request.RangeRowQueryCriteria.Direction = direction
	request.RangeRowQueryCriteria.TableName = dest.TableName()
	request.RangeRowQueryCriteria.StartPrimaryKey = startPrimaryKey
	request.RangeRowQueryCriteria.EndPrimaryKey = endPrimaryKey

	for _, option := range options {
		option(request)
	}

	response, err := t.GetClient().GetRange(request)
	if err != nil {
		return QueryRangeResponse{Error: err}
	}

	resultSlice, _ := msslice.MakeSameTypeValue(list, len(response.Rows), len(response.Rows))
	for index, tableRow := range response.Rows {
		row := reflect.New(rowType).Interface()
		tableSchema.FillRow(row, tableRow.PrimaryKey.PrimaryKeys, tableRow.Columns)
		resultSlice.Index(index).Set(reflect.ValueOf(row))
	}
	listValue.Elem().Set(resultSlice)

	return QueryRangeResponse{
		Response:            response,
		NextStartPrimaryKey: response.NextStartPrimaryKey,
		HasNext:             nil != response.NextStartPrimaryKey,
		RowCount:            len(response.Rows),
	}
}
