package tablestore

import (
	"errors"
	aliTableStore "github.com/aliyun/aliyun-tablestore-go-sdk/tablestore"
	"github.com/hughcube-go/tablestore/schema"
	"reflect"
)

type QueryOneResponse struct {
	Error    error
	Response *aliTableStore.GetRowResponse
	Exists   bool
}

func (t *TableStore) BuildQueryOneRequest(row schema.Tabler) (*aliTableStore.GetRowRequest, error) {
	rowValue := reflect.ValueOf(row)
	for rowValue.Kind() != reflect.Ptr {
		return nil, errors.New("row It must to be a pointer")
	}

	tableSchema, err := t.ParseSchema(row)
	if err != nil {
		return nil, err
	}

	request := new(aliTableStore.GetRowRequest)
	request.SingleRowQueryCriteria = new(aliTableStore.SingleRowQueryCriteria)
	request.SingleRowQueryCriteria.MaxVersion = 1
	request.SingleRowQueryCriteria.TableName = row.TableName()
	request.SingleRowQueryCriteria.PrimaryKey = tableSchema.BuildRequestPrimaryKey(row)

	return request, nil
}

func (t *TableStore) QueryOne(row schema.Tabler, options ...func(*aliTableStore.GetRowRequest)) QueryOneResponse {
	request, err := t.BuildQueryOneRequest(row)
	if err != nil {
		return QueryOneResponse{Error: err}
	}

	tableSchema, err := t.ParseSchema(row)
	if err != nil {
		return QueryOneResponse{Error: err}
	}

	for _, option := range options {
		option(request)
	}

	response, err := t.GetRow(request)
	if err != nil {
		return QueryOneResponse{Response: response, Error: err}
	}

	tableSchema.FillRow(row, response.PrimaryKey.PrimaryKeys, response.Columns)

	return QueryOneResponse{
		Response: response,
		Exists:   nil != response.PrimaryKey.PrimaryKeys && nil != response.Columns,
	}
}
