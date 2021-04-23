package tablestore

import (
	aliTableStore "github.com/aliyun/aliyun-tablestore-go-sdk/tablestore"
	"github.com/hughcube-go/tablestore/schema"
)

type UpdateOneResponse struct {
	Error    error
	Response *aliTableStore.UpdateRowResponse
}

func (t *TableStore) UpdateOne(row schema.Tabler, columns map[string]interface{}, options ...func(*aliTableStore.UpdateRowRequest)) UpdateOneResponse {
	tableSchema, err := t.ParseSchema(row)
	if err != nil {
		return UpdateOneResponse{Error: err}
	}

	UpdateRowChange, directlyColumns := tableSchema.BuildRequestUpdateColumns(columns)

	request := new(aliTableStore.UpdateRowRequest)
	request.UpdateRowChange = UpdateRowChange
	request.UpdateRowChange.TableName = row.TableName()
	request.UpdateRowChange.PrimaryKey = tableSchema.BuildRequestPrimaryKey(row)
	request.UpdateRowChange.SetCondition(aliTableStore.RowExistenceExpectation_IGNORE)

	for _, option := range options {
		option(request)
	}

	response, err := t.UpdateRow(request)
	if err != nil {
		return UpdateOneResponse{Error: err, Response: response}
	}

	tableSchema.FillRowColumns(row, directlyColumns)
	tableSchema.FillRow(row, ([]*aliTableStore.PrimaryKeyColumn{}), response.Columns)

	return UpdateOneResponse{Error: err, Response: response}
}
