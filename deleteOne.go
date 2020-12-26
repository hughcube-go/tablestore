package tablestore

import (
	aliTableStore "github.com/aliyun/aliyun-tablestore-go-sdk/tablestore"
	"github.com/hughcube-go/tablestore/schema"
)

type DeleteResponse struct {
	Error        error
	Response     *aliTableStore.DeleteRowResponse
	RowsAffected int
}

func (t *TableStore) DeleteOne(row schema.Tabler) DeleteResponse {
	request := new(aliTableStore.DeleteRowRequest)
	request.DeleteRowChange = new(aliTableStore.DeleteRowChange)
	request.DeleteRowChange.TableName = row.TableName()
	request.DeleteRowChange.SetCondition(aliTableStore.RowExistenceExpectation_IGNORE)

	request.DeleteRowChange.PrimaryKey = new(aliTableStore.PrimaryKey)

	tableSchema, err := t.ParseSchema(row)
	if err != nil {
		return DeleteResponse{Error: err}
	}
	tableSchema.FillRequestPrimaryKey(row, request.DeleteRowChange.PrimaryKey)

	response, err := t.GetClient().DeleteRow(request)
	if err != nil {
		return DeleteResponse{Error: err, Response: response}
	}

	return DeleteResponse{Response: response}
}
