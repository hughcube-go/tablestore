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
	tableSchema, err := t.ParseSchema(row)
	if err != nil {
		return DeleteResponse{Error: err}
	}

	request := new(aliTableStore.DeleteRowRequest)
	request.DeleteRowChange = new(aliTableStore.DeleteRowChange)
	request.DeleteRowChange.TableName = row.TableName()
	request.DeleteRowChange.SetCondition(aliTableStore.RowExistenceExpectation_IGNORE)
	request.DeleteRowChange.PrimaryKey = tableSchema.BuildRequestPrimaryKey(row)

	response, err := t.DeleteRow(request)
	if err != nil {
		return DeleteResponse{Error: err, Response: response}
	}

	return DeleteResponse{Response: response}
}
