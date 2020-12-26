package tablestore

import (
	aliTableStore "github.com/aliyun/aliyun-tablestore-go-sdk/tablestore"
	"github.com/hughcube-go/tablestore/schema"
)

type BatchInstallResponse struct {
	Error    error
	Response *aliTableStore.BatchWriteRowResponse
	LastId   int64
}

func (t *TableStore) BuildBatchInsertRequest(list interface{}, rowOptions ...func(*aliTableStore.PutRowChange)) (*aliTableStore.BatchWriteRowRequest, error) {
	rows, err := schema.ToTablerSlice(list, false)
	if err != nil {
		return nil, err
	}

	request := new(aliTableStore.BatchWriteRowRequest)

	for _, row := range rows {
		tableSchema, err := t.ParseSchema(row)
		if err != nil {
			return nil, err
		}

		putRowChange := new(aliTableStore.PutRowChange)
		putRowChange.TableName = row.TableName()
		putRowChange.PrimaryKey = new(aliTableStore.PrimaryKey)
		putRowChange.SetCondition(aliTableStore.RowExistenceExpectation_EXPECT_NOT_EXIST)
		tableSchema.FillRequestPutRowChange(row, putRowChange)

		for _, rowOption := range rowOptions {
			rowOption(putRowChange)
		}

		request.AddRowChange(putRowChange)
	}

	return request, nil
}

func (t *TableStore) BatchInsert(list interface{}, options ...func(*aliTableStore.BatchWriteRowRequest)) BatchInstallResponse {
	request, err := t.BuildBatchInsertRequest(list)
	if err != nil {
		return BatchInstallResponse{Error: err}
	}

	for _, option := range options {
		option(request)
	}

	response, err := t.GetClient().BatchWriteRow(request)
	if err != nil {
		return BatchInstallResponse{Error: err, Response: response}
	}

	return BatchInstallResponse{Response: response}
}
