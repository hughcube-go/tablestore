package tablestore

import (
	aliTableStore "github.com/aliyun/aliyun-tablestore-go-sdk/tablestore"
	"github.com/hughcube-go/tablestore/schema"
)

type BatchInstallResponse struct {
	Error        error
	Response     *aliTableStore.BatchWriteRowResponse
	FailureCount int
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

		putRowChange := tableSchema.BuildRequestPutRowChange(row)
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

	failureCount := 0
	for _, tableRowResponses := range response.TableToRowsResult {
		for _, tableRowResponse := range tableRowResponses {
			if !tableRowResponse.IsSucceed {
				failureCount++
			}
		}
	}

	return BatchInstallResponse{Response: response, FailureCount: failureCount}
}
