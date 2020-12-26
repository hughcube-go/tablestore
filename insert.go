package tablestore

import (
	aliTableStore "github.com/aliyun/aliyun-tablestore-go-sdk/tablestore"
	"github.com/hughcube-go/tablestore/schema"
)

type InstallResponse struct {
	Error    error
	Response *aliTableStore.PutRowResponse
	LastId   int64
}

func (t *TableStore) BuildInsertRequest(row schema.Tabler) (*aliTableStore.PutRowRequest, error) {
	request := new(aliTableStore.PutRowRequest)
	request.PutRowChange = new(aliTableStore.PutRowChange)
	request.PutRowChange.TableName = row.TableName()
	request.PutRowChange.PrimaryKey = new(aliTableStore.PrimaryKey)
	request.PutRowChange.SetCondition(aliTableStore.RowExistenceExpectation_EXPECT_NOT_EXIST)

	// 填充请求的 PutRowChange
	tableSchema, err := t.ParseSchema(row)
	if err != nil {
		return nil, err
	}
	tableSchema.FillRequestPutRowChange(row, request.PutRowChange)

	return request, nil
}

func (t *TableStore) Insert(row schema.Tabler, options ...func(*aliTableStore.PutRowRequest)) InstallResponse {
	request, err := t.BuildInsertRequest(row)
	if err != nil {
		return InstallResponse{Error: err}
	}

	for _, option := range options {
		option(request)
	}

	tableSchema, err := t.ParseSchema(row)
	if err != nil {
		return InstallResponse{Error: err}
	}

	response, err := t.GetClient().PutRow(request)
	if err != nil {
		return InstallResponse{Error: err, Response: response}
	}

	// 如果存在自增键, 结果带上
	for _, v := range response.PrimaryKey.PrimaryKeys {
		if autoIncrField := tableSchema.GetAutoIncrField(); nil != autoIncrField && autoIncrField.DBName == v.ColumnName {
			return InstallResponse{LastId: v.Value.(int64), Response: response}
		}
	}

	return InstallResponse{Response: response}
}
