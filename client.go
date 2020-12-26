package tablestore

import (
	aliTableStore "github.com/aliyun/aliyun-tablestore-go-sdk/tablestore"
	"sync"
	"tablestore/schema"
)

type TableStore struct {
	client      *aliTableStore.TableStoreClient
	schemaCache *sync.Map
}

func New(endPoint, instanceName, accessKeyId, accessKeySecret string, options ...aliTableStore.ClientOption) *TableStore {
	client := aliTableStore.NewClient(endPoint, instanceName, accessKeyId, accessKeySecret, options...)

	tableStore := &TableStore{
		client:      client,
		schemaCache: new(sync.Map),
	}

	return tableStore
}

func (t *TableStore) ParseSchema(dest interface{}) (*schema.Schema, error) {
	return schema.Parse(dest, t.schemaCache)
}

func (t *TableStore) GetClient() *aliTableStore.TableStoreClient {
	return t.client
}


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

//func (t *TableStore) Update(primaryKey interface{}, values map[string]interface{}) error {
//}
//
//func (t *TableStore) QueryOne(primaryKey interface{}, dest interface{}) error {
//}
//
//func (t *TableStore) QueryRange(startPrimaryKey interface{}, endPrimaryKey interface{}, dest interface{}) error {
//}
