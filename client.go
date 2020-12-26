package tablestore

import (
	aliTableStore "github.com/aliyun/aliyun-tablestore-go-sdk/tablestore"
	"github.com/hughcube-go/tablestore/schema"
	"sync"
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


//func (t *TableStore) Update(primaryKey interface{}, values map[string]interface{}) error {
//}
//
//func (t *TableStore) QueryOne(primaryKey interface{}, dest interface{}) error {
//}
//
//func (t *TableStore) QueryRange(startPrimaryKey interface{}, endPrimaryKey interface{}, dest interface{}) error {
//}
