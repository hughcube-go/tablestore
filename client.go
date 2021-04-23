package tablestore

import (
	aliTableStore "github.com/aliyun/aliyun-tablestore-go-sdk/tablestore"
	"github.com/hughcube-go/tablestore/schema"
	"sync"
)

type ClientOption func(*TableStore)

type TableStore struct {
	*aliTableStore.TableStoreClient
	schemaCache *sync.Map
}

func New(endPoint, instanceName, accessKeyId, accessKeySecret string, options ...ClientOption) *TableStore {

	client := &TableStore{
		TableStoreClient: aliTableStore.NewClient(endPoint, instanceName, accessKeyId, accessKeySecret),
		schemaCache:      new(sync.Map),
	}

	for _, option := range options {
		option(client)
	}

	return client
}

func (t *TableStore) ParseSchema(dest interface{}) (*schema.Schema, error) {
	return schema.Parse(dest, t.schemaCache)
}

func (t *TableStore) GetSdk() *aliTableStore.TableStoreClient {
	return t.TableStoreClient
}
