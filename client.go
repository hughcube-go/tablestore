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
	config      Config
}

func New(endPoint, instanceName, accessKeyId, accessKeySecret string, options ...ClientOption) *TableStore {
	return NewWithConfig(Config{
		EndPoint:        endPoint,
		InstanceName:    instanceName,
		AccessKeyId:     accessKeyId,
		AccessKeySecret: accessKeySecret,
		Options:         options,
	})
}

func NewWithConfig(config Config) *TableStore {
	client := &TableStore{
		TableStoreClient: aliTableStore.NewClient(config.EndPoint, config.InstanceName, config.AccessKeyId, config.AccessKeySecret),
		schemaCache:      new(sync.Map),
		config:           config,
	}

	for _, option := range config.Options {
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

func (t *TableStore) GetConfig() Config {
	return t.config
}
