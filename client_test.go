package tablestore

import (
	"database/sql"
	"github.com/hughcube-go/tablestore/schema"
	"github.com/hughcube-go/timestamps"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
	"time"
)

type AModel struct {
	timestamps.Timestamps
	CreatedAt sql.NullTime `tableStore:"column:created_at;statement;"`
	UpdatedAt sql.NullTime `tableStore:"column:updated_at;statement;"`
	DeletedAt sql.NullTime `tableStore:"column:deleted_at;statement;"`
}

type BModel struct {
	AModel
}

type TestModel struct {
	BModel
	Test     int64
	Pk       int64         `tableStore:"primaryKey;column:pk;sort:1;"`
	ID       int64         `tableStore:"primaryKey;column:id;autoIncrement;sort:2;"`
	typeTest time.Duration `tableStore:"column:type_test;"`
}

func (m *TestModel) TableName() string {
	return "ots_test"
}

func client_test_model() (*TestModel, sql.NullTime) {
	now := sql.NullTime{Time: time.Now(), Valid: true}
	m := &TestModel{
		Pk:       now.Time.UnixNano(),
		ID:       now.Time.UnixNano(),
		typeTest: 10,
	}

	m.SetCreatedAt(now.Time)
	m.SetUpdatedAt(now.Time)
	m.SetDeletedAt(now.Time)

	return m, now
}

func client_test_client() *TableStore {
	client := New(
		os.Getenv("ALIYUN_OTS_END_POINT"),
		os.Getenv("ALIYUN_OTS_INSTANCE_NAME"),
		os.Getenv("ALIYUN_ACCESS_KEY"),
		os.Getenv("ALIYUN_ACCESS_KEY_SECRET"),
	)

	return client
}

func Test_Client_ParseSchema(t *testing.T) {
	a := assert.New(t)

	client := client_test_client()

	tableSchema, err := client.ParseSchema(&Model{})
	a.Nil(err)

	tableSchema1, err := client.ParseSchema(&Model{})
	a.Nil(err)

	a.Equal(tableSchema, tableSchema1)
}

func Test_Client_Insert(t *testing.T) {
	a := assert.New(t)

	client := client_test_client()
	m, _ := client_test_model()
	response := client.Insert(m)
	a.Nil(response.Error)
	a.True(response.LastId > 0)
}

func Test_Client_BatchInsert(t *testing.T) {
	a := assert.New(t)

	client := client_test_client()

	rows := []*TestModel{}
	for i := 1; i <= 5; i++ {
		m, _ := client_test_model()
		rows = append(rows, m)
	}
	response := client.BatchInsert(rows)
	a.Nil(response.Error)
}

func Test_Client_QueryOne(t *testing.T) {
	a := assert.New(t)

	client := client_test_client()
	row, _ := client_test_model()
	queryOneResponse := client.QueryOne(row)
	a.Nil(queryOneResponse.Error)
	a.False(queryOneResponse.Exists)

	installOneResponse := client.Insert(row)
	a.Nil(installOneResponse.Error)
	a.True(installOneResponse.LastId > 0)
	row.ID = installOneResponse.LastId

	queryRow := &TestModel{Pk: row.Pk, ID: row.ID}
	queryOneResponse = client.QueryOne(queryRow)
	a.Nil(queryOneResponse.Error)
	a.True(queryOneResponse.Exists)
	a.Equal(queryRow.Pk, row.Pk)
	a.Equal(queryRow.ID, row.ID)
}

func Test_Client_QueryAll(t *testing.T) {
	a := assert.New(t)

	client := client_test_client()

	row1, _ := client_test_model()
	installResponse := client.Insert(row1)
	a.Nil(installResponse.Error)
	a.True(installResponse.LastId > 0)
	row1.ID = installResponse.LastId

	row2, _ := client_test_model()
	installResponse = client.Insert(row2)
	a.Nil(installResponse.Error)
	a.True(installResponse.LastId > 0)
	row2.ID = installResponse.LastId

	rows := []interface{}{
		&TestModel{Pk: row1.Pk, ID: row1.ID},
		&TestModel{Pk: row2.Pk, ID: row2.ID},
		&TestModel{Pk: row2.Pk, ID: 1},
	}

	queryAllResponse := client.QueryAll(&rows)
	a.Nil(queryAllResponse.Error)
}

func Test_Client_Delete(t *testing.T) {
	a := assert.New(t)

	client := client_test_client()

	row, _ := client_test_model()
	installResponse := client.Insert(row)
	a.Nil(installResponse.Error)
	a.True(installResponse.LastId > 0)
	row.ID = installResponse.LastId

	deleteResponse := client.DeleteOne(&TestModel{Pk: row.Pk, ID: row.ID})
	a.Nil(deleteResponse.Error)

	deleteResponse = client.DeleteOne(&TestModel{Pk: row.Pk, ID: row.ID})
	a.Nil(deleteResponse.Error)
}

func Test_Client_QueryRange(t *testing.T) {
	a := assert.New(t)

	var rows []*TestModel

	client := client_test_client()

	row, _ := client_test_model()
	_ = client.Insert(row)

	var response QueryRangeResponse

	response = client.QueryRange(&rows, schema.MaxPrimaryKey{}, schema.MinPrimaryKey{}, 1)
	a.Nil(response.Error)
	a.True(len(rows) > 0)

	response = client.QueryRange(&rows, schema.MinPrimaryKey{}, schema.MaxPrimaryKey{}, 1)
	a.Nil(response.Error)
	a.True(len(rows) > 0)
}
