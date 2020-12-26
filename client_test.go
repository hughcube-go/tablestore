package tablestore

import (
	"database/sql"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
	"time"
)

type TestModel struct {
	Pk        int64        `tableStore:"primaryKey;column:pk;"`
	ID        int64        `tableStore:"primaryKey;column:id;autoIncrement;"`
	CreatedAt sql.NullTime `tableStore:"autoCreateTime;column:created_at;"`
	UpdatedAt sql.NullTime `tableStore:"autoUpdateTime;column:updated_at;"`
	DeletedAt sql.NullTime `tableStore:"autoCreateTime;column:deleted_at;"`
}

func (m *TestModel) TableName() string {
	return "ots_test"
}

func client_test_model() (*TestModel, sql.NullTime) {
	now := sql.NullTime{Time: time.Now(), Valid: true}
	m := &TestModel{
		Pk:        now.Time.UnixNano(),
		ID:        now.Time.UnixNano(),
		CreatedAt: now,
		UpdatedAt: now,
		DeletedAt: now,
	}

	return m, now
}

func client_test_client() *TableStore {
	client := New(
		"https://mscube.cn-shenzhen.ots.aliyuncs.com",
		"mscube",
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
	for i := 1; i <= 100; i++ {
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
	println(queryAllResponse.Error)
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
