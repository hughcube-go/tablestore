package schema_test

import (
	"database/sql"
	"github.com/hughcube-go/tablestore/schema"
	"github.com/stretchr/testify/assert"
	"testing"
)

type AModel struct {
	CreatedAt sql.NullTime
	UpdatedAt sql.NullTime
	DeletedAt sql.NullTime
}

type BModel struct {
	AModel
	ID int64 `tableStore:"primaryKey;column:id;autoIncrement;"`
	CreatedAt sql.NullTime `tableStore:"autoCreateTime;column:created_at;statement;"`
	UpdatedAt sql.NullTime `tableStore:"autoUpdateTime;column:updated_at;statement;"`
	DeletedAt sql.NullTime `tableStore:"autoCreateTime;column:deleted_at;statement;"`
}

type TestModel struct {
	BModel
	ID int64 `tableStore:"primaryKey;column:id;autoIncrement;"`
	CreatedAt sql.NullTime
	UpdatedAt sql.NullTime
	DeletedAt sql.NullTime `tableStore:"autoCreateTime;column:deleted_at;statement;"`
}

func (m *TestModel) TableName() string {
	return "model"
}

func TestSchemaParse(t *testing.T) {
	a := assert.New(t)

	model := &TestModel{
		ID: 12345,
	}

	tableSchema, err := schema.Parse(model, nil)
	a.Nil(err)
	a.IsType(tableSchema, &schema.Schema{})
}
