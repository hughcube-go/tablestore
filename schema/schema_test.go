package schema

import (
	"database/sql"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

type TestModel struct {
	ID        int64        `tableStore:"primaryKey;column:id;autoIncrement;"`
	CreatedAt sql.NullTime `tableStore:"autoCreateTime;column:created_at;"`
	UpdatedAt sql.NullTime `tableStore:"autoUpdateTime;column:updated_at;"`
	DeletedAt sql.NullTime `tableStore:"autoCreateTime;column:deleted_at;"`
}

func (m *TestModel) TableName() string {
	return "model"
}

func TestSchemaParse(t *testing.T) {
	a := assert.New(t)

	model := &TestModel{
		ID:        12345,
		CreatedAt: sql.NullTime{Time: time.Now(), Valid: true},
		UpdatedAt: sql.NullTime{Time: time.Now(), Valid: true},
		DeletedAt: sql.NullTime{Time: time.Now(), Valid: true},
	}

	tableSchema, err := Parse(model, nil)
	a.Nil(err)
	a.IsType(tableSchema, &Schema{})
}
