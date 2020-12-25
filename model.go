package tablestore

import (
	"database/sql"
)

type Model struct {
	ID        int64        `tableStore:"primaryKey;column:id;autoIncrement;"`
	CreatedAt sql.NullTime `tableStore:"autoCreateTime;column:created_at;"`
	UpdatedAt sql.NullTime `tableStore:"autoUpdateTime;column:updated_at;"`
	DeletedAt sql.NullTime `tableStore:"autoCreateTime;column:deleted_at;"`
}

func (m *Model) TableName() string {
	return "model"
}
