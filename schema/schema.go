package schema

import (
	"database/sql"
	"errors"
	"fmt"
	aliTableStore "github.com/aliyun/aliyun-tablestore-go-sdk/tablestore"
	"github.com/hughcube-go/timestamps"
	"go/ast"
	"reflect"
	"sync"
)

// ErrUnsupportedDataType unsupported data type
var ErrUnsupportedDataType = errors.New("unsupported data type")

type Schema struct {
	Name           string
	Type           reflect.Type
	Fields         []*Field
	FieldDbNameMap map[string]*Field
	FieldNameMap   map[string]*Field
}

func NewSchema(typ reflect.Type) *Schema {
	return &Schema{
		Name:           typ.Name(),
		Type:           typ,
		Fields:         []*Field{},
		FieldDbNameMap: map[string]*Field{},
		FieldNameMap:   map[string]*Field{},
	}
}

func getDestElemType(dest interface{}) (reflect.Type, error) {
	if dest == nil {
		return nil, fmt.Errorf("%w: %+v", ErrUnsupportedDataType, dest)
	}

	typ := reflect.ValueOf(dest).Type()
	for typ.Kind() == reflect.Slice || typ.Kind() == reflect.Array || typ.Kind() == reflect.Ptr {
		typ = typ.Elem()
	}

	if typ.Kind() != reflect.Struct {
		if typ.PkgPath() == "" {
			return nil, fmt.Errorf("%w: %+v", ErrUnsupportedDataType, dest)
		}
		return nil, fmt.Errorf("%w: %v.%v", ErrUnsupportedDataType, typ.PkgPath(), typ.Name())
	}

	return typ, nil
}

func Parse(dest interface{}, cache *sync.Map) (*Schema, error) {
	modelType, err := getDestElemType(dest)
	if err != nil {
		return nil, err
	}

	if nil != cache {
		if tableSchemaInterface, ok := cache.Load(modelType); ok {
			if tableSchema, ok := tableSchemaInterface.(*Schema); ok {
				return tableSchema, nil
			}
		}
	}

	tableSchema := NewSchema(modelType)

	for i := 0; i < modelType.NumField(); i++ {
		if fieldType := modelType.Field(i); ast.IsExported(fieldType.Name) {
			field := tableSchema.ParseField(fieldType)
			tableSchema.Fields = append(tableSchema.Fields, field)
			tableSchema.FieldDbNameMap[field.DBName] = field
			tableSchema.FieldNameMap[field.Name] = field
		}
	}

	if nil != cache {
		cache.Store(modelType, tableSchema)
	}

	return tableSchema, nil
}

func (s *Schema) ParseField(fieldStruct reflect.StructField) *Field {
	return ParseField(fieldStruct)
}

func (s *Schema) GetAutoIncrField() *Field {
	for _, field := range s.Fields {
		if field.IsPrimaryKey && field.IsAutoIncrement {
			return field
		}
	}
	return nil
}

func (s *Schema) SetDataRowChange(row Tabler, callback func(field *Field, value interface{})) {
	rowValue := reflect.ValueOf(row)
	for rowValue.Kind() == reflect.Slice || rowValue.Kind() == reflect.Array || rowValue.Kind() == reflect.Ptr {
		rowValue = rowValue.Elem()
	}

	for index, field := range s.Fields {
		value := rowValue.Field(index).Interface()
		if val, ok := value.(sql.NullTime); ok {
			callback(field, timestamps.FormatRFC3339Nano(val))
		} else {
			callback(field, value)
		}
	}
}

func (s *Schema) FillRequestPrimaryKey(row Tabler, primaryKeys *aliTableStore.PrimaryKey) bool {
	s.SetDataRowChange(row, func(field *Field, value interface{}) {
		if field.IsPrimaryKey {
			primaryKeys.AddPrimaryKeyColumn(field.DBName, value)
		}
	})

	return true
}

func (s *Schema) FillRequestPutRowChange(row Tabler, putRowChange *aliTableStore.PutRowChange) bool {
	s.SetDataRowChange(row, func(field *Field, value interface{}) {
		if field.IsPrimaryKey && !field.IsAutoIncrement {
			putRowChange.PrimaryKey.AddPrimaryKeyColumn(field.DBName, value)
		} else if field.IsPrimaryKey && field.IsAutoIncrement {
			putRowChange.PrimaryKey.AddPrimaryKeyColumnWithAutoIncrement(field.DBName)
		} else {
			putRowChange.AddColumn(field.DBName, value)
		}

		if field.IsAutoIncrement {
			putRowChange.SetCondition(aliTableStore.RowExistenceExpectation_IGNORE)
			putRowChange.SetReturnPk()
		}
	})

	return true
}
