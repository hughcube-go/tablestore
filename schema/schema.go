package schema

import (
	"database/sql"
	"errors"
	"fmt"
	aliTableStore "github.com/aliyun/aliyun-tablestore-go-sdk/tablestore"
	"github.com/hughcube-go/timestamps"
	"go/ast"
	"reflect"
	"sort"
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

	// 按照字段名分组
	fieldNameMap := map[string]FieldSlice{}
	for _, field := range tableSchema.parse(modelType, 0) {
		fieldNameMap[field.Name] = append(fieldNameMap[field.Name], field)
	}

	// 进行排序
	for fieldName := range fieldNameMap {
		sort.Sort(fieldNameMap[fieldName])
	}

	// 进行筛选
	for _, fieldSlice := range fieldNameMap {
		var trueField *Field
		for _, field := range fieldSlice {
			if "" == field.DBName && nil == trueField {
				continue
			}

			if nil == trueField {
				trueField = field
			}

			trueField.ValueHierarchy = field.ValueHierarchy
			trueField.StructField = field.StructField

			if !field.IsStatement {
				break
			}
		}

		if nil != trueField {
			tableSchema.FieldNameMap[trueField.Name] = trueField
		}
	}

	for _, field := range tableSchema.FieldNameMap {
		tableSchema.Fields = append(tableSchema.Fields, field)
		tableSchema.FieldDbNameMap[field.DBName] = field
	}

	if nil != cache {
		cache.Store(modelType, tableSchema)
	}

	return tableSchema, nil
}

func (s *Schema) parse(modelType reflect.Type, hierarchy int) []*Field {
	fields := []*Field{}

	for i := 0; i < modelType.NumField(); i++ {
		fieldType := modelType.Field(i)
		if !ast.IsExported(fieldType.Name) {
			continue
		}

		if field := s.ParseField(fieldType); field != nil {
			field.TypeHierarchy = hierarchy
			field.ValueHierarchy = hierarchy
			fields = append(fields, field)
		}

		if fieldType.Type.Kind() == reflect.Struct {
			for _, field := range s.parse(fieldType.Type, hierarchy+1) {
				fields = append(fields, field)
			}
		}
	}

	return fields
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

func (s *Schema) eachField(row interface{}, callback func(field *Field, value interface{}), hierarchy int) {
	rowValue := reflect.ValueOf(row)
	rowType := reflect.TypeOf(row)
	for rowType.Kind() == reflect.Slice || rowType.Kind() == reflect.Array || rowType.Kind() == reflect.Ptr {
		rowValue = rowValue.Elem()
		rowType = rowType.Elem()
	}

	for i := 0; i < rowType.NumField(); i++ {
		fieldValue := rowValue.Field(i)
		fieldType := rowType.Field(i)

		if(!fieldValue.CanInterface()){
			continue
		}

		value := fieldValue.Interface()

		if field, ok := s.FieldNameMap[fieldType.Name]; ok && field.ValueHierarchy == hierarchy && fieldValue.CanInterface() {
			if val, ok := value.(sql.NullTime); ok {
				callback(field, timestamps.FormatRFC3339Nano(val))
			} else {
				callback(field, value)
			}
			continue
		}

		if fieldType.Type.Kind() == reflect.Struct {
			s.eachField(value, callback, hierarchy+1)
		}
	}
}

func (s *Schema) EachField(row Tabler, callback func(field *Field, value interface{})) {
	s.eachField(row, callback, 0)
}

func (s *Schema) SetRequestPrimaryKey(row Tabler, primaryKeys *aliTableStore.PrimaryKey) bool {
	s.EachField(row, func(field *Field, value interface{}) {
		if field.IsPrimaryKey {
			primaryKeys.AddPrimaryKeyColumn(field.DBName, value)
		}
	})

	return true
}

func (s *Schema) SetRequestPutRowChange(row Tabler, putRowChange *aliTableStore.PutRowChange) bool {
	s.EachField(row, func(field *Field, value interface{}) {
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
