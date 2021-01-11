package schema

import (
	"errors"
	"fmt"
	aliTableStore "github.com/aliyun/aliyun-tablestore-go-sdk/tablestore"
	"go/ast"
	"reflect"
	"sort"
	"sync"
)

// ErrUnsupportedDataType unsupported data type
var ErrUnsupportedDataType = errors.New("unsupported data type")

type RangePrimaryKey map[string]interface{}
type MaxPrimaryKey map[string]interface{}
type MinPrimaryKey map[string]interface{}

//
type FieldSlice []*Field

func (p FieldSlice) Len() int           { return len(p) }
func (p FieldSlice) Less(i, j int) bool { return p[i].Sort < p[j].Sort }
func (p FieldSlice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

// 用来做按照优先级排序的
type FieldLevelSlice FieldSlice

func (p FieldLevelSlice) Len() int           { return len(p) }
func (p FieldLevelSlice) Less(i, j int) bool { return p[i].ValueLevel < p[j].ValueLevel }
func (p FieldLevelSlice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

type Schema struct {
	Name           string
	Type           reflect.Type
	Fields         FieldSlice
	ColumnFieldMap map[string]*Field
	FieldMap       map[string]*Field
}

func NewSchema(typ reflect.Type) *Schema {
	return &Schema{
		Name:           typ.Name(),
		Type:           typ,
		Fields:         []*Field{},
		ColumnFieldMap: map[string]*Field{},
		FieldMap:       map[string]*Field{},
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
	fieldNameMap := map[string]FieldLevelSlice{}
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

			trueField.ValueLevel = field.ValueLevel
			trueField.StructField = field.StructField

			if !field.IsStatement {
				break
			}
		}

		if nil != trueField {
			tableSchema.FieldMap[trueField.Name] = trueField
		}
	}

	for _, field := range tableSchema.FieldMap {
		tableSchema.Fields = append(tableSchema.Fields, field)
		tableSchema.ColumnFieldMap[field.DBName] = field
	}

	sort.Sort(tableSchema.Fields)

	if nil != cache {
		cache.Store(modelType, tableSchema)
	}

	return tableSchema, nil
}

func (s *Schema) parse(modelType reflect.Type, level int) []*Field {
	fields := []*Field{}

	for i := 0; i < modelType.NumField(); i++ {
		fieldType := modelType.Field(i)
		if !ast.IsExported(fieldType.Name) {
			continue
		}

		if field := s.ParseField(fieldType); field != nil {
			field.TypeLevel = level
			field.ValueLevel = level
			fields = append(fields, field)
		}

		if fieldType.Type.Kind() == reflect.Struct {
			fields = append(fields, s.parse(fieldType.Type, level+1)...)
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

func (s *Schema) eachField(row interface{}, callback func(field *Field, value reflect.Value), level int) {
	rowValue := reflect.ValueOf(row)
	rowType := reflect.TypeOf(row)

	if rowType.Kind() == reflect.Ptr {
		rowValue = rowValue.Elem()
		rowType = rowType.Elem()
	}

	for i := 0; i < rowType.NumField(); i++ {
		fieldValue := rowValue.Field(i)
		fieldType := rowType.Field(i)

		if !fieldValue.CanInterface() {
			continue
		}

		if field, ok := s.FieldMap[fieldType.Name]; ok && field.ValueLevel == level {
			callback(field, fieldValue)
			continue
		}

		if fieldType.Type.Kind() == reflect.Struct {
			s.eachField(fieldValue.Addr().Interface(), callback, level+1)
		}
	}
}

func (s *Schema) EachSetRequestColumn(row Tabler, callback func(field *Field, value interface{})) {
	setRequestColumnCallback := func(field *Field, columnValue reflect.Value) {
		callback(field, field.ToOtsValue(columnValue.Interface()))
	}
	s.eachField(row, setRequestColumnCallback, 0)
}

func (s *Schema) BuildRequestPrimaryKey(row Tabler) *aliTableStore.PrimaryKey {
	primaryKeys := new(aliTableStore.PrimaryKey)
	s.EachSetRequestColumn(row, func(field *Field, value interface{}) {
		if field.IsPrimaryKey {
			primaryKeys.AddPrimaryKeyColumn(field.DBName, value)
		}
	})
	return primaryKeys
}

func (s *Schema) BuildRequestPutRowChange(row Tabler) *aliTableStore.PutRowChange {
	putRowChange := new(aliTableStore.PutRowChange)
	putRowChange.TableName = row.TableName()
	putRowChange.PrimaryKey = new(aliTableStore.PrimaryKey)
	putRowChange.SetCondition(aliTableStore.RowExistenceExpectation_EXPECT_NOT_EXIST)

	s.EachSetRequestColumn(row, func(field *Field, value interface{}) {
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

	return putRowChange
}

func (s *Schema) BuildRequestRangePrimaryKey(condition interface{}) (*aliTableStore.PrimaryKey, bool, error) {

	if primaryKey, ok := condition.(*aliTableStore.PrimaryKey); ok {
		return primaryKey, false, nil
	}

	var conditionMap RangePrimaryKey
	var isMin bool

	if _, ok := condition.(MaxPrimaryKey); ok {
		isMin = false
		conditionMap = RangePrimaryKey(condition.(MaxPrimaryKey))
	} else if _, ok := condition.(MinPrimaryKey); ok {
		isMin = true
		conditionMap = RangePrimaryKey(condition.(MinPrimaryKey))
	} else {
		return nil, false, errors.New("The type must be MaxPrimaryKey or MinPrimaryKey")
	}

	primaryKeys := new(aliTableStore.PrimaryKey)
	for _, field := range s.Fields {
		if !field.IsPrimaryKey {
			continue
		}

		if value, ok := conditionMap[field.DBName]; ok && nil != value {
			primaryKeys.AddPrimaryKeyColumn(field.DBName, field.ToOtsValue(value))
		} else if value, ok := conditionMap[field.Name]; ok && nil != value {
			primaryKeys.AddPrimaryKeyColumn(field.DBName, field.ToOtsValue(value))
		} else if isMin {
			primaryKeys.AddPrimaryKeyColumnWithMinValue(field.DBName)
		} else {
			primaryKeys.AddPrimaryKeyColumnWithMaxValue(field.DBName)
		}
	}

	return primaryKeys, isMin, nil
}

func (s *Schema) FillRow(row interface{}, primaryKeys []*aliTableStore.PrimaryKeyColumn, columns []*aliTableStore.AttributeColumn) {

	columnMap := map[string]interface{}{}
	for _, primaryKey := range primaryKeys {
		columnMap[primaryKey.ColumnName] = primaryKey.Value
	}

	for _, column := range columns {
		columnMap[column.ColumnName] = column.Value
	}

	setRowFieldCallback := func(field *Field, fieldValue reflect.Value) {
		if value, ok := columnMap[field.DBName]; ok && fieldValue.CanSet() {
			field.SetValue(fieldValue, value)
		}
	}

	s.eachField(row, setRowFieldCallback, 0)
}
