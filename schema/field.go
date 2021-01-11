package schema

import (
	"database/sql"
	"fmt"
	"github.com/hughcube-go/timestamps"
	"github.com/hughcube-go/utils/msstruct"
	"math"
	"reflect"
)

type DataType string
type TimeType int64

type Field struct {
	Sort        int
	Name        string
	DBName      string
	StructField reflect.StructField

	PtrLevel int
	Type     reflect.Type
	BaseType reflect.Type

	IsPrimaryKey    bool
	IsAutoIncrement bool
	IsStatement     bool

	TypeLevel  int
	ValueLevel int
}

func ParseField(structField reflect.StructField) *Field {
	field := &Field{
		Name:        structField.Name,
		StructField: structField,
		Sort:        math.MaxInt32,
		Type:        structField.Type,
	}

	// 基本类型
	field.BaseType = field.Type
	for field.BaseType.Kind() == reflect.Ptr {
		field.PtrLevel++
		field.BaseType = field.BaseType.Elem()
	}

	tag := msstruct.ParseTag(structField.Tag.Get("tableStore"))

	field.DBName = tag.Get("column")
	field.IsPrimaryKey = tag.IsTrue("primaryKey")
	field.IsAutoIncrement = tag.IsTrue("autoincrement")
	field.IsStatement = tag.IsTrue("statement")

	if sort, err := tag.GetInt("SORT"); err == nil {
		field.Sort = sort
	}

	return field
}

func (f *Field) IsSqlTime() bool {
	return f.BaseType == reflect.TypeOf(sql.NullTime{})
}

func (f *Field) SetValue(fieldValue reflect.Value, value interface{}) {
	// 提取基本value
	baseValue := reflect.ValueOf(value)
	for baseValue.Kind() == reflect.Ptr {
		baseValue = baseValue.Elem()
	}

	// 时间格式的需要单独处理
	if baseValue.Kind() == reflect.String && f.IsSqlTime() {
		if sqlTimeDate, err := timestamps.ParseRFC3339Nano(baseValue.String()); err == nil {
			baseValue = reflect.ValueOf(sqlTimeDate)
		}
	}

	// 数据类型转换
	trueValue := baseValue.Convert(f.BaseType)

	// 如果是指针类型, 先初始化, 在用baseValue设置值
	tmpValue := trueValue
	for i := 1; i <= f.PtrLevel; i++ {
		pv := reflect.New(tmpValue.Type())
		pv.Elem().Set(tmpValue)
		tmpValue = pv
	}
	trueValue = tmpValue

	fieldValue.Set(trueValue)
}

func (f *Field) ToOtsValue(val interface{}) interface{} {
	// 表格存储, 支持的类型有   字符串, 整形, 二进制, 浮点数, 布尔值
	// 其中可能需要做强制转换的有  整形, 浮点, sqlTime

	value := reflect.ValueOf(val)
	for value.Kind() == reflect.Ptr {
		value = value.Elem()
	}
	valueKind := value.Kind()

	valueType := reflect.TypeOf(val)
	for valueType.Kind() == reflect.Ptr {
		valueType = valueType.Elem()
	}
	typeKind := valueType.Kind()

	// 可能存在ZeroValue的情况
	if typeKind != valueKind {
		value = reflect.New(valueType).Elem()
		valueKind = value.Kind()
	}

	if val, is := value.Interface().(sql.NullTime); is {
		return timestamps.FormatRFC3339Nano(val)
	}

	if typeKind == reflect.String {
		return value.String()
	}

	if valueKind == reflect.Slice && valueType.Elem().Kind() == reflect.Uint8 {
		return value.Bytes()
	}

	if valueKind == reflect.Int || valueKind == reflect.Int8 || valueKind == reflect.Int16 || valueKind == reflect.Int32 || valueKind == reflect.Int64 {
		return value.Int()
	}

	if valueKind == reflect.Uint || valueKind == reflect.Uint8 || valueKind == reflect.Uint16 || valueKind == reflect.Uint32 || valueKind == reflect.Uint64 {
		return int64(value.Uint())
	}

	if valueKind == reflect.Float32 || valueKind == reflect.Float64 {
		return value.Float()
	}

	if valueKind == reflect.Bool {
		return value.Bool()
	}

	// 无可用被识别的类型, 直接抛出错误
	panic(fmt.Sprintf(
		"field.ToOtsValue A cast failure requires that the %s give an %s",
		reflect.TypeOf(value).Kind().String(),
		f.StructField.Type.Kind().String(),
	))
}
