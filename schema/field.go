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

	IsPrimaryKey    bool
	IsAutoIncrement bool
	IsStatement     bool

	TypeHierarchy  int
	ValueHierarchy int
}

func ParseField(fieldStruct reflect.StructField) *Field {
	field := &Field{
		Name:        fieldStruct.Name,
		StructField: fieldStruct,
		Sort:        math.MaxInt32,
	}

	tag := msstruct.ParseTag(fieldStruct.Tag.Get("tableStore"))

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
	fieldType := f.StructField.Type

	for fieldType.Kind() == reflect.Ptr {
		fieldType = fieldType.Elem()
	}

	return fieldType == reflect.TypeOf(sql.NullTime{})
}

func (f *Field) IsBytes() bool {
	return f.StructField.Type == reflect.TypeOf([]byte{})
}

func (f *Field) GetPtrLevel() int {
	ptrLevel := 0
	tmpFieldType := f.StructField.Type
	for tmpFieldType.Kind() == reflect.Ptr {
		tmpFieldType = tmpFieldType.Elem()
		ptrLevel++
	}

	return ptrLevel
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

	// 提取字段基本类型
	fieldBaseType := f.StructField.Type
	for fieldBaseType.Kind() == reflect.Ptr {
		fieldBaseType = fieldBaseType.Elem()
	}

	// 数据类型转换
	baseValue = baseValue.Convert(fieldBaseType)

	// 如果只指针属性, 找出最基本的value设置值
	trueValue := baseValue
	if f.StructField.Type.Kind() == reflect.Ptr {

		tmpValue := trueValue
		for i := 1; i <= f.GetPtrLevel(); i++ {
			pv := reflect.New(tmpValue.Type())
			pv.Elem().Set(tmpValue)

			tmpValue = pv
		}

		trueValue = tmpValue
	}
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

	if typeKind != valueKind {
		value = reflect.New(valueType).Elem()
		valueKind = value.Kind()
	}

	ok := true
	var otsValue interface{}

	if val, is := val.(sql.NullTime); is {
		otsValue = timestamps.FormatRFC3339Nano(val)
		///////////////////////////////////////
		///////////////////////////////////////
	} else if typeKind == reflect.String {
		otsValue = value.String()
	} else if valueKind == reflect.Slice && valueType.Elem().Kind() == reflect.Uint8 {
		otsValue = value.Bytes()
		///////////////////////////////////////
		///////////////////////////////////////
	} else if valueKind == reflect.Int || valueKind == reflect.Int8 || valueKind == reflect.Int16 || valueKind == reflect.Int32 || valueKind == reflect.Int64 {
		otsValue = value.Int()
	} else if valueKind == reflect.Uint || valueKind == reflect.Uint8 || valueKind == reflect.Uint16 || valueKind == reflect.Uint32 || valueKind == reflect.Uint64 {
		otsValue = int64(value.Uint())
	} else if valueKind == reflect.Float32 || valueKind == reflect.Float64 {
		otsValue = value.Float()
	} else if valueKind == reflect.Bool {
		otsValue = value.Bool()
	} else {
		ok = false
	}

	if !ok {
		panic(fmt.Sprintf(
			"field.ToOtsValue A cast failure requires that the %s give an %s",
			reflect.TypeOf(value).Kind().String(),
			f.StructField.Type.Kind().String(),
		))
	}

	return otsValue
}
