package schema

import (
	"database/sql"
	"fmt"
	"github.com/hughcube-go/timestamps"
	"github.com/hughcube-go/utils/mscheck"
	"reflect"
	"strings"
)

type DataType string
type TimeType int64

type Field struct {
	Name        string
	DBName      string
	StructField reflect.StructField

	IsPrimaryKey    bool
	IsAutoIncrement bool
	IsStatement     bool

	TypeHierarchy  int
	ValueHierarchy int
}

type FieldSlice []*Field

func (p FieldSlice) Len() int           { return len(p) }
func (p FieldSlice) Less(i, j int) bool { return p[i].ValueHierarchy < p[j].ValueHierarchy }
func (p FieldSlice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

func ParseField(fieldStruct reflect.StructField) *Field {
	field := &Field{
		Name:        fieldStruct.Name,
		StructField: fieldStruct,
	}

	tags := ParseFieldTag(fieldStruct.Tag.Get("tableStore"), ";")

	if dbName, ok := tags["COLUMN"]; ok {
		field.DBName = dbName
	}

	if val, ok := tags["PRIMARYKEY"]; ok && (mscheck.IsTrue(val) || "PRIMARYKEY" == val) {
		field.IsPrimaryKey = true
	}

	if val, ok := tags["AUTOINCREMENT"]; ok && (mscheck.IsTrue(val) || "AUTOINCREMENT" == val) {
		field.IsAutoIncrement = true
	}

	if val, ok := tags["STATEMENT"]; ok && (mscheck.IsTrue(val) || "STATEMENT" == val) {
		field.IsStatement = true
	}

	return field
}

func ParseFieldTag(str string, sep string) map[string]string {
	settings := map[string]string{}
	names := strings.Split(str, sep)

	for i := 0; i < len(names); i++ {
		j := i
		if len(names[j]) > 0 {
			for {
				if names[j][len(names[j])-1] == '\\' {
					i++
					names[j] = names[j][0:len(names[j])-1] + sep + names[i]
					names[i] = ""
				} else {
					break
				}
			}
		}

		values := strings.Split(names[j], ":")
		k := strings.TrimSpace(strings.ToUpper(values[0]))

		if len(values) >= 2 {
			settings[k] = strings.Join(values[1:], ":")
		} else if k != "" {
			settings[k] = k
		}
	}

	return settings
}

func (f *Field) IsString() bool {
	return f.StructField.Type.Kind() == reflect.String
}

func (f *Field) IsInt() bool {
	typ := f.StructField.Type.Kind()

	return typ == reflect.Int ||
		typ == reflect.Int8 ||
		typ == reflect.Int16 ||
		typ == reflect.Int32 ||
		typ == reflect.Int64 ||

		//
		typ == reflect.Uint ||
		typ == reflect.Uint8 ||
		typ == reflect.Uint16 ||
		typ == reflect.Uint32 ||
		typ == reflect.Uint64
}

func (f *Field) IsByte() bool {
	return f.StructField.Type == reflect.TypeOf([]byte(""))
}

func (f *Field) IsFloat() bool {
	typ := f.StructField.Type.Kind()
	return typ == reflect.Float32 || typ == reflect.Float64
}

func (f *Field) IsBool() bool {
	return f.StructField.Type.Kind() == reflect.Bool
}

func (f *Field) IsSqlTime() bool {
	return f.StructField.Type == reflect.TypeOf(sql.NullTime{})
}

func (f *Field) ToStructFieldValue(value interface{}) interface{} {
	valueKind := reflect.TypeOf(value).Kind()
	fieldKind := f.StructField.Type.Kind()

	// 表格存储, 支持的类型有   字符串, 整形, 二进制, 浮点数, 布尔值
	// 其中可能需要做强制转换的有  整形, 浮点, sqlTime

	var ok bool
	var err error
	if f.IsSqlTime() && valueKind == reflect.String {
		value, err = timestamps.ParseRFC3339Nano(value.(string))
		ok = err == nil
		//
		//
	} else if valueKind == reflect.Int64 && fieldKind == reflect.Int {
		value, ok = value.(int)
	} else if valueKind == reflect.Int64 && fieldKind == reflect.Int8 {
		value, ok = value.(int8)
	} else if valueKind == reflect.Int64 && fieldKind == reflect.Int16 {
		value, ok = value.(int16)
	} else if valueKind == reflect.Int64 && fieldKind == reflect.Int32 {
		value, ok = value.(int32)
	} else if valueKind == reflect.Int64 && fieldKind == reflect.Int64 {
		value, ok = value.(int64)
	} else if valueKind == reflect.Int64 && fieldKind == reflect.Uint {
		value, ok = value.(uint)
	} else if valueKind == reflect.Int64 && fieldKind == reflect.Uint8 {
		value, ok = value.(uint8)
	} else if valueKind == reflect.Int64 && fieldKind == reflect.Uint16 {
		value, ok = value.(uint16)
	} else if valueKind == reflect.Int64 && fieldKind == reflect.Uint32 {
		value, ok = value.(uint32)
	} else if valueKind == reflect.Int64 && fieldKind == reflect.Uint64 {
		value, ok = value.(uint64)
		//
		//
	} else if valueKind == reflect.Float64 && fieldKind == reflect.Float32 {
		value, ok = value.(float32)
	} else if valueKind == reflect.Float64 && fieldKind == reflect.Float64 {
		value, ok = value.(float64)
	}

	if !ok {
		panic(fmt.Sprintf(
			"field.ToStructFieldValue A cast failure requires that the %s give an %s",
			f.StructField.Type.Kind().String(),
			reflect.TypeOf(value).Kind().String(),
		))
	}

	return value
}

func (f *Field) ToOtsColumnValue(value interface{}) interface{} {
	var ok bool

	// 表格存储, 支持的类型有   字符串, 整形, 二进制, 浮点数, 布尔值
	// 其中可能需要做强制转换的有  整形, 浮点, sqlTime

	if f.IsSqlTime() {
		var sqlValue sql.NullTime
		if sqlValue, ok = value.(sql.NullTime); ok {
			value = timestamps.FormatRFC3339Nano(sqlValue)
		}
	} else if f.IsInt() {
		value, ok = value.(int64)
	} else if f.IsFloat() {
		value, ok = value.(float64)
	}

	if !ok {
		panic(fmt.Sprintf(
			"field.ToOtsColumnValue A cast failure requires that the %s give an %s",
			f.StructField.Type.Kind().String(),
			reflect.TypeOf(value).Kind().String(),
		))
	}

	return value
}
