package schema

import (
	"database/sql"
	"github.com/hughcube-go/timestamps"
	"github.com/hughcube-go/utils/mscheck"
	"reflect"
	"strings"
)

type DataType string
type TimeType int64

type Field struct {
	Name            string
	DBName          string
	IsPrimaryKey    bool
	IsAutoIncrement bool
	StructField     reflect.StructField
}

func ParseField(fieldStruct reflect.StructField) *Field {
	field := &Field{
		Name:        fieldStruct.Name,
		StructField: fieldStruct,
	}

	tagSettings := ParseTagSetting(fieldStruct.Tag.Get("tableStore"), ";")

	if dbName, ok := tagSettings["COLUMN"]; ok {
		field.DBName = dbName
	}

	if val, ok := tagSettings["PRIMARYKEY"]; ok && (mscheck.IsTrue(val) || "PRIMARYKEY" == val) {
		field.IsPrimaryKey = true
	}

	if val, ok := tagSettings["AUTOINCREMENT"]; ok && (mscheck.IsTrue(val) || "AUTOINCREMENT" == val) {
		field.IsAutoIncrement = true
	}

	return field
}

func ParseTagSetting(str string, sep string) map[string]string {
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

func (f *Field) SetValue(field reflect.Value, value interface{}) {
	valueKind := reflect.TypeOf(value).Kind()
	fieldKind := f.StructField.Type.Kind()

	fieldValue := value
	var err error

	// sql.NullTime 特殊处理
	if f.StructField.Type == reflect.TypeOf(sql.NullTime{}) && valueKind == reflect.String {
		fieldValue, err = timestamps.ParseRFC3339Nano(value.(string))
	} else if valueKind == reflect.Int64 && fieldKind == reflect.Int {
		fieldValue = value.(int)
	} else if valueKind == reflect.Int64 && fieldKind == reflect.Int8 {
		fieldValue = value.(int8)
	} else if valueKind == reflect.Int64 && fieldKind == reflect.Int16 {
		fieldValue = value.(int16)
	} else if valueKind == reflect.Int64 && fieldKind == reflect.Int32 {
		fieldValue = value.(int32)
	} else if valueKind == reflect.Int64 && fieldKind == reflect.Int64 {
		fieldValue = value.(int64)
	} else if valueKind == reflect.Int64 && fieldKind == reflect.Uint {
		fieldValue = value.(uint)
	} else if valueKind == reflect.Int64 && fieldKind == reflect.Uint8 {
		fieldValue = value.(uint8)
	} else if valueKind == reflect.Int64 && fieldKind == reflect.Uint16 {
		fieldValue = value.(uint16)
	} else if valueKind == reflect.Int64 && fieldKind == reflect.Uint32 {
		fieldValue = value.(uint32)
	} else if valueKind == reflect.Int64 && fieldKind == reflect.Uint64 {
		fieldValue = value.(uint64)
	}

	if err != nil {
		panic(err)
	}

	field.Set(reflect.ValueOf(fieldValue))
}
