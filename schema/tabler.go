package schema

import (
	"errors"
	"reflect"
)

var CannotConvertTablerSlice = errors.New("list must be an slice of schema.Tabler")
var CannotConvertTablerPointerSlice = errors.New("list must be an slice of schema.Tabler pointer")

type Tabler interface {
	TableName() string
}

func ToTablerSlice(list interface{}, usedModify bool) ([]Tabler, error) {
	listValue := reflect.ValueOf(list)

	if usedModify && listValue.Kind() != reflect.Ptr {
		return nil, CannotConvertTablerSlice
	}

	if listValue.Kind() == reflect.Ptr {
		listValue = listValue.Elem()
	}

	if listValue.Kind() != reflect.Slice {
		return nil, CannotConvertTablerSlice
	}

	rows := []Tabler{}
	for i := 0; i < listValue.Len(); i++ {
		if usedModify && listValue.Index(i).Elem().Kind() != reflect.Ptr {
			return nil, CannotConvertTablerPointerSlice
		}

		row, ok := listValue.Index(i).Interface().(Tabler)
		if !ok {
			return nil, CannotConvertTablerSlice
		}
		rows = append(rows, row)
	}

	return rows, nil
}
