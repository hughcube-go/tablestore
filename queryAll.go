package tablestore

import (
	"errors"
	aliTableStore "github.com/aliyun/aliyun-tablestore-go-sdk/tablestore"
	"github.com/hughcube-go/tablestore/schema"
	"reflect"
)

type QueryAllResponse struct {
	Error        error
	Response     *aliTableStore.BatchGetRowResponse
	RowsAffected int
}

func (t *TableStore) BuildQueryAllRequest(row schema.Tabler) (*aliTableStore.GetRowRequest, error) {
	rowValue := reflect.ValueOf(row)
	for rowValue.Kind() != reflect.Ptr {
		return nil, errors.New("row It must to be a pointer")
	}

	tableSchema, err := t.ParseSchema(row)
	if err != nil {
		return nil, err
	}

	request := new(aliTableStore.GetRowRequest)
	request.SingleRowQueryCriteria = new(aliTableStore.SingleRowQueryCriteria)
	request.SingleRowQueryCriteria.MaxVersion = 1
	request.SingleRowQueryCriteria.TableName = row.TableName()
	request.SingleRowQueryCriteria.PrimaryKey = new(aliTableStore.PrimaryKey)
	tableSchema.SetRequestPrimaryKey(row, request.SingleRowQueryCriteria.PrimaryKey)

	return request, nil
}

func (t *TableStore) QueryAll(list interface{}, options ...func(*aliTableStore.BatchGetRowRequest)) QueryAllResponse {
	rows, err := schema.ToTablerSlice(list, true)
	if err != nil {
		return QueryAllResponse{Error: err}
	}

	// 组合查询语句
	request := new(aliTableStore.BatchGetRowRequest)
	criteria := map[string]*aliTableStore.MultiRowQueryCriteria{}
	for _, row := range rows {
		tableName := row.TableName()
		if criterion, ok := criteria[tableName]; !ok {
			criterion = new(aliTableStore.MultiRowQueryCriteria)
			criterion.TableName = row.TableName()
			criterion.MaxVersion = 1
			criteria[tableName] = criterion
		}

		tableSchema, err := t.ParseSchema(row)
		if err != nil {
			return QueryAllResponse{Error: err}
		}

		primaryKey := new(aliTableStore.PrimaryKey)
		tableSchema.SetRequestPrimaryKey(row, primaryKey)
		criteria[tableName].AddRow(primaryKey)
	}

	for _, criterion := range criteria {
		request.MultiRowQueryCriteria = append(request.MultiRowQueryCriteria, criterion)
	}

	for _, option := range options {
		option(request)
	}

	response, err := t.GetClient().BatchGetRow(request)
	if err != nil {
		return QueryAllResponse{Response: response, Error: err}
	}

	resultRows := []schema.Tabler{}
	for tableName, tableRows := range response.TableToRowsResult {
		for _, tableRow := range tableRows {
			if !tableRow.IsSucceed {
				continue
			}

			if nil == tableRow.Columns ||
				nil == tableRow.PrimaryKey.PrimaryKeys ||
				0 >= len(tableRow.Columns) ||
				0 >= len(tableRow.PrimaryKey.PrimaryKeys) {
				continue
			}

			hitRowIndex := -1
			for rowIndex, row := range rows {
				if row.TableName() != tableName {
					continue
				}

				tableSchema, err := t.ParseSchema(row)
				if err != nil {
					return QueryAllResponse{Response: response, Error: err}
				}

				rowValueElem := reflect.ValueOf(row).Elem()
				for _, column := range tableRow.Columns {
					if field, ok := tableSchema.FieldDbNameMap[column.ColumnName]; ok {
						field.SetValue(rowValueElem.FieldByName(field.Name), column.Value)
					}
				}

				for _, column := range tableRow.PrimaryKey.PrimaryKeys {
					if field, ok := tableSchema.FieldDbNameMap[column.ColumnName]; ok {
						field.SetValue(rowValueElem.FieldByName(field.Name), column.Value)
					}
				}

				resultRows = append(resultRows, row)
				hitRowIndex = rowIndex
				break
			}

			if 0 <= hitRowIndex && 1 == len(rows) {
				rows = []schema.Tabler{}
			} else if 0 <= hitRowIndex && 0 == hitRowIndex {
				rows = rows[1:]
			} else if 0 <= hitRowIndex && ((len(rows) - 1) == hitRowIndex) {
				rows = rows[:len(rows)-1]
			} else if 0 <= hitRowIndex {
				rows = append(rows[:hitRowIndex], rows[hitRowIndex+1:]...)
			}
		}
	}
	listType := reflect.TypeOf(list)
	resultSlice := reflect.MakeSlice(listType.Elem(), len(resultRows), len(resultRows))
	for index, row := range resultRows {
		resultSlice.Index(index).Set(reflect.ValueOf(row))
	}
	reflect.ValueOf(list).Elem().Set(resultSlice)

	return QueryAllResponse{Response: response, RowsAffected: resultSlice.Len()}
}
