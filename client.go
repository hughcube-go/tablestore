package tablestore

import (
	"database/sql"
	"errors"
	aliTableStore "github.com/aliyun/aliyun-tablestore-go-sdk/tablestore"
	"github.com/hughcube-go/timestamps"
	"reflect"
	"sync"
	"tablestore/schema"
)

type SetDataRowChangeFun func(field *schema.Field, value interface{})

type TableStore struct {
	client      *aliTableStore.TableStoreClient
	schemaCache *sync.Map
}

func New(endPoint, instanceName, accessKeyId, accessKeySecret string, options ...aliTableStore.ClientOption) *TableStore {
	client := aliTableStore.NewClient(endPoint, instanceName, accessKeyId, accessKeySecret, options...)

	tableStore := &TableStore{
		client:      client,
		schemaCache: new(sync.Map),
	}

	return tableStore
}

func (t *TableStore) ParseSchema(dest interface{}) (*schema.Schema, error) {
	return schema.Parse(dest, t.schemaCache)
}

func (t *TableStore) GetClient() *aliTableStore.TableStoreClient {
	return t.client
}

func (s *TableStore) TraverseSetDataRowChange(row schema.Tabler, callback SetDataRowChangeFun) error {
	tableSchema, err := s.ParseSchema(row)
	if err != nil {
		return err
	}

	rowValue := reflect.ValueOf(row)
	for rowValue.Kind() == reflect.Slice || rowValue.Kind() == reflect.Array || rowValue.Kind() == reflect.Ptr {
		rowValue = rowValue.Elem()
	}

	for index, field := range tableSchema.Fields {
		value := rowValue.Field(index).Interface()
		if val, ok := value.(sql.NullTime); ok {
			callback(field, timestamps.FormatRFC3339Nano(val))
		} else {
			callback(field, value)
		}
	}

	return nil
}

type InstallResponse struct {
	Error    error
	Response *aliTableStore.PutRowResponse
	LastId   int64
}

func (t *TableStore) Install(row schema.Tabler, options ...func(*aliTableStore.PutRowRequest)) InstallResponse {
	request := new(aliTableStore.PutRowRequest)
	request.PutRowChange = new(aliTableStore.PutRowChange)
	request.PutRowChange.TableName = row.TableName()
	request.PutRowChange.PrimaryKey = new(aliTableStore.PrimaryKey)
	request.PutRowChange.SetCondition(aliTableStore.RowExistenceExpectation_EXPECT_NOT_EXIST)

	var autoIncrementField *schema.Field
	err := t.TraverseSetDataRowChange(row, func(field *schema.Field, value interface{}) {
		if field.IsPrimaryKey && !field.IsAutoIncrement {
			request.PutRowChange.PrimaryKey.AddPrimaryKeyColumn(field.DBName, value)
		} else if field.IsPrimaryKey && field.IsAutoIncrement {
			request.PutRowChange.PrimaryKey.AddPrimaryKeyColumnWithAutoIncrement(field.DBName)
			autoIncrementField = field
		} else {
			request.PutRowChange.AddColumn(field.DBName, value)
		}

		if field.IsAutoIncrement {
			request.PutRowChange.SetCondition(aliTableStore.RowExistenceExpectation_IGNORE)
			request.PutRowChange.SetReturnPk()
		}
	})
	if err != nil {
		return InstallResponse{Error: err}
	}

	for _, option := range options {
		option(request)
	}

	response, err := t.GetClient().PutRow(request)
	if err != nil {
		return InstallResponse{Error: err, Response: response}
	}

	for _, v := range response.PrimaryKey.PrimaryKeys {
		if autoIncrementField != nil && autoIncrementField.DBName == v.ColumnName {
			return InstallResponse{LastId: v.Value.(int64), Response: response}
		}
	}

	return InstallResponse{Response: response}
}

type BatchInstallResponse struct {
	Error    error
	Response *aliTableStore.BatchWriteRowResponse
	LastId   int64
}

func (t *TableStore) BatchInstall(list interface{}, options ...func(*aliTableStore.BatchWriteRowRequest)) BatchInstallResponse {
	if reflect.TypeOf(list).Kind() != reflect.Slice {
		return BatchInstallResponse{Error: errors.New("row must be an array of components schema.Tabler")}
	}

	items := reflect.ValueOf(list)
	rows := []schema.Tabler{}
	for i := 0; i < items.Len(); i++ {
		row, ok := items.Index(i).Interface().(schema.Tabler)
		if !ok {
			return BatchInstallResponse{Error: errors.New("row must be an array of components schema.Tabler")}
		}
		rows = append(rows, row)
	}

	request := new(aliTableStore.BatchWriteRowRequest)

	for _, row := range rows {
		putRowChange := new(aliTableStore.PutRowChange)
		putRowChange.TableName = row.TableName()
		putRowChange.PrimaryKey = new(aliTableStore.PrimaryKey)
		putRowChange.SetCondition(aliTableStore.RowExistenceExpectation_EXPECT_NOT_EXIST)

		err := t.TraverseSetDataRowChange(row, func(field *schema.Field, value interface{}) {
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

		if err != nil {
			return BatchInstallResponse{Error: err}
		}

		request.AddRowChange(putRowChange)
	}

	for _, option := range options {
		option(request)
	}

	response, err := t.GetClient().BatchWriteRow(request)
	if err != nil {
		return BatchInstallResponse{Error: err, Response: response}
	}

	return BatchInstallResponse{Response: response}
}

type QueryOneResponse struct {
	Error    error
	Response *aliTableStore.GetRowResponse
	Exists   bool
}

func (t *TableStore) QueryOne(row schema.Tabler, options ...func(*aliTableStore.GetRowRequest)) QueryOneResponse {
	rowValue := reflect.ValueOf(row)
	for rowValue.Kind() != reflect.Ptr {
		return QueryOneResponse{Error: errors.New("row It has to be a pointer")}
	}

	tableSchema, err := t.ParseSchema(row)
	if err != nil {
		return QueryOneResponse{Error: err}
	}

	request := new(aliTableStore.GetRowRequest)
	request.SingleRowQueryCriteria = new(aliTableStore.SingleRowQueryCriteria)
	request.SingleRowQueryCriteria.MaxVersion = 1
	request.SingleRowQueryCriteria.TableName = row.TableName()
	request.SingleRowQueryCriteria.PrimaryKey = new(aliTableStore.PrimaryKey)

	err = t.TraverseSetDataRowChange(row, func(field *schema.Field, value interface{}) {
		if field.IsPrimaryKey {
			request.SingleRowQueryCriteria.PrimaryKey.AddPrimaryKeyColumn(field.DBName, value)
		}
	})
	if err != nil {
		return QueryOneResponse{Error: err}
	}

	for _, option := range options {
		option(request)
	}

	response, err := t.GetClient().GetRow(request)
	if err != nil {
		return QueryOneResponse{Response: response, Error: err}
	}

	rowValueElem := rowValue.Elem()
	columnMap := response.GetColumnMap()
	if columnMap != nil {
		for _, columns := range response.GetColumnMap().Columns {
			for _, column := range columns {
				if field, ok := tableSchema.FieldDbNameMap[column.ColumnName]; ok {
					field.SetValue(rowValueElem.FieldByName(field.Name), column.Value)
				}
			}
		}
	}

	return QueryOneResponse{
		Response: response,
		Exists:   nil != response.PrimaryKey.PrimaryKeys && nil != response.Columns,
	}
}

type QueryAllResponse struct {
	Error        error
	Response     *aliTableStore.BatchGetRowResponse
	RowsAffected int
}

func (t *TableStore) QueryAll(list interface{}, options ...func(*aliTableStore.BatchGetRowRequest)) QueryAllResponse {
	request := new(aliTableStore.BatchGetRowRequest)

	listValue := reflect.ValueOf(list)
	listType := reflect.TypeOf(list)

	if listValue.Kind() != reflect.Ptr {
		return QueryAllResponse{Error: errors.New("row must be an array of components schema.Tabler")}
	}

	listValueElem := listValue.Elem()
	listTypeElem := listType.Elem()
	if listValueElem.Kind() != reflect.Slice {
		return QueryAllResponse{Error: errors.New("row must be an array of components schema.Tabler")}
	}

	rows := []schema.Tabler{}
	for i := 0; i < listValueElem.Len(); i++ {
		if listValueElem.Index(i).Elem().Kind() != reflect.Ptr {
			return QueryAllResponse{Error: errors.New("row must be an array of components schema.Tabler")}
		}

		row, ok := listValueElem.Index(i).Interface().(schema.Tabler)
		if !ok {
			return QueryAllResponse{Error: errors.New("row must be an array of components schema.Tabler")}
		}
		rows = append(rows, row)
	}

	criteria := map[string]*aliTableStore.MultiRowQueryCriteria{}
	for _, row := range rows {
		tableName := row.TableName()
		if criterion, ok := criteria[tableName]; !ok {
			criterion = new(aliTableStore.MultiRowQueryCriteria)
			criterion.TableName = row.TableName()
			criterion.MaxVersion = 1
			criteria[tableName] = criterion
		}

		primaryKey := new(aliTableStore.PrimaryKey)
		err := t.TraverseSetDataRowChange(row, func(field *schema.Field, value interface{}) {
			if field.IsPrimaryKey {
				primaryKey.AddPrimaryKeyColumn(field.DBName, value)
			}
		})
		if err != nil {
			return QueryAllResponse{Error: err}
		}
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

			if 0 >= len(tableRow.Columns) || 0 >= len(tableRow.PrimaryKey.PrimaryKeys) || nil == tableRow.Columns || nil == tableRow.PrimaryKey.PrimaryKeys {
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

	resultSlice := reflect.MakeSlice(listTypeElem, len(resultRows), len(resultRows))
	for index, row := range resultRows {
		resultSlice.Index(index).Set(reflect.ValueOf(row))
	}
	listValueElem.Set(resultSlice)

	return QueryAllResponse{Response: response, RowsAffected: resultSlice.Len()}
}

//func (t *TableStore) Delete(primaryKey interface{}) error {
//}
//
//func (t *TableStore) DeleteAll(primaryKeys []interface{}) error {
//}
//
//func (t *TableStore) Update(primaryKey interface{}, values map[string]interface{}) error {
//}
//
//func (t *TableStore) QueryOne(primaryKey interface{}, dest interface{}) error {
//}
//
//func (t *TableStore) QueryRange(startPrimaryKey interface{}, endPrimaryKey interface{}, dest interface{}) error {
//}
