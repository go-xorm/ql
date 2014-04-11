package ql

import (
	"database/sql"
	"strings"

	"github.com/go-xorm/core"
)

var _ core.Dialect = (*ql)(nil)

func init() {
	core.RegisterDriver("ql", &qlDriver{})
}

type qlDriver struct {
}

func (driver *qlDriver) Parse(driverName, dataSourceName string) (*core.Uri, error) {
	return &core.Uri{DbType: "ql", DbName: dataSourceName}, nil
}

type ql struct {
	core.Base
}

func (db *ql) Init(uri *core.Uri, drivername, dataSourceName string) error {
	return db.Base.Init(db, uri, drivername, dataSourceName)
}

func (db *ql) SqlType(c *core.Column) string {
	switch t := c.SQLType.Name; t {
	case core.Date, core.DateTime, core.TimeStamp, core.Time:
		return core.Numeric
	case core.TimeStampz:
		return core.Text
	case core.Char, core.Varchar, core.TinyText, core.Text, core.MediumText, core.LongText:
		return core.Text
	case core.Bit, core.TinyInt, core.SmallInt, core.MediumInt, core.Int, core.Integer, core.BigInt, core.Bool:
		return core.Integer
	case core.Float, core.Double, core.Real:
		return core.Real
	case core.Decimal, core.Numeric:
		return core.Numeric
	case core.TinyBlob, core.Blob, core.MediumBlob, core.LongBlob, core.Bytea, core.Binary, core.VarBinary:
		return core.Blob
	case core.Serial, core.BigSerial:
		c.IsPrimaryKey = true
		c.IsAutoIncrement = true
		c.Nullable = false
		return core.Integer
	default:
		return t
	}
}

func (db *ql) SupportInsertMany() bool {
	return true
}

func (db *ql) QuoteStr() string {
	return ""
}

func (db *ql) RollBackStr() string {
	return "ROLLBACK"
}

func (db *ql) AutoIncrStr() string {
	return "AUTOINCREMENT"
}

func (db *ql) SupportEngine() bool {
	return false
}

func (db *ql) SupportCharset() bool {
	return false
}

func (db *ql) IndexOnTable() bool {
	return false
}

func (db *ql) IndexCheckSql(tableName, idxName string) (string, []interface{}) {
	args := []interface{}{tableName, idxName}
	return "SELECT Name FROM __Index WHERE TableName == ? and Name == ?", args
}

func (db *ql) TableCheckSql(tableName string) (string, []interface{}) {
	args := []interface{}{tableName}
	return "SELECT Name FROM __Table WHERE Name == ?", args
}

func (db *ql) ColumnCheckSql(tableName, colName string) (string, []interface{}) {
	args := []interface{}{tableName, colName}
	sql := "SELECT Name FROM __Column WHERE TableName == ? and Name == ?"
	return sql, args
}

func (db *ql) GetColumns(tableName string) ([]string, map[string]*core.Column, error) {
	args := []interface{}{tableName}
	s := "SELECT Name, Ordinal, Type FROM __Column WHERE TableName == ?"
	cnn, err := core.Open(db.DriverName(), db.DataSourceName())
	if err != nil {
		return nil, nil, err
	}
	defer cnn.Close()
	rows, err := cnn.Query(s, args...)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	colsMap := make(map[string]*core.Column)
	ordinalMap := make(map[string]int)
	for rows.Next() {
		col := new(core.Column)
		col.Indexes = make(map[string]bool)
		col.Nullable = true

		var name string
		var ordinal, typ int
		err := rows.Scan(&name, ordinal, typ)
		if err != nil {
			return nil, nil, err
		}
		col.Name = name
		//col.SQLType = SQLType{field, 0, 0}
		colsMap[name] = col
		ordinalMap[name] = ordinal
	}

	cols := make([]string, len(colsMap))
	for name, ordinal := range ordinalMap {
		cols[ordinal-1] = name
	}

	return cols, colsMap, nil
}

func (db *ql) GetTables() ([]*core.Table, error) {
	args := []interface{}{}
	s := "SELECT Name FROM __Table"

	cnn, err := core.Open(db.DriverName(), db.DataSourceName())
	if err != nil {
		return nil, err
	}
	defer cnn.Close()

	rows, err := cnn.Query(s, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	tables := make([]*core.Table, 0)
	for rows.Next() {
		table := new(core.Table)
		var name string
		err := rows.Scan(&name)
		if err != nil {
			return nil, err
		}

		table.Name = name
		tables = append(tables, table)
	}
	return tables, nil
}

func (db *ql) GetIndexes(tableName string) (map[string]*core.Index, error) {
	args := []interface{}{tableName}
	s := "SELECT Name, ColumnName, Unique FROM __Index WHERE TableName == ?"
	cnn, err := sql.Open(db.DriverName(), db.DataSourceName())
	if err != nil {
		return nil, err
	}
	defer cnn.Close()

	rows, err := cnn.Query(s, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	indexes := make(map[string]*core.Index, 0)
	for rows.Next() {
		index := new(core.Index)
		var indexName, columnName string
		var isUnique bool
		err := rows.Scan(&indexName, &columnName, &isUnique)
		if err != nil {
			return nil, err
		}

		//fmt.Println(indexName)
		if strings.HasPrefix(indexName, "IDX_"+tableName) || strings.HasPrefix(indexName, "UQE_"+tableName) {
			index.Name = indexName[5+len(tableName) : len(indexName)]
		} else {
			index.Name = indexName
		}

		if isUnique {
			index.Type = core.UniqueType
		} else {
			index.Type = core.IndexType
		}

		index.Cols = make([]string, 0)
		//for _, col := range colIndexes {
		index.Cols = append(index.Cols, columnName)
		//}
		indexes[index.Name] = index
	}

	return indexes, nil
}

func (db *ql) Filters() []core.Filter {
	return []core.Filter{&core.IdFilter{}, &core.SeqFilter{"$", 1}}
}
