package ql

import (
	"fmt"
	"strings"

	"github.com/go-xorm/core"
)

var _ core.Dialect = (*ql)(nil)

func init() {
	core.RegisterDriver("ql", &qlDriver{})
	core.RegisterDialect("ql", &ql{})
}

type qlDriver struct {
}

func (driver *qlDriver) Parse(driverName, dataSourceName string) (*core.Uri, error) {
	return &core.Uri{DbType: "ql", DbName: dataSourceName}, nil
}

type ql struct {
	core.Base
}

func (db *ql) Init(d *core.DB, uri *core.Uri, drivername, dataSourceName string) error {
	return db.Base.Init(d, db, uri, drivername, dataSourceName)
}

func (db *ql) AndStr() string {
	return "&&"
}

func (db *ql) OrStr() string {
	return "||"
}

func (db *ql) EqStr() string {
	return "=="
}

func (db *ql) SqlType(c *core.Column) string {
	switch t := c.SQLType.Name; t {
	case core.Date, core.DateTime, core.TimeStamp, core.Time:
		return "time"
	case core.TimeStampz:
		return "string"
	case core.Char, core.Varchar, core.TinyText, core.Text, core.MediumText, core.LongText:
		return "string"
	case core.Bit, core.TinyInt, core.SmallInt, core.MediumInt, core.Int, core.Integer, core.BigInt:
		return "int"
	case core.Bool:
		return "bool"
	case core.Float, core.Double, core.Real:
		return "float64"
	case core.Decimal, core.Numeric:
		return "string"
	case core.TinyBlob, core.Blob, core.MediumBlob, core.LongBlob, core.Bytea, core.Binary, core.VarBinary:
		return "blob"
	case core.Serial, core.BigSerial:
		c.IsPrimaryKey = true
		c.IsAutoIncrement = true
		c.Nullable = false
		return "int64"
	default:
		return t
	}
}

func (b *ql) CreateIndexSql(tableName string, index *core.Index) string {
	var unique string
	var idxName string
	if index.Type == core.UniqueType {
		unique = " UNIQUE"
		idxName = fmt.Sprintf("UQE_%v_%v", tableName, index.Name)
	} else {
		idxName = fmt.Sprintf("IDX_%v_%v", tableName, index.Name)
	}
	return fmt.Sprintf("CREATE%s INDEX %v ON %v (%v);", unique,
		idxName, tableName, strings.Join(index.Cols, (",")))
}

func (b *ql) CreateTableSql(table *core.Table, tableName, storeEngine, charset string) string {
	var sql string
	sql = "CREATE TABLE IF NOT EXISTS "
	if tableName == "" {
		tableName = table.Name
	}

	sql += b.Quote(tableName) + " ("

	for _, colName := range table.ColumnsSeq() {
		col := table.GetColumn(colName)
		// for ql, no pk, pk is id()
		if !col.IsPrimaryKey {
			sql += col.StringNoPk(b)
			sql = strings.TrimSpace(sql)
			sql += ", "
		}
	}

	return sql[:len(sql)-2] + ");"
}

func (db *ql) SupportInsertMany() bool {
	return true
}

func (b *ql) ShowCreateNull() bool {
	return false
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
	args := []interface{}{}
	return "SELECT Name FROM __Index WHERE TableName == \"" + tableName + "\" && Name == \"" + idxName + "\"", args
}

func (db *ql) TableCheckSql(tableName string) (string, []interface{}) {
	args := []interface{}{}
	return "SELECT Name FROM __Table WHERE Name == \"" + tableName + "\"", args
}

/*func (db *ql) ColumnCheckSql(tableName, colName string, isPK bool) (string, []interface{}) {
	args := []interface{}{}
	var sql string
	if isPK {
		sql = "SELECT \"" + colName + "\" AS Name"
	} else {
		sql = "SELECT Name FROM __Column WHERE TableName == \"" + tableName + "\" && Name == \"" + colName + "\""
	}
	return sql, args
}*/

func (db *ql) IsColumnExist(tableName string, col *core.Column) (bool, error) {
	// since ql always has id() for every table. so we dont' check primary key columns
	if col.IsPrimaryKey {
		return true, nil
	}

	query := "SELECT Name FROM __Column WHERE TableName == \"" + tableName + "\" && Name == \"" + col.Name + "\""
	rows, err := db.DB().Query(query)
	if err != nil {
		return false, err
	}
	defer rows.Close()

	if rows.Next() {
		return true, nil
	}
	return false, nil
}

func (db *ql) GetColumns(tableName string) ([]string, map[string]*core.Column, error) {
	args := []interface{}{}
	s := "SELECT Name, Ordinal, Type FROM __Column WHERE TableName == \"" + tableName + "\""

	rows, err := db.DB().Query(s, args...)
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

	rows, err := db.DB().Query(s, args...)
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
	args := []interface{}{}
	s := "SELECT Name, ColumnName, IsUnique FROM __Index WHERE TableName == \"" + tableName + "\""

	rows, err := db.DB().Query(s, args...)
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

// IdFilter filter SQL replace (id) to primary key column name
type IdFilter struct {
}

type Quoter struct {
	dialect core.Dialect
}

func NewQuoter(dialect core.Dialect) *Quoter {
	return &Quoter{dialect}
}

func (q *Quoter) Quote(content string) string {
	return q.dialect.QuoteStr() + content + q.dialect.QuoteStr()
}

func (i *IdFilter) Do(sql string, dialect core.Dialect, table *core.Table) string {
	quoter := NewQuoter(dialect)
	if table != nil && len(table.PrimaryKeys) == 1 {
		sql = strings.Replace(sql, "`(id)`", "id()", -1)
		sql = strings.Replace(sql, quoter.Quote("(id)"), "id()", -1)
		return strings.Replace(sql, "(id)", "id()", -1)
	}
	return sql
}

func (db *ql) Filters() []core.Filter {
	return []core.Filter{&IdFilter{}, &core.QuoteFilter{}, &core.SeqFilter{"$", 1}}
}
