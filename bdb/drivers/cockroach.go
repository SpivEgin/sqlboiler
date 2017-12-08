package drivers

import (
	"database/sql"
	"fmt"
	"os"
	"strings"

	// Side-effect import sql driver

	_ "github.com/lib/pq"
	"github.com/pkg/errors"
	"github.com/SpivEgin/sqlboiler/bdb"
)

// CockroachDriver holds the database connection string and a handle
// to the database connection.
type CockroachDriver struct {
	connStr string
	dbConn  *sql.DB
}

// NewCockroachDriver takes the database connection details as parameters and
// returns a pointer to a CockroachDriver object. Note that it is required to
// call CockroachDriver.Open() and CockroachDriver.Close() to open and close
// the database connection once an object has been obtained.
func NewCockroachDriver(user, pass, dbname, host string, port int, sslmode, sslkey, sslcert, sslrootcert string) *CockroachDriver {
	driver := CockroachDriver{
		connStr: CockroachBuildQueryString(user, pass, dbname, host, port, sslmode, sslkey, sslcert, sslrootcert),
	}

	return &driver
}

// CockroachBuildQueryString builds a query string.
func CockroachBuildQueryString(user, pass, dbname, host string, port int, sslmode, sslkey, sslcert, sslrootcert string) string {
	parts := []string{}
	if len(user) != 0 {
		parts = append(parts, fmt.Sprintf("user=%s", user))
	}
	if len(pass) != 0 {
		parts = append(parts, fmt.Sprintf("password=%s", pass))
	}
	if len(dbname) != 0 {
		parts = append(parts, fmt.Sprintf("dbname=%s", dbname))
	}
	if len(host) != 0 {
		parts = append(parts, fmt.Sprintf("host=%s", host))
	}
	if port != 0 {
		parts = append(parts, fmt.Sprintf("port=%d", port))
	}
	if len(sslkey) != 0 {
		parts = append(parts, fmt.Sprintf("sslkey=%s", sslkey))
	}
	if len(sslcert) != 0 {
		parts = append(parts, fmt.Sprintf("sslcert=%s", sslcert))
	}
	if len(sslrootcert) != 0 {
		parts = append(parts, fmt.Sprintf("sslrootcert=%s", sslrootcert))
	}
	if len(sslmode) != 0 {
		parts = append(parts, fmt.Sprintf("sslmode=%s", sslmode))
	}
	return strings.Join(parts, " ")
}

// Open opens the database connection using the connection string
func (p *CockroachDriver) Open() error {
	var err error
	p.dbConn, err = sql.Open("postgres", p.connStr)
	if err != nil {
		return err
	}

	return nil
}

// Close closes the database connection
func (p *CockroachDriver) Close() {
	p.dbConn.Close()
}

// UseLastInsertID returns false for postgres
func (p *CockroachDriver) UseLastInsertID() bool {
	return false
}

// UseTopClause returns false to indicate PSQL doesnt support SQL TOP clause
func (m *CockroachDriver) UseTopClause() bool {
	return false
}

// TableNames connects to the postgres database and
// retrieves all table names from the information_schema where the
// table schema is schema. It uses a whitelist and blacklist.
func (p *CockroachDriver) TableNames(schema string, whitelist, blacklist []string) ([]string, error) {
	var names []string
	if len(whitelist) > 0 {
		return whitelist, nil
	}
	query := fmt.Sprintf(`show tables from $1`)
	args := []interface{}{schema}
	rows, err := p.dbConn.Query(query, args...)

	if err != nil {
		return nil, err
	}

	defer rows.Close()
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		names = append(names, name)
	}

	return names, nil

}

// Columns takes a table name and attempts to retrieve the table information
// from the database information_schema.columns. It retrieves the column names
// and column types and returns those as a []Column after TranslateColumnType()
// converts the SQL types to Go types, for example: "varchar" to "string"
type tRowB struct {
	Name  string
	Unique bool

}
func (p *CockroachDriver) Columns(schema, tableName string) ([]bdb.Column, error) {
	var columns []bdb.Column
	var cols []tRowB
	rows, err := p.dbConn.Query(`
select c.column_name as column_name, c.data_type as column_type, c.data_type as udt_name, c.is_nullable as is_nullable
FROM information_schema.columns as c
where c.table_schema = $1
	and c.TABLE_NAME = $2
;
`, schema, tableName)
	rowsB, err := p.dbConn.Query(`
	SELECT st.column_name as "name", st.non_unique as "unique"  from INFORMATION_SCHEMA.statistics as st
	where st.table_schema = $1
		and st.TABLE_NAME = $2
	;
	`, schema, tableName)

	if err != nil {
		return nil, err
	}
	defer rows.Close()
	defer rowsB.Close()

	for rowsB.Next() {
		var colName string
		var unique bool
		if err := rowsB.Scan(&colName, &unique); err != nil {
			return nil, errors.Wrapf(err, "unable to scan for table %s", tableName)
		}

		col := tRowB{
			Name:     colName,
			Unique:   unique,
		}
		cols = append(cols, col)
	}

	for rows.Next() {
		var colName, colType, udtName string
		var defaultValue, arrayType *string
		var nullable, unique bool
		if err := rows.Scan(&colName, &colType, &udtName, &arrayType, &defaultValue, &nullable); err != nil {
			return nil, errors.Wrapf(err, "unable to scan for table %s", tableName)
		}
		for n := len(cols); n <= 0; n++ {
			if cols[n].Name == colName && cols[n].Unique == false{
				unique = true
			}
		}
		column := bdb.Column{
			Name:     colName,
			DBType:   colType,
			ArrType:  arrayType,
			UDTName:  udtName,
			Nullable: nullable,
			Unique:   unique,
		}
		if defaultValue != nil {
			column.Default = *defaultValue
		}

		columns = append(columns, column)
	}

	return columns, nil
}

// PrimaryKeyInfo looks up the primary key for a table.
func (p *CockroachDriver) PrimaryKeyInfo(schema, tableName string) (*bdb.PrimaryKey, error) {
	pkey := &bdb.PrimaryKey{}
	var err error

	query := `
	select tc.constraint_name
	from information_schema.table_constraints as tc
	where tc.table_name = $1 and tc.constraint_type = 'PRIMARY KEY' and tc.table_schema = $2;
	`

	row := p.dbConn.QueryRow(query, tableName, schema)
	if err = row.Scan(&pkey.Name); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}

	queryColumns := `
	select kcu.column_name
	from   information_schema.key_column_usage as kcu
	where  constraint_name = $1 and table_schema = $2;
	`

	var rows *sql.Rows
	if rows, err = p.dbConn.Query(queryColumns, pkey.Name, schema); err != nil {
		return nil, err
	}
	defer rows.Close()

	var columns []string
	for rows.Next() {
		var column string

		err = rows.Scan(&column)
		if err != nil {
			return nil, err
		}

		columns = append(columns, column)
	}

	if err = rows.Err(); err != nil {
		return nil, err
	}

	pkey.Columns = columns

	return pkey, nil
}

// ForeignKeyInfo retrieves the foreign keys for a given table name.
func (p *CockroachDriver) ForeignKeyInfo(schema, tableName string) ([]bdb.ForeignKey, error) {
	var fkeys []bdb.ForeignKey

	query := `
select
    pgcon.conname,
    pgc.relname as source_table,
    pgasrc.attname as source_column,
    dstlookupname.relname as dest_table,
    pgadst.attname as dest_column
from pg_catalog.pg_namespace pgn
    inner join pg_catalog.pg_class pgc on pgn.oid = pgc.relnamespace and pgc.relkind = 'r'
    inner join pg_catalog.pg_constraint pgcon on pgn.oid = pgcon.connamespace and pgc.oid = pgcon.conrelid
    inner join pg_catalog.pg_class dstlookupname on pgcon.confrelid = dstlookupname.oid
    inner join pg_catalog.pg_attribute pgasrc on pgc.oid = pgasrc.attrelid and pgasrc.attnum = ANY(pgcon.conkey)
    inner join pg_catalog.pg_attribute pgadst on pgcon.confrelid = pgadst.attrelid and pgadst.attnum = ANY(pgcon.confkey)
where pgn.nspname = $2 and pgc.relname = $1 and pgcon.contype = 'f'
;`

	var rows *sql.Rows
	var err error
	if rows, err = p.dbConn.Query(query, tableName, schema); err != nil {
		return nil, err
	}

	for rows.Next() {
		var fkey bdb.ForeignKey
		var sourceTable string

		fkey.Table = tableName
		err = rows.Scan(&fkey.Name, &sourceTable, &fkey.Column, &fkey.ForeignTable, &fkey.ForeignColumn)
		if err != nil {
			return nil, err
		}

		fkeys = append(fkeys, fkey)
	}

	if err = rows.Err(); err != nil {
		return nil, err
	}

	return fkeys, nil
}

// TranslateColumnType converts postgres database types to Go types, for example
// "varchar" to "string" and "bigint" to "int64". It returns this parsed data
// as a Column object.
func (p *CockroachDriver) TranslateColumnType(c bdb.Column) bdb.Column {
	if c.Nullable {
		switch c.DBType {
		case "int", "serial", "INT", "SERIAL":
			c.Type = "null.Int64"
		case "int4", "INT4":
			c.Type = "null.Int"
		case "int2", "INT2":
			c.Type = "null.Int16"
		case "decimal", "DECIMAL":
			c.Type = "null.Float64"
		case "float", "real", "FLOAT", "REAL":
			c.Type = "null.Float64"
		case "timestamp", "interval", "collate", "string", "uuid", "array", "TIMESTAMP", "INTERVAL", "COLLATE", "STRING", "UUID":
			c.Type = "null.String"
		case `"char"`:
			c.Type = "null.Byte"
		case "bytes":
			c.Type = "null.Bytes"
		case "json", "jsonb":
			c.Type = "null.JSON"
		case "boolean":
			c.Type = "null.Bool"
		case "date", "time", "timestamp without time zone", "timestamp with time zone":
			c.Type = "null.Time"
		case "ARRAY":
			if c.ArrType == nil {
				panic("unable to get postgres ARRAY underlying type")
			}
			c.Type = getArrayType(c)
			// Make DBType something like ARRAYinteger for parsing with randomize.Struct
			c.DBType = c.DBType + *c.ArrType
		case "USER-DEFINED":
			if c.UDTName == "hstore" {
				c.Type = "types.HStore"
				c.DBType = "hstore"
			} else {
				c.Type = "string"
				fmt.Fprintf(os.Stderr, "Warning: Incompatible data type detected: %s\n", c.UDTName)
			}
		default:
			c.Type = "null.String"
		}
	} else {
		switch c.DBType {
		case "bigint", "bigserial":
			c.Type = "int64"
		case "integer", "serial":
			c.Type = "int"
		case "smallint", "smallserial":
			c.Type = "int16"
		case "decimal", "numeric", "double precision":
			c.Type = "float64"
		case "real":
			c.Type = "float32"
		case "bit", "interval", "uuint", "bit varying", "character", "money", "character varying", "cidr", "inet", "macaddr", "text", "uuid", "xml":
			c.Type = "string"
		case `"char"`:
			c.Type = "types.Byte"
		case "json", "jsonb":
			c.Type = "types.JSON"
		case "bytea":
			c.Type = "[]byte"
		case "boolean":
			c.Type = "bool"
		case "date", "time", "timestamp without time zone", "timestamp with time zone":
			c.Type = "time.Time"
		case "ARRAY":
			c.Type = getArrayType(c)
			// Make DBType something like ARRAYinteger for parsing with randomize.Struct
			c.DBType = c.DBType + *c.ArrType
		case "USER-DEFINED":
			if c.UDTName == "hstore" {
				c.Type = "types.HStore"
				c.DBType = "hstore"
			} else {
				c.Type = "string"
				fmt.Printf("Warning: Incompatible data type detected: %s\n", c.UDTName)
			}
		default:
			c.Type = "string"
		}
	}

	return c
}

// TODO: need to replace ArrayTypes for postgresql with Cockroach types
// getArrayType returns the correct boil.Array type for each database type
func getCockroachArrayType(c bdb.Column) string {
	switch *c.ArrType {
	case "INI", "int":
		return "types.Int64Array"
	case "BYTES", "bytes":
		return "types.BytesArray"
	case "timestamp", "interval", "collate", "string", "uuid", "array", "TIMESTAMP", "INTERVAL", "COLLATE", "STRING", "UUID", "ARRAY":
		return "types.StringArray"
	case "bool":
		return "types.BoolArray"
	case "decimal", "serial", "float", "DECIMAL", "SERIAL", "FLOAT":
		return "types.Float64Array"
	default:
		return "types.StringArray"
	}
}

// RightQuote is the quoting character for the right side of the identifier
func (p *CockroachDriver) RightQuote() byte {
	return '"'
}

// LeftQuote is the quoting character for the left side of the identifier
func (p *CockroachDriver) LeftQuote() byte {
	return '"'
}

// IndexPlaceholders returns true to indicate PSQL supports indexed placeholders
func (p *CockroachDriver) IndexPlaceholders() bool {
	return true
}
