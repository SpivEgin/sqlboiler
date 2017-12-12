package drivers

import (
	"database/sql"
	"fmt"
	"os"
	"strings"

	// Side-effect import sql driver

	"github.com/SpivEgin/sqlboiler/bdb"
	_ "github.com/lib/pq"
	"log"
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
	xt := conformCockroachDB(schema)
	p.dbConn.Exec(xt)

	if len(whitelist) > 0 {
		return whitelist, nil
	}
	query := fmt.Sprintf(`select t.table_name from ` + schema + `.rveg_table as t;`)

	rows, err := p.dbConn.Query(query)

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
func (p *CockroachDriver) Columns(schema, tableName string) ([]bdb.Column, error) {
	var columns []bdb.Column

	rows, err0 := p.dbConn.Query(`
-- 	SET DATABASE "$1"
 	select x.column_name, x.column_type, x.column_default, x.udt_name, x.unique
 	from rveg as x, rveg_table as t
 	where x.track_id = t.track_id and t.table_name = $1
 	;`, tableName)


	rowsB, err1 := p.dbConn.Query(`
-- 	SET DATABASE "$1"
 	select x.column_name, x.table_name
 	from rveg_unique as x
 	where x.table_name = $1
	;
	`, tableName)


	if err0 != nil {
		log.Printf("this is x0 %s \n", err0)

		return nil, err0
	}
	if err1 != nil {
		log.Printf("this is x1 %s \n", err1)

	}

	defer rows.Close()
	defer rowsB.Close()

	for rows.Next() {
		var colName, colType, udtName, defaultValue  string
		var arrayType *string
		var unique bool
		err := rows.Scan(&colName, &colType, &defaultValue, &udtName, &unique)
		if err != nil {
			log.Printf("Row error\n")
			log.Fatal(err)
		}
		//cB := 0
		for rowsB.Next() {
			var colName1, xTableName string
			err := rowsB.Scan(&colName1, &xTableName)
			//cB++
			//log.Printf("Count %v\n", cB)
			if err != nil {
				//log.Printf("RowB error: %s\n", err)
			}
			if colName == colName1 && tableName == xTableName {
				unique = true
				//log.Printf("This is unique %v\n", unique)
			}
		}

		column := bdb.Column{
			Name:     colName,
			DBType:   colType,
			ArrType:  arrayType,
			UDTName:  udtName,
			Nullable: true,
			Unique:   unique,
		}

		if len(defaultValue) > 1  {
			//log.Printf("The DefaultValue is: %v\n", defaultValue)
			column.Default = defaultValue
			column.Nullable = false
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
	where tc.table_name = $1 and tc.constraint_type ilike '%primary%' and tc.table_schema = $2;
	`

	row := p.dbConn.QueryRow(query, tableName, schema)
	if err = row.Scan(&pkey.Name); err != nil {
		if err == sql.ErrNoRows {
			log.Printf("\n#### Primary Key ### \n %v \n", err)

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
		case "int", "serial", "INT", "SERIAL":
			c.Type = "int64"
		case "bigint", "bigserial":
			c.Type = "int64"
		case "integer":
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
	case "bool", "BOOL":
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

// This Creates 4 tables to conform the tables to sudo postgres fromat
func conformCockroachDB(schema string) string {
	if len(schema) == 0 {
		log.Panic("No database selected")
	}
	var x = `
	drop view if EXISTS `+ schema +`.rveg_is_null;
	drop table IF EXISTS `+ schema +`.rveg;
	drop table IF EXISTS `+ schema +`.rveg_null;
	drop TABLE IF EXISTS `+ schema +`.rveg_table;
	drop table if EXISTS `+ schema +`.rveg_unique;
	CREATE TABLE `+ schema +`.rveg (
	  column_name string,
	  column_type string,
	  column_default string,
	  udt_name string,
	  "unique" bool,
	  track_id int not null,
	  id INT NOT NULL PRIMARY KEY DEFAULT unique_rowid()
	);
	CREATE TABLE `+ schema +`.rveg_null (
	  COLUMN_NAME string,
	  is_null string(3),
	  track_id int not null,
	  id INT NOT NULL PRIMARY KEY DEFAULT unique_rowid()
	);
	CREATE TABLE `+ schema +`.rveg_table (
	  "table_schema" string,
	  "table_name" string,
	  "table_type" string,
	  track_id int not null,
	  id INT NOT NULL PRIMARY KEY DEFAULT unique_rowid()
	);
	CREATE TABLE `+ schema +`.rveg_unique (
	  "table_schema" string,
	  "table_name" string,
	  "column_name" string,
	  "unique" bool,
	  count_id int not null,
	  id INT NOT NULL PRIMARY KEY DEFAULT unique_rowid()
	);
	
	
	INSERT INTO `+ schema +`.rveg_table (table_schema, table_name, table_type, track_id)
	SELECT t.table_schema, t.table_name, t.table_type, row_number() OVER (ORDER by t.table_name) as track
	FROM information_schema.tables as t
	WHERE t.table_name not ilike 'rveg%' and  t.table_type = 'BASE TABLE' ;
	
	INSERT INTO `+ schema +`.rveg (column_name, column_type, column_default, udt_name, track_id)
	select c.column_name as column_name, c.data_type as column_type, c.column_default as column_default, c.data_type as udt_name, tbl.track_id as track_id
	FROM information_schema.columns as c, `+ schema +`.rveg_table as tbl
	where c.table_name = tbl.table_name and c.table_schema = tbl.table_schema
	;
	INSERT INTO `+ schema +`.rveg_unique ("unique" , table_name, column_name, count_id, table_schema )
	select e.non_unique, e.table_name, e.column_name, row_number() OVER (ORDER by e.table_name) as count_id, e.table_schema
	from information_schema.statistics as e
	Where e.non_unique = false AND e.table_name not ilike 'rveg%';
	
	INSERT INTO `+ schema +`.rveg_null ( is_null, column_name, track_id )
	select c.is_nullable, c.column_name, t.track_id
	from information_schema.columns as c, rveg_table as t
	Where c.is_nullable = 'YES' AND c.table_name not ilike 'rveg%'
	  AND t.TABLE_schema = c.table_schema
	  AND t.TABLE_NAME = c.table_name
	;
	CREATE VIEW `+ schema +`.rveg_is_null
	AS
	select c.is_nullable, c.column_name, t.table_name, t.table_schema
	from information_schema.columns as c, rveg_table as t
	Where c.is_nullable = 'YES' AND c.table_name not ilike 'rveg%'
	  AND t.TABLE_schema = c.table_schema
	  AND t.TABLE_NAME = c.table_name
	;
	
	
	UPDATE `+ schema +`.rveg as rt
	SET column_default = ' '
	WHERE rt.column_default is null;
	
	UPDATE `+ schema +`.rveg
	SET "unique" = FALSE

	`
	return x
}
