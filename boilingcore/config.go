package boilingcore

// Config for the running of the commands
type Config struct {
	DriverName       string
	Schema           string
	PkgName          string
	OutFolder        string
	BaseDir          string
	WhitelistTables  []string
	BlacklistTables  []string
	Tags             []string
	Replacements     []string
	Debug            bool
	NoTests          bool
	NoHooks          bool
	NoAutoTimestamps bool
	Wipe             bool
	StructTagCasing  string

	Postgres PostgresConfig
	MySQL    MySQLConfig
	MSSQL    MSSQLConfig
	Cockroach CockroachConfig
}

// PostgresConfig configures a postgres database
type PostgresConfig struct {
	User    string
	Pass    string
	Host    string
	Port    int
	DBName  string
	SSLMode string
	SSLKey string
	SSLCert string
	SSLRootCert string

}

// CockroachConfig configures a postgres database
type CockroachConfig struct {
	User    string
	Pass    string
	Host    string
	Port    int
	DBName  string
	SSLMode string
	SSLKey 	string
	SSLCert string
	SSLRootCert string

}

// MySQLConfig configures a mysql database
type MySQLConfig struct {
	User    string
	Pass    string
	Host    string
	Port    int
	DBName  string
	SSLMode string
}

// MSSQLConfig configures a mysql database
type MSSQLConfig struct {
	User    string
	Pass    string
	Host    string
	Port    int
	DBName  string
	SSLMode string
}
