package data

var (
	TypePostgreSQL = "postgresql"
	TypeMySQL      = "mysql"
	TypeSQLServer  = "mssql"
	TypeOracle     = "oracle"
	TypeAWS        = "aws"
	TypeGCP        = "gcp"
	TypeAzure      = "azure"
)

type Instance struct {
	ID             string
	Type           string
	CloudProvider  string
	Region         string
	CloudAccountID string
	Host           string
	Port           int
	Username       string
	Password       string
}

var AllowedInstanceTypes = map[string]struct{}{
	TypePostgreSQL: {},
	TypeMySQL:      {},
	TypeSQLServer:  {},
	TypeOracle:     {},
}

var AllowedCloudProviders = map[string]struct{}{
	TypeAWS:   {},
	TypeGCP:   {},
	TypeAzure: {},
}
