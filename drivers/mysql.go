package drivers

import (
	"database/sql"
	"fmt"
	"github.com/go-sql-driver/mysql"
	"github.com/xo/dburl"
	"io/ioutil"
	"net"
	"os"
	"strconv"
	"strings"

	"github.com/jorgerojas26/lazysql/models"

	_ "github.com/go-sql-driver/mysql"
	"golang.org/x/crypto/ssh"
)

type ViaSSHDialer struct {
	client *ssh.Client
}

func (self *ViaSSHDialer) Dial(addr string) (net.Conn, error) {
	return self.client.Dial("tcp", addr)
}

type MySQL struct {
	Connection *sql.DB
	Provider   string
}

func (db *MySQL) TestConnection(urlstr string) (err error) {
	return db.Connect(urlstr)
}

func (db *MySQL) Connect(urlstr string) (err error) {
	var sshUrl, sshUser, sshKey string
	if strings.Contains(urlstr, "ssh://") {
		urlSplit := strings.Split(urlstr, "+")
		sshUrl, urlstr = urlSplit[0], urlSplit[1]
		sshUrl = strings.Split(sshUrl, "://")[1]
		sshSplit := strings.Split(sshUrl, "@")
		sshUser, sshUrl = sshSplit[0], sshSplit[1]
		if strings.Contains(sshUser, ":") {
			sshUserSplit := strings.Split(sshUser, ":")
			sshUser = sshUserSplit[0]
			sshKey = sshUserSplit[1]
		}
	}

	db.SetProvider("mysql")
	pemBytes, err := ioutil.ReadFile(os.Getenv("HOME") + "/.ssh/id_rsa")
	if err != nil {
		return fmt.Errorf("Reading private key file failed %v", err)
	}

	// generate signer instance from plain key
	signer, err := ssh.ParsePrivateKey(pemBytes)
	if err != nil {
		return fmt.Errorf("Parsing plain private key failed %v", err)
	}

	// The client configuration with configuration option to use the ssh-agent
	sshConfig := &ssh.ClientConfig{
		User: sshUser,
		Auth: []ssh.AuthMethod{},
	}

	sshConfig.Auth = append(sshConfig.Auth, ssh.PublicKeys(signer))
	sshConfig.HostKeyCallback = ssh.InsecureIgnoreHostKey()
	// When there's a non empty password add the password AuthMethod
	// sshPass := "vagrant"
	if sshKey != "" {
		sshConfig.Auth = append(sshConfig.Auth, ssh.PasswordCallback(func() (string, error) {
			return sshKey, nil
		}))
	}
	// Connect to the SSH Server
	if sshcon, err := ssh.Dial("tcp", fmt.Sprintf("%s:%d", sshUrl, 22), sshConfig); err == nil {
		mysql.RegisterDial("ssh", (&ViaSSHDialer{sshcon}).Dial)
		url, err := dburl.Parse(urlstr)
		if err != nil {
			return err
		}
		// db.Connection, err = dburl.Open(urlstr)

		// urlSplit := strings.Split(urlstr, "/")
		// dbname := urlSplit[len(urlSplit)-1]
		password, _ := url.User.Password()
		db.Connection, err = sql.Open("mysql", fmt.Sprintf("%s:%s@ssh(%s)/%s", url.User.Username(), password, url.Host, ""))
		// db.Connection, err = sql.Open("mysql", url.DSN)
		if err != nil {
			return err
		}

		err = db.Connection.Ping()
		if err != nil {
			return err
		}

		// if _, err := db.Connection.Query("SELECT Rid FROM roles ORDER BY rid"); err != nil {
		// 	return err
		// }
		// db.Connection.Close()

	} else {
		// return fmt.Errorf("ssh %s@%s, error: %s", sshUser, sshUrl, err)
		return err
	}
	return nil
}

func (db *MySQL) GetDatabases() ([]string, error) {
	var databases []string

	rows, err := db.Connection.Query("SHOW DATABASES")
	if err != nil {
		return databases, err
	}

	for rows.Next() {
		var database string
		err := rows.Scan(&database)
		if err != nil {
			return databases, err
		}
		if database != "information_schema" && database != "mysql" && database != "performance_schema" && database != "sys" {
			databases = append(databases, database)
		}
	}

	return databases, nil
}

func (db *MySQL) GetTables(database string) (map[string][]string, error) {
	rows, err := db.Connection.Query("SHOW TABLES FROM " + database)

	tables := make(map[string][]string)

	if err != nil {
		return tables, err
	}

	for rows.Next() {
		var table string
		rows.Scan(&table)

		tables[database] = append(tables[database], table)
	}

	return tables, nil
}

func (db *MySQL) GetTableColumns(database, table string) (results [][]string, err error) {
	rows, err := db.Connection.Query("DESCRIBE " + table)
	if err != nil {
		return results, err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return results, err
	}

	results = append(results, columns)

	for rows.Next() {
		rowValues := make([]interface{}, len(columns))
		for i := range columns {
			rowValues[i] = new(sql.RawBytes)
		}

		rows.Scan(rowValues...)

		var row []string
		for _, col := range rowValues {
			row = append(row, string(*col.(*sql.RawBytes)))
		}

		results = append(results, row)
	}

	return
}

func (db *MySQL) GetConstraints(table string) (results [][]string, err error) {
	splitTableString := strings.Split(table, ".")
	database := splitTableString[0]
	tableName := splitTableString[1]

	rows, err := db.Connection.Query(fmt.Sprintf("SELECT CONSTRAINT_NAME, COLUMN_NAME, REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME FROM information_schema.KEY_COLUMN_USAGE where TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s'", database, tableName))
	if err != nil {
		return results, err
	}

	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return results, err
	}

	results = append(results, columns)

	for rows.Next() {
		rowValues := make([]interface{}, len(columns))
		for i := range columns {
			rowValues[i] = new(sql.RawBytes)
		}

		rows.Scan(rowValues...)

		var row []string
		for _, col := range rowValues {
			row = append(row, string(*col.(*sql.RawBytes)))
		}

		results = append(results, row)
	}

	return
}

func (db *MySQL) GetForeignKeys(table string) (results [][]string, err error) {
	return results, err
	splitTableString := strings.Split(table, ".")
	database := splitTableString[0]
	tableName := splitTableString[1]

	rows, err := db.Connection.Query(fmt.Sprintf("SELECT TABLE_NAME, COLUMN_NAME, CONSTRAINT_NAME, REFERENCED_COLUMN_NAME, REFERENCED_TABLE_NAME FROM information_schema.KEY_COLUMN_USAGE where REFERENCED_TABLE_SCHEMA = '%s' AND REFERENCED_TABLE_NAME = '%s'", database, tableName))
	if err != nil {
		return results, err
	}

	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return results, err
	}

	results = append(results, columns)

	for rows.Next() {
		rowValues := make([]interface{}, len(columns))
		for i := range columns {
			rowValues[i] = new(sql.RawBytes)
		}

		rows.Scan(rowValues...)

		var row []string
		for _, col := range rowValues {
			row = append(row, string(*col.(*sql.RawBytes)))
		}

		results = append(results, row)
	}

	return
}

func (db *MySQL) GetIndexes(table string) (results [][]string, err error) {
	rows, err := db.Connection.Query("SHOW INDEX FROM " + table)
	if err != nil {
		return results, err
	}
	defer rows.Close()

	columns, _ := rows.Columns()

	results = append(results, columns)

	for rows.Next() {
		rowValues := make([]interface{}, len(columns))
		for i := range columns {
			rowValues[i] = new(sql.RawBytes)
		}

		rows.Scan(rowValues...)

		var row []string
		for _, col := range rowValues {
			row = append(row, string(*col.(*sql.RawBytes)))
		}

		results = append(results, row)
	}

	return
}

func (db *MySQL) GetRecords(table, where, sort string, offset, limit int) (paginatedResults [][]string, totalRecords int, err error) {
	defaultLimit := 300

	isPaginationEnabled := offset >= 0 && limit >= 0

	if limit != 0 {
		defaultLimit = limit
	}

	query := fmt.Sprintf("SELECT * FROM %s s LIMIT %d,%d", table, offset, defaultLimit)

	if where != "" {
		query = fmt.Sprintf("SELECT * FROM %s %s LIMIT %d,%d", table, where, offset, defaultLimit)
	}

	if sort != "" {
		query = fmt.Sprintf("SELECT * FROM %s %s ORDER BY %s LIMIT %d,%d", table, where, sort, offset, defaultLimit)
	}

	paginatedRows, err := db.Connection.Query(query)
	if err != nil {
		return paginatedResults, totalRecords, err
	}

	if isPaginationEnabled {
		queryWithoutLimit := fmt.Sprintf("SELECT COUNT(*) FROM %s %s", table, where)

		rows := db.Connection.QueryRow(queryWithoutLimit)

		if err != nil {
			return paginatedResults, totalRecords, err
		}

		rows.Scan(&totalRecords)

		defer paginatedRows.Close()
	}

	columns, _ := paginatedRows.Columns()

	paginatedResults = append(paginatedResults, columns)

	for paginatedRows.Next() {
		rowValues := make([]interface{}, len(columns))
		for i := range columns {
			rowValues[i] = new(sql.RawBytes)
		}

		paginatedRows.Scan(rowValues...)

		var row []string
		for _, col := range rowValues {
			row = append(row, string(*col.(*sql.RawBytes)))
		}

		paginatedResults = append(paginatedResults, row)

	}

	return
}

func (db *MySQL) ExecuteQuery(query string) (results [][]string, err error) {
	rows, err := db.Connection.Query(query)
	if err != nil {
		return results, err
	}

	defer rows.Close()

	columns, _ := rows.Columns()

	results = append(results, columns)

	for rows.Next() {
		rowValues := make([]interface{}, len(columns))
		for i := range columns {
			rowValues[i] = new(sql.RawBytes)
		}

		rows.Scan(rowValues...)

		var row []string
		for _, col := range rowValues {
			row = append(row, string(*col.(*sql.RawBytes)))
		}

		results = append(results, row)

	}

	return
}

// TODO: Rewrites this logic to use the primary key instead of the id
func (db *MySQL) UpdateRecord(table, column, value, primaryKeyColumnName, primaryKeyValue string) error {
	query := fmt.Sprintf("UPDATE %s SET %s = \"%s\" WHERE %s = \"%s\"", table, column, value, primaryKeyColumnName, primaryKeyValue)
	_, err := db.Connection.Exec(query)

	return err
}

// TODO: Rewrites this logic to use the primary key instead of the id
func (db *MySQL) DeleteRecord(table, primaryKeyColumnName, primaryKeyValue string) error {
	query := fmt.Sprintf("DELETE FROM %s WHERE %s = \"%s\"", table, primaryKeyColumnName, primaryKeyValue)
	_, err := db.Connection.Exec(query)

	return err
}

func (db *MySQL) ExecuteDMLStatement(query string) (result string, err error) {
	res, error := db.Connection.Exec(query)

	if error != nil {
		return result, error
	} else {
		rowsAffected, _ := res.RowsAffected()

		return fmt.Sprintf("%d rows affected", rowsAffected), error
	}
}

func (db *MySQL) ExecutePendingChanges(changes []models.DbDmlChange, inserts []models.DbInsert) (err error) {
	queries := make([]string, 0, len(changes)+len(inserts))

	// This will hold grouped changes by their RowId and Table
	groupedUpdated := make(map[string][]models.DbDmlChange)
	groupedDeletes := make([]models.DbDmlChange, 0, len(changes))

	// Group changes by RowId and Table
	for _, change := range changes {
		switch change.Type {
		case "UPDATE":
			key := fmt.Sprintf("%s|%s|%s", change.Table, change.PrimaryKeyColumnName, change.PrimaryKeyValue)
			groupedUpdated[key] = append(groupedUpdated[key], change)
		case "DELETE":
			groupedDeletes = append(groupedDeletes, change)
		}
	}

	// Combine individual changes to SQL statements
	for key, changes := range groupedUpdated {
		columns := []string{}

		// Split key into table and rowId
		splitted := strings.Split(key, "|")
		table := splitted[0]
		primaryKeyColumnName := splitted[1]
		primaryKeyValue := splitted[2]

		for _, change := range changes {
			columns = append(columns, fmt.Sprintf("%s='%s'", change.Column, change.Value))
		}

		// Merge all column updates
		updateClause := strings.Join(columns, ", ")

		query := fmt.Sprintf("UPDATE %s SET %s WHERE %s = '%s';", table, updateClause, primaryKeyColumnName, primaryKeyValue)

		queries = append(queries, query)
	}

	for _, delete := range groupedDeletes {
		statementType := ""
		query := ""

		statementType = "DELETE FROM"

		query = fmt.Sprintf("%s %s WHERE %s = \"%s\"", statementType, delete.Table, delete.PrimaryKeyColumnName, delete.PrimaryKeyValue)

		if query != "" {
			queries = append(queries, query)
		}
	}

	for _, insert := range inserts {
		values := make([]string, 0, len(insert.Values))

		for _, value := range insert.Values {
			_, error := strconv.ParseFloat(value, 64)

			if strings.ToLower(value) != "default" && error != nil {
				values = append(values, fmt.Sprintf("\"%s\"", value))
			} else {
				values = append(values, value)
			}
		}

		query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", insert.Table, strings.Join(insert.Columns, ", "), strings.Join(values, ", "))

		queries = append(queries, query)
	}

	tx, error := db.Connection.Begin()
	if error != nil {
		return error
	}

	for _, query := range queries {

		_, err = tx.Exec(query)

		if err != nil {
			tx.Rollback()

			return err
		}
	}

	err = tx.Commit()

	if err != nil {
		return err
	}

	return err
}

func (db *MySQL) SetProvider(provider string) {
	db.Provider = provider
}

func (db *MySQL) GetProvider() string {
	return db.Provider
}
