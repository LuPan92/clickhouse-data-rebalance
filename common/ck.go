package common

import (
	"database/sql"
	"fmt"
	"net/url"
	"sync"
	"time"

	"github.com/LuPan92/clickhouse-data-rebalance/log"
	"github.com/pkg/errors"
)

const (
	ClickHouseDefaultDB       string = "default"
	ClickHouseDefaultUser     string = "default"
	ClickHouseDefaultPort     int    = 9000
	ClickHouseDefaultHttpPort int    = 8123
	ClickHouseDefaultZkPort   int    = 2181
	ZkStatusDefaultPort       int    = 8080
	SshDefaultPort            int    = 22

	SshPasswordSave      int = 0
	SshPasswordNotSave   int = 1
	SshPasswordUsePubkey int = 2
)

var ConnectPool sync.Map

type Connection struct {
	dsn string
	db  *sql.DB
}

func ConnectClickHouse(host string, port int, database string, user string, password string) (*sql.DB, error) {
	var db *sql.DB
	var err error

	dsn := fmt.Sprintf("tcp://%s:%d?database=%s&username=%s&password=%s&read_timeout=300",
		host, port, url.QueryEscape(database), url.QueryEscape(user), url.QueryEscape(password))

	if conn, ok := ConnectPool.Load(host); ok {
		c := conn.(Connection)
		err := c.db.Ping()
		if err == nil {
			if c.dsn == dsn {
				return c.db, nil
			} else {
				//dsn is different, maybe annother user, close connection before and reconnect
				_ = c.db.Close()
			}
		}
	}

	if db, err = sql.Open("clickhouse", dsn); err != nil {
		err = errors.Wrapf(err, "")
		return nil, err
	}

	if err = db.Ping(); err != nil {
		err = errors.Wrapf(err, "")
		return nil, err
	}
	SetConnOptions(db)
	connction := Connection{
		dsn: dsn,
		db:  db,
	}
	ConnectPool.Store(host, connction)
	return db, nil
}

func SetConnOptions(conn *sql.DB) {
	conn.SetMaxOpenConns(2)
	conn.SetMaxIdleConns(0)
	conn.SetConnMaxIdleTime(10 * time.Second)
}

func CloseConns(hosts []string) {
	for _, host := range hosts {
		conn, ok := ConnectPool.LoadAndDelete(host)
		if ok {
			_ = conn.(Connection).db.Close()
		}
	}
}

func GetConnection(host string) *sql.DB {
	if conn, ok := ConnectPool.Load(host); ok {
		db := conn.(Connection).db
		err := db.Ping()
		if err == nil {
			return db
		}
	}
	return nil
}

func GetMergeTreeTables(engine string, db *sql.DB) ([]string, map[string][]string, error) {
	var rows *sql.Rows
	var databases []string
	var err error
	dbtables := make(map[string][]string)
	query := fmt.Sprintf("SELECT DISTINCT  database, name FROM system.tables WHERE (match(engine, '%s')) AND (database != 'system') ORDER BY database", engine)
	log.Logger.Debugf("query: %s", query)
	if rows, err = db.Query(query); err != nil {
		err = errors.Wrapf(err, "")
		return nil, nil, err
	}
	defer rows.Close()
	var tables []string
	var predbname string
	for rows.Next() {
		var database, name string
		if err = rows.Scan(&database, &name); err != nil {
			err = errors.Wrapf(err, "")
			return nil, nil, err
		}
		if database != predbname {
			if predbname != "" {
				dbtables[predbname] = tables
				databases = append(databases, predbname)
			}
			tables = []string{}
		}
		tables = append(tables, name)
		predbname = database
	}
	if predbname != "" {
		dbtables[predbname] = tables
		databases = append(databases, predbname)
	}
	return databases, dbtables, nil
}
