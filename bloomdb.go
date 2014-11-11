package bloomdb

import (
	"database/sql"
	"github.com/spf13/viper"
	elastigo "github.com/mattbaird/elastigo/lib"
)

type BloomDatabase struct {
	sqlConnStr string
	searchHosts []string
}

func (bdb *BloomDatabase) SqlConnection() (*sql.DB, error) {
	db, err := sql.Open("postgres", bdb.sqlConnStr)
	if err != nil {
		return nil, err
	}

	return db, nil
}

func (bdb *BloomDatabase) SearchConnection() (*elastigo.Conn) {
	conn := elastigo.NewConn()
	conn.SetHosts(bdb.searchHosts)
	return conn
}

func CreateDB () *BloomDatabase {
	return &BloomDatabase {
		viper.GetString("sqlConnStr"),
		viper.GetStringSlice("searchHosts"),
	}
}