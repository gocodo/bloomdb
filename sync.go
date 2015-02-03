package bloomdb

import (
	"bytes"
	"database/sql"
	"github.com/lib/pq"
	"text/template"
	"time"
	"fmt"
)

type syncInfo struct {
	Table   string
	Columns []string
	CreatedAt string
	UpdatedAt string
}

func buildSyncQuery(table string, columns []string) (string, error) {
	buf := new(bytes.Buffer)
	t, err := template.New("sync.sql.template").Funcs(fns).Parse(syncSql)
	if err != nil {
		return "", err
	}
	now := time.Now().UTC().Format(time.RFC3339)
	info := syncInfo{table, columns, now, now}
	err = t.Execute(buf, info)
	if err != nil {
		return "", err
	}
	return buf.String(), nil
}

func Sync(db *sql.DB, table string, columns []string, rows chan []string) error {
	query, err := buildSyncQuery(table, columns)
	if err != nil {
		return err
	}

	txn, err := db.Begin()
	if err != nil {
		return err
	}

	_, err = db.Exec("DROP TABLE IF EXISTS " + table + "_temp;")
	if err != nil {
		return err
	}

	_, err = txn.Exec("CREATE TABLE " + table + "_temp(LIKE " + table + " INCLUDING INDEXES);")
	if err != nil {
		return err
	}

	stmt, err := txn.Prepare(pq.CopyIn(table+"_temp", columns...))
	if err != nil {
		return err
	}

	for rawRow := range rows {
		row := make([]interface{}, len(rawRow))
		for i, column := range rawRow {
			if column == "" {
				row[i] = nil
			} else {
				row[i] = column
			}
		}

		_, err = stmt.Exec(row...)
		if err != nil {
			fmt.Println("table", table, "row", row)
			return err
		}
	}

	_, err = stmt.Exec()
	if err != nil {
		return err
	}

	err = txn.Commit()
	if err != nil {
		return err
	}

	_, err = db.Exec("analyze " + table + "_temp (id, revision)")
	if err != nil {
		return err
	}

	_, err = db.Exec("analyze " + table + " (id, revision)")
	if err != nil {
		return err
	}

	_, err = db.Exec(query)
	if err != nil {
		return err
	}

	_, err = db.Exec("DROP TABLE " + table + "_temp;")
	if err != nil {
		return err
	}

	return nil
}
