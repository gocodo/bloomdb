package bloomdb

import (
	"bytes"
	"database/sql"
	"github.com/lib/pq"
	"text/template"
	"time"
	"fmt"
)

var fns = template.FuncMap{
	"eq": func(x, y interface{}) bool {
		return x == y
	},
	"sub": func(y, x int) int {
		return x - y
	},
}

type upsertInfo struct {
	Table   string
	Columns []string
	ParentKey string
	CreatedAt string
	UpdatedAt string
}

func buildQuery(table string, columns []string, parentKey string) (string, error) {
	buf := new(bytes.Buffer)
	t, err := template.New("upsert.sql.template").Funcs(fns).Parse(upsertSql)
	if err != nil {
		return "", err
	}
	now := time.Now().UTC().Format(time.RFC3339)
	info := upsertInfo{table, columns, parentKey, now, now}
	err = t.Execute(buf, info)
	if err != nil {
		return "", err
	}
	return buf.String(), nil
}

func Upsert(db *sql.DB, table string, columns []string, rows chan []string, parentKey string) error {
	query, err := buildQuery(table, columns, parentKey)
	if err != nil {
		return err
	}

	txn, err := db.Begin()
	if err != nil {
		return err
	}

	_, err = txn.Exec("CREATE TEMP TABLE " + table + "_temp(LIKE " + table + ") ON COMMIT DROP;")
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

	_, err = txn.Exec(query)
	if err != nil {
		return err
	}

	err = txn.Commit()
	if err != nil {
		return err
	}

	return nil
}
