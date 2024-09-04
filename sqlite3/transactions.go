package sqlite3

// #cgo LDFLAGS: -lsqlite3
// #include <sqlite3.h>
// #include <stdlib.h>
import "C"

func (db *SQLite3) Begin() error {
	sql := "BEGIN TRANSACTION"
	return db.Exec(sql)
}

func (db *SQLite3) Commit() error {
	sql := "COMMIT"
	return db.Exec(sql)
}

func (db *SQLite3) Rollback() error {
	sql := "ROLLBACK"
	return db.Exec(sql)
}

func (db *SQLite3) Transaction(fn func() error) error {
	if err := db.Begin(); err != nil {
		return err
	}

	if err := fn(); err != nil {
		db.Rollback()
		return err
	}

	return db.Commit()
}
