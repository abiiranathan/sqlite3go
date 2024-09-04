package sqlite3

import (
	"testing"
	"time"
)

func setupDB() *SQLite3 {
	db, err := OpenSQLite3("todos.db")
	if err != nil {
		panic(err)
	}
	db.EnableWalMode(true)

	err = db.Exec("CREATE TABLE IF NOT EXISTS todos (id INTEGER PRIMARY KEY, text TEXT, done BOOLEAN)")
	if err != nil {
		panic(err)
	}

	return db
}

func BenchmarkExec(b *testing.B) {
	db := setupDB()
	defer db.Close()

	for i := 0; i < b.N; i++ {
		err := db.Exec("INSERT INTO todos (text, done) VALUES ('Benchmark Exec', false)")
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkQuery(b *testing.B) {
	db := setupDB()
	defer db.Close()

	for i := 0; i < b.N; i++ {
		stmt, err := db.Query("SELECT id, text, done FROM todos")
		if err != nil {
			b.Fatal(err)
		}
		stmt.Close()
	}
}

func BenchmarkSelect(b *testing.B) {
	db := setupDB()
	defer db.Close()

	stmt, err := db.Query("SELECT id, text, done FROM todos")
	if err != nil {
		b.Fatal(err)
	}
	defer stmt.Close()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for {
			hasRow, err := stmt.Step()
			if err != nil {
				b.Fatal(err)
			}
			if !hasRow {
				break
			}
			_ = stmt.ColumnInt(0)
			_ = stmt.ColumnText(1)
			_ = stmt.ColumnBool(2)
		}
	}
}

func BenchmarkInsertsAndQueries(b *testing.B) {
	db := setupDB()
	defer db.Close()

	start := time.Now()
	stmt, err := db.Prepare("INSERT INTO todos (text, done) VALUES (?, ?)")
	if err != nil {
		b.Fatal(err)
	}
	defer stmt.Close()

	for i := 0; i < 200; i++ {
		err := stmt.Exec("Benchmark Inserts", false)
		if err != nil {
			b.Fatal(err)
		}
	}

	elapsed := time.Since(start)
	b.Logf("Inserts took %s", elapsed)

	start = time.Now()
	for i := 0; i < b.N; i++ {
		rows, err := db.Query("SELECT id, text, done FROM todos")
		if err != nil {
			b.Fatal(err)
		}
		defer rows.Close()

		for {
			hasRow, err := rows.Step()
			if err != nil {
				b.Fatal(err)
			}
			if !hasRow {
				break
			}
			_ = rows.ColumnInt(0)
			_ = rows.ColumnText(1)
			_ = rows.ColumnBool(2)
		}
	}

	elapsed = time.Since(start)
	b.Logf("Queries took %s", elapsed)

}

// Benchmark BulkInsert
func BenchmarkBulkInsert(b *testing.B) {
	db := setupDB()
	defer db.Close()

	args := make([][]string, 0, 100000)
	for i := 0; i < 100000; i++ {
		args = append(args, []string{"Benchmark BulkInsert", "false"})
	}

	start := time.Now()
	err := db.BulkInsert("INSERT INTO todos (text, done) VALUES (?, ?)", args)
	if err != nil {
		b.Fatal(err)
	}

	elapsed := time.Since(start)
	b.Logf("BulkInsert took %s", elapsed)
}
