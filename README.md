# sqlite3go

sqlite3go is a simple sqlite3 wrapper for golang.

Its is designed as a simple guide to wrap C libraries in golang
using cgo. It's not meant for production use.

For production use, you should use the official sqlite3 driver
for go.

## Installation

```bash
go get github.com/abiiranathan/sqlite3go
```

## Usage

```go
package main

import (
	"fmt"

	"github.com/abiiranathan/sqlite3go/sqlite3"
)

func main() {
	db, err := sqlite3.OpenSQLite3("sqlite3/todos.db")
	if err != nil {
		panic(err)
	}
	defer db.Close()

	// create a todos table if it doesn't exist (id, text, done)
	err = db.Exec("CREATE TABLE IF NOT EXISTS todos (id INTEGER PRIMARY KEY, text TEXT, done BOOLEAN)")
	if err != nil {
		panic(err)
	}

	// insert a todo
	err = db.Exec("INSERT INTO todos (text, done) VALUES ('Learn Go', false)")
	if err != nil {
		panic(err)
	}

	// insert another todo
	err = db.Exec("INSERT INTO todos (text, done) VALUES ('Learn SQLite3', false)")
	if err != nil {
		panic(err)
	}

	// query all todos
	stmt, err := db.Query("SELECT id, text, done FROM todos")
	if err != nil {
		panic(err)
	}
	defer stmt.Close()

	// iterate over the rows
	for {
		hasRow, err := stmt.Step()
		if err != nil {
			panic(err)
		}

		if !hasRow {
			break
		}

		id := stmt.ColumnInt(0)
		text := stmt.ColumnText(1)
		done := stmt.ColumnBool(2)

		fmt.Printf("id: %d, text: %s, done: %t\n", id, text, done)
	}
}
```

## License

MIT
