package sqlite3

/*

#cgo LDFLAGS: -lsqlite3

#include <sqlite3.h>
#include <stdlib.h>

// we can't use variadic functions in cgo, so we us a helper function
int sqlite3_db_config_helper(sqlite3 *db, int op, int value) {
	return sqlite3_db_config(db, op, value);
}

int bulk_insert(sqlite3 *db, const char *sql, int num_rows, const char **values) {
    sqlite3_stmt *stmt;
    int rc;

    rc = sqlite3_prepare_v2(db, sql, -1, &stmt, 0);
    if (rc != SQLITE_OK) {
        return rc;
    }

    sqlite3_exec(db, "BEGIN TRANSACTION", 0, 0, 0);

    for (int i = 0; i < num_rows; i++) {
        int num_cols = sqlite3_bind_parameter_count(stmt);
        for (int j = 0; j < num_cols; j++) {
            rc = sqlite3_bind_text(stmt, j+1, values[i*num_cols + j], -1, SQLITE_STATIC);
            if (rc != SQLITE_OK) {
                sqlite3_finalize(stmt);
                sqlite3_exec(db, "ROLLBACK", 0, 0, 0);
                return rc;
            }
        }

        rc = sqlite3_step(stmt);
        if (rc != SQLITE_DONE) {
            sqlite3_finalize(stmt);
            sqlite3_exec(db, "ROLLBACK", 0, 0, 0);
            return rc;
        }

        sqlite3_reset(stmt);
    }

    sqlite3_finalize(stmt);
    sqlite3_exec(db, "COMMIT", 0, 0, 0);

    return SQLITE_OK;
}
*/
import "C"
import (
	"fmt"
	"unsafe"
)

type SQLite3 struct {
	db *C.sqlite3
}

func OpenSQLite3(filename string) (*SQLite3, error) {
	cFilename := C.CString(filename)
	defer C.free(unsafe.Pointer(cFilename))

	var db *C.sqlite3
	if rc := C.sqlite3_open(cFilename, &db); rc != C.SQLITE_OK {
		return nil, fmt.Errorf("can't open database: %s", C.GoString(C.sqlite3_errmsg(db)))
	}

	return &SQLite3{db: db}, nil
}

func (s *SQLite3) Close() {
	ret := C.sqlite3_close(s.db)
	if ret != C.SQLITE_OK {
		fmt.Println("Error closing database")
	}
}

func (s *SQLite3) Exec(query string) error {
	cQuery := C.CString(query)
	defer C.free(unsafe.Pointer(cQuery))

	var errMsg *C.char
	if rc := C.sqlite3_exec(s.db, cQuery, nil, nil, &errMsg); rc != C.SQLITE_OK {
		return fmt.Errorf("error executing query: %s", C.GoString(errMsg))
	}

	return nil
}

// BulkInsert performs a bulk insert operation on the SQLite3 database.
// All parameters are passed as a 2D slice of strings. The first dimension
// represents the rows, and the second dimension represents the columns.
// The number of columns must match the number of placeholders in the query.
// This is a convenience method for inserting multiple rows at once avoiding the CGO overhead.
// The query must contain placeholders for the values, e.g. "INSERT INTO table (col1, col2) VALUES (?, ?)".
func (s *SQLite3) BulkInsert(query string, values [][]string) error {
	cQuery := C.CString(query)
	defer C.free(unsafe.Pointer(cQuery))

	if len(values) == 0 {
		return nil
	}

	numRows := len(values)
	numCols := len(values[0])

	// Flatten the 2D slice into a 1D slice
	flatValues := make([]string, 0, numRows*numCols)
	for _, row := range values {
		flatValues = append(flatValues, row...)
	}

	// Convert Go strings to C strings
	cValues := make([]*C.char, len(flatValues))
	for i, v := range flatValues {
		cValues[i] = C.CString(v)
	}

	defer func() {
		for _, v := range cValues {
			C.free(unsafe.Pointer(v))
		}
	}()

	// Create a C array of C strings
	cValuesPtr := (**C.char)(unsafe.Pointer(&cValues[0]))

	rc := C.bulk_insert(s.db, cQuery, C.int(numRows), cValuesPtr)
	if rc != C.SQLITE_OK {
		return fmt.Errorf("bulk insert failed: %s", C.GoString(C.sqlite3_errmsg(s.db)))
	}

	return nil
}

func (s *SQLite3) Query(query string) (*SQLite3Stmt, error) {
	cQuery := C.CString(query)
	defer C.free(unsafe.Pointer(cQuery))

	var stmt *C.sqlite3_stmt
	if rc := C.sqlite3_prepare_v2(s.db, cQuery, -1, &stmt, nil); rc != C.SQLITE_OK {
		return nil, fmt.Errorf("error preparing query: %s", C.GoString(C.sqlite3_errmsg(s.db)))
	}

	return &SQLite3Stmt{stmt: stmt}, nil
}

func (s *SQLite3) LastInsertRowID() int64 {
	return int64(C.sqlite3_last_insert_rowid(s.db))
}

func (s *SQLite3) Changes() int {
	return int(C.sqlite3_changes(s.db))
}

func (s *SQLite3) TotalChanges() int {
	return int(C.sqlite3_total_changes(s.db))
}

func (s *SQLite3) ErrorCode() int {
	return int(C.sqlite3_errcode(s.db))
}

func (s *SQLite3) ErrorMsg() string {
	return C.GoString(C.sqlite3_errmsg(s.db))
}

// Enum constants for sqlite3_db_config options
/*
https://www.sqlite.org/c3ref/c_dbconfig_defensive.html
*/
type Config int

const (
	SQLITE_DBCONFIG_MAINDBNAME            Config = 1000 + iota // Configures the name of the main database.
	SQLITE_DBCONFIG_LOOKASIDE                                  // Configures the lookaside memory allocator.
	SQLITE_DBCONFIG_ENABLE_FKEY                                // Enables or disables foreign key constraints.
	SQLITE_DBCONFIG_ENABLE_TRIGGER                             // Enables or disables triggers.
	SQLITE_DBCONFIG_ENABLE_FTS3_TOKENIZER                      // Enables or disables FTS3 tokenizers.
	SQLITE_DBCONFIG_ENABLE_LOAD_EXTENSION                      // Enables or disables loading extensions.
	SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE                           // Configures the automatic checkpoint behavior.
	SQLITE_DBCONFIG_ENABLE_QPSG                                // Enables or disables the query planner stability guarantee.
	SQLITE_DBCONFIG_TRIGGER_EQP                                // Enables or disables the trigger evaluation plan.
	SQLITE_DBCONFIG_RESET_DATABASE                             // Configures the database reset behavior.
	SQLITE_DBCONFIG_DEFENSIVE                                  // Configures the defensive mode setting.
	SQLITE_DBCONFIG_WRITABLE_SCHEMA                            // Configures the writable schema setting.
	SQLITE_DBCONFIG_LEGACY_ALTER_TABLE                         // Enables or disables legacy behavior for ALTER TABLE.
	SQLITE_DBCONFIG_DQS_DML                                    // Enables or disables double-quoted string literals in DML statements.
	SQLITE_DBCONFIG_DQS_DDL                                    // Enables or disables double-quoted string literals in DDL statements.
	SQLITE_DBCONFIG_ENABLE_VIEW                                // Enables or disables the use of views in SQL statements.
	SQLITE_DBCONFIG_LEGACY_FILE_FORMAT                         // Enables or disables the legacy file format.
	SQLITE_DBCONFIG_TRUSTED_SCHEMA                             // Enables or disables the trusted schema setting.
	SQLITE_DBCONFIG_STMT_SCANSTATUS                            // Enables or disables the statement scan status setting.
	SQLITE_DBCONFIG_REVERSE_SCANORDER                          // Enables or disables the reverse scan order setting.
	SQLITE_DBCONFIG_MAX                   = 1019               // The maximum value for a SQLiteConfig constant.
)

func (s *SQLite3) Config(config Config, value int) error {
	if s.db == nil {
		return fmt.Errorf("database connection is not initialized")
	}

	if config < SQLITE_DBCONFIG_MAINDBNAME || config > SQLITE_DBCONFIG_MAX {
		return fmt.Errorf("invalid configuration option: %d", config)
	}

	switch config {
	// allocator requires 3 arguments
	case SQLITE_DBCONFIG_LOOKASIDE:
		return fmt.Errorf("sqlite3_db_config with SQLITE_DBCONFIG_LOOKASIDE is not supported")
	}

	rc := C.sqlite3_db_config_helper(s.db, C.int(config), C.int(value))
	if rc != C.SQLITE_OK {
		return fmt.Errorf("sqlite3_db_config failed: %d", rc)
	}
	return nil
}

// Enable foreign key constraints
func (db *SQLite3) EnableForeignKeyConstraints(enable bool) error {
	var on int
	if enable {
		on = 1
	}
	return db.Config(SQLITE_DBCONFIG_ENABLE_FKEY, on)
}

// enable write-ahead logging (WAL) mode
func (db *SQLite3) EnableWalMode(enable bool) error {
	return db.Exec("PRAGMA journal_mode=WAL")
}

// enable synchronous mode
func (db *SQLite3) EnableSynchronousMode(enable bool) error {
	return db.Exec("PRAGMA synchronous=FULL")
}

// enable auto-vacuum mode
func (db *SQLite3) EnableAutoVacuumMode(enable bool) error {
	return db.Exec("PRAGMA auto_vacuum=FULL")
}

// default rollback journal mode
func (db *SQLite3) EnableDefaultJournalMode() error {
	return db.Exec("PRAGMA journal_mode=DELETE")
}
