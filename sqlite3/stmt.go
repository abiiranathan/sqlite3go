package sqlite3

// #cgo LDFLAGS: -lsqlite3
// #include <sqlite3.h>
// #include <stdlib.h>
import "C"

import (
	"fmt"
	"unsafe"
)

type SQLite3Stmt struct {
	stmt *C.sqlite3_stmt
}

func (s *SQLite3) Prepare(query string) (*SQLite3Stmt, error) {
	cQuery := C.CString(query)
	defer C.free(unsafe.Pointer(cQuery))

	var stmt *C.sqlite3_stmt
	if rc := C.sqlite3_prepare_v2(s.db, cQuery, -1, &stmt, nil); rc != C.SQLITE_OK {
		return nil, fmt.Errorf("error preparing query: %s", C.GoString(C.sqlite3_errmsg(s.db)))
	}

	return &SQLite3Stmt{stmt: stmt}, nil
}

func (s *SQLite3Stmt) Close() error {
	return s.Finalize()
}

func (s *SQLite3Stmt) Exec(args ...interface{}) error {
	for i, arg := range args {
		switch v := arg.(type) {
		case int:
			if err := s.BindInt(i+1, v); err != nil {
				return err
			}
		case int64:
			if err := s.BindInt64(i+1, v); err != nil {
				return err
			}
		case float64:
		case float32:
			if err := s.BindFloat(i+1, float64(v)); err != nil {
				return err
			}
		case string:
			if err := s.BindText(i+1, v); err != nil {
				return err
			}
		case []byte:
			if err := s.BindBlob(i+1, v); err != nil {
				return err
			}
		case nil:
			if err := s.BindNull(i + 1); err != nil {
				return err
			}
		case bool:
			intVal := 0
			if v {
				intVal = 1
			}
			if err := s.BindInt(i+1, intVal); err != nil {
				return err
			}
		default:
			return fmt.Errorf("unsupported type %T", v)
		}
	}

	if _, err := s.Step(); err != nil {
		return err
	}

	return s.Reset()
}

func (s *SQLite3Stmt) Step() (bool, error) {
	rc := C.sqlite3_step(s.stmt)
	if rc == C.SQLITE_ROW {
		return true, nil
	}

	if rc == C.SQLITE_DONE {
		return false, nil
	}
	return false, s.error()
}

func (s *SQLite3Stmt) Reset() error {
	if rc := C.sqlite3_reset(s.stmt); rc != C.SQLITE_OK {
		return s.error()
	}
	return nil
}

func (s *SQLite3Stmt) Finalize() error {
	if rc := C.sqlite3_finalize(s.stmt); rc != C.SQLITE_OK {
		return s.error()
	}
	return nil
}

func (s *SQLite3Stmt) error() error {
	return fmt.Errorf("error binding int: %s", C.GoString(C.sqlite3_errmsg(C.sqlite3_db_handle(s.stmt))))
}

func (s *SQLite3Stmt) BindInt(index int, value int) error {
	if rc := C.sqlite3_bind_int(s.stmt, C.int(index), C.int(value)); rc != C.SQLITE_OK {
		return s.error()
	}
	return nil
}

func (s *SQLite3Stmt) BindInt64(index int, value int64) error {
	if rc := C.sqlite3_bind_int64(s.stmt, C.int(index), C.sqlite3_int64(value)); rc != C.SQLITE_OK {
		return s.error()
	}
	return nil
}

func (s *SQLite3Stmt) BindFloat(index int, value float64) error {
	if rc := C.sqlite3_bind_double(s.stmt, C.int(index), C.double(value)); rc != C.SQLITE_OK {
		return s.error()
	}
	return nil
}

func (s *SQLite3Stmt) BindText(index int, value string) error {
	cValue := C.CString(value)
	defer C.free(unsafe.Pointer(cValue))
	if rc := C.sqlite3_bind_text(s.stmt, C.int(index), cValue, C.int(len(value)), nil); rc != C.SQLITE_OK {
		return s.error()
	}
	return nil
}

func (s *SQLite3Stmt) BindBlob(index int, value []byte) error {
	if rc := C.sqlite3_bind_blob(s.stmt, C.int(index), unsafe.Pointer(&value[0]), C.int(len(value)), nil); rc != C.SQLITE_OK {
		return s.error()
	}
	return nil
}

func (s *SQLite3Stmt) BindNull(index int) error {
	if rc := C.sqlite3_bind_null(s.stmt, C.int(index)); rc != C.SQLITE_OK {
		return s.error()
	}
	return nil
}

// ======== column getters ========
func (s *SQLite3Stmt) ColumnInt(index int) int {
	return int(C.sqlite3_column_int(s.stmt, C.int(index)))
}

// ColumnBool returns the boolean value of the column at the given index.
// SQLite3 does not have a boolean type, so we use integers instead.
// 0 is false, 1 is true.
func (s *SQLite3Stmt) ColumnBool(index int) bool {
	return C.sqlite3_column_int(s.stmt, C.int(index)) == 1
}

func (s *SQLite3Stmt) ColumnInt64(index int) int64 {
	return int64(C.sqlite3_column_int64(s.stmt, C.int(index)))
}

func (s *SQLite3Stmt) ColumnFloat(index int) float64 {
	return float64(C.sqlite3_column_double(s.stmt, C.int(index)))
}

func (s *SQLite3Stmt) ColumnText(index int) string {
	return C.GoString((*C.char)(unsafe.Pointer(C.sqlite3_column_text(s.stmt, C.int(index)))))
}

func (s *SQLite3Stmt) ColumnBlob(index int) []byte {
	length := C.sqlite3_column_bytes(s.stmt, C.int(index))
	if length == 0 {
		return nil
	}
	return C.GoBytes(unsafe.Pointer(C.sqlite3_column_blob(s.stmt, C.int(index))), length)
}

func (s *SQLite3Stmt) IsColumnNull(index int) bool {
	return C.sqlite3_column_type(s.stmt, C.int(index)) == C.SQLITE_NULL
}
