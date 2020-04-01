package dlstorage

import (
	"fmt"
	"github.com/easy-bus/bus"
	"github.com/go-pg/pg"
)

type pgsqlDLStorage struct {
	sql string
	db  *pg.DB
}

func (sds *pgsqlDLStorage) Store(queue string, data []byte) error {
	_, err := sds.db.Exec(sds.sql, queue, data)
	return err
}

func NewPGSQL(table string, db *pg.DB) bus.DLStorageInterface {
	return &pgsqlDLStorage{sql: fmt.Sprintf("INSERT INTO %s (queue, data) VALUE (?, ?)", table), db: db}
}
