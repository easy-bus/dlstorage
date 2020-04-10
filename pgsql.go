package dlstorage

import (
	"fmt"

	"github.com/easy-bus/bus"
	"github.com/go-pg/pg/v9"
	"github.com/letsfire/utils"
)

type pgsqlDLStorage struct {
	sql string
	db  *pg.DB
}

func (sds *pgsqlDLStorage) Store(queue string, data []byte) error {
	_, err := sds.db.Exec(sds.sql, utils.GenerateSeqId(), queue, string(data))
	return err
}

func NewPGSQL(table string, db *pg.DB) bus.DLStorageInterface {
	return &pgsqlDLStorage{sql: fmt.Sprintf("INSERT INTO %s (id, queue, data) VALUES (?, ?, ?)", table), db: db}
}
