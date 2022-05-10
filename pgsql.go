package dlstorage

import (
	"fmt"
	"time"

	"github.com/easy-bus/bus"
	"github.com/go-pg/pg/v10"
)

// pgsqlDLModel
type pgsqlDLModel struct {
	Id         string `pg:"id,use_zero" json:"id"`
	Queue      string `pg:"queue,use_zero" json:"queue"`
	Data       string `pg:"data,use_zero" json:"data"`
	AllowRetry bool   `pg:"allow_retry,use_zero" json:"allow_retry"`
	CreatedAt  int64  `pg:"created_at,use_zero" json:"created_at"`
}

type pgsqlDLStorage struct {
	db    *pg.DB
	table string
}

func (sds *pgsqlDLStorage) Store(queue string, data []byte) error {
	sql := fmt.Sprintf("INSERT INTO %s (id, queue, data, allow_retry, created_at) VALUES (?, ?, ?, ?, ?)", sds.table)
	_, err := sds.db.Exec(sql, generateSeqId(), queue, string(data), false, time.Now().Unix())
	return err
}

func (sds *pgsqlDLStorage) Fetch(queue string) (map[string][]byte, error) {
	var bs = make(map[string][]byte)
	var ms = make([]pgsqlDLModel, 0)
	sql := fmt.Sprintf("SELECT * FROM %s WHERE queue = ? AND allow_retry = ?", sds.table)
	_, err := sds.db.Query(&ms, sql, queue, true)
	for _, m := range ms {
		bs[m.Id] = []byte(m.Data)
	}
	return bs, err
}

func (sds *pgsqlDLStorage) Remove(id string) error {
	sql := fmt.Sprintf("DELETE FROM %s WHERE id = ?", sds.table)
	_, err := sds.db.Exec(sql, id)
	return err
}

func NewPGSQL(table string, db *pg.DB) bus.DLStorageInterface {
	return &pgsqlDLStorage{db: db, table: table}
}
