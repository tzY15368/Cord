package rdx

import (
	"github.com/hashicorp/go-memdb"
)

type RadixKV struct {
	db           *memdb.MemDB
	watchEnabled bool
}

type KV struct {
	Key   string
	Value string
}

func NewRadixKV(watchEnabled bool) *RadixKV {

	schema := &memdb.DBSchema{
		Tables: map[string]*memdb.TableSchema{
			"kv": &memdb.TableSchema{
				Name: "kv",
				Indexes: map[string]*memdb.IndexSchema{
					"id": &memdb.IndexSchema{
						Name:    "id",
						Unique:  true,
						Indexer: &memdb.StringFieldIndex{Field: "Key"},
					},
				},
			},
		},
	}
	db, err := memdb.NewMemDB(schema)
	if err != nil {
		panic(err)
	}
	rkv := RadixKV{
		watchEnabled: watchEnabled,
		db:           db,
	}
	return &rkv
}

// func (r *RadixKV) EvalCMD(*proto.ServiceArgs, bool, bool) (intf.IEvalResult, []byte) {

// 	txn := r.db.Txn()
// }
func (r *RadixKV) LoadSnapshot([]byte) {

}
