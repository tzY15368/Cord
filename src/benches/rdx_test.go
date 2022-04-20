package benches

import (
	"fmt"
	"math/rand"
	"runtime"
	"sync"
	"testing"
	"time"

	"6.824/common"
	"github.com/armon/go-radix"
	"github.com/hashicorp/go-memdb"
)

// 8读2写
var key_template = []string{"999999999-aaa-cc-d-%d", "999999999-bbb-ee-%d-s", "999999999-uiuio-%d-32", "999999999-mnbmbmnb-%d"}

func BenchmarkStoHT(b *testing.B) {
	data := make(map[string]string)
	var mu sync.Mutex
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var wg sync.WaitGroup
		// wg.Add(1)
		// wg.Add(1)
		// go func() {
		// 	mu.Lock()
		// 	defer mu.Unlock()
		// 	for j := 0; j < 2; j++ {
		// 		key := fmt.Sprintf(key_template[j], j)
		// 		v := common.common.GenRandomBytes(20, 50)
		// 		data[key] = string(*v)
		// 	}
		// 	wg.Done()
		// }()
		// go func() {
		// 	mu.Lock()
		// 	defer mu.Unlock()
		// 	for j := 2; j < 4; j++ {
		// 		key := fmt.Sprintf(key_template[j], j)
		// 		v := common.common.GenRandomBytes(20, 50)
		// 		data[key] = string(*v)
		// 	}
		// 	wg.Done()
		// }()
		for j := 0; j < 16; j++ {
			wg.Add(1)
			go func(k int) {
				mu.Lock()
				defer mu.Unlock()
				key := fmt.Sprintf(key_template[k%4], k%4)
				d := data[key]
				d += "_"
				wg.Done()
			}(j)
		}
		wg.Wait()

	}
}

func BenchmarkStoImmRdx(b *testing.B) {
	// tree := iradix.New()
	// b.ResetTimer()
	// for i := 0; i < b.N; i++ {
	// 	var wg sync.WaitGroup
	// 	wg.Add(1)
	// 	wg.Add(1)
	// 	go func() {
	// 		tx := tree.Txn()
	// 		mu.Lock()
	// 		defer mu.Unlock()
	// 		for j := 0; j < 2; j++ {
	// 			key := fmt.Sprintf(key_template[j], j)
	// 			v := common.common.GenRandomBytes(20, 50)
	// 			data[key] = string(*v)
	// 		}
	// 		wg.Done()
	// 	}()
	// 	go func() {
	// 		mu.Lock()
	// 		defer mu.Unlock()
	// 		for j := 2; j < 4; j++ {
	// 			key := fmt.Sprintf(key_template[j], j)
	// 			v := common.common.GenRandomBytes(20, 50)
	// 			data[key] = string(*v)
	// 		}
	// 		wg.Done()
	// 	}()
	// 	for j := 0; j < 16; j++ {
	// 		wg.Add(1)
	// 		go func(k int) {
	// 			mu.Lock()
	// 			defer mu.Unlock()
	// 			key := fmt.Sprintf(key_template[k%4], k%4)
	// 			d := data[key]
	// 			d += "_"
	// 			wg.Done()
	// 		}(j)
	// 	}
	// 	wg.Wait()

	// }
	// for i := 0; i < b.N; i++ {
	// 	tx := tree.Txn()

	// 	for j := 0; j < 4; j++ {
	// 		key := fmt.Sprintf(key_template[j], j)
	// 		v := common.common.GenRandomBytes(20, 50)
	// 		tx.Insert([]byte(key), string(*v))
	// 	}
	// 	o := rand.Intn(4)
	// 	key := fmt.Sprintf(key_template[o], o)
	// 	data, _ := tx.Get([]byte(key))
	// 	v := data.(string)
	// 	v += "_"
	// 	tree = tx.Commit()
	// }
}

var N = 10000

func TestHT(t *testing.T) {
	data := make(map[string]string)
	var mu sync.RWMutex
	start := time.Now()
	for i := 0; i < 1; i++ {
		var wg sync.WaitGroup
		// wg.Add(1)
		// wg.Add(1)
		// go func() {
		// 	mu.Lock()
		// 	defer mu.Unlock()
		// 	for j := 0; j < 2; j++ {
		// 		key := fmt.Sprintf(key_template[j], j)
		// 		v := common.common.GenRandomBytes(20, 50)
		// 		data[key] = string(*v)
		// 	}
		// 	wg.Done()
		// }()
		// go func() {
		// 	mu.Lock()
		// 	defer mu.Unlock()
		// 	for j := 2; j < 4; j++ {
		// 		key := fmt.Sprintf(key_template[j], j)
		// 		v := common.common.GenRandomBytes(20, 50)
		// 		data[key] = string(*v)
		// 	}
		// 	wg.Done()
		// }()
		for j := 0; j < 12; j++ {
			wg.Add(1)
			go func(k int) {
				time.Sleep(1 * time.Second)
				if k > 10 {
					wg.Done()
					return
				}
				mu.RLock()
				defer mu.RUnlock()
				key := fmt.Sprintf(key_template[k%4], k%4)
				d := data[key]
				d += "_"
				wg.Done()
			}(j)
		}
		wg.Wait()

	}
	since := time.Since(start)
	taken := since.Nanoseconds() / int64(N)
	fmt.Println(taken, "ns/op")
}

func TestMemDB(t *testing.T) {
	runtime.GOMAXPROCS(2)

	type KV struct {
		Key   string
		Value string
	}
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
	start := time.Now()
	for i := 0; i < N; i++ {
		var wg sync.WaitGroup
		// wg.Add(1)
		// wg.Add(1)
		// go func() {
		// 	tx := db.Txn(true)
		// 	for j := 0; j < 2; j++ {
		// 		key := fmt.Sprintf(key_template[j], j)
		// 		v := common.common.GenRandomBytes(20, 50)
		// 		err := tx.Insert("kv", KV{Key: key, Value: string(*v)})
		// 		if err != nil {
		// 			panic(err)
		// 		}
		// 	}
		// 	tx.Commit()
		// 	wg.Done()
		// }()
		// go func() {
		// 	tx := db.Txn(true)
		// 	for j := 2; j < 4; j++ {
		// 		key := fmt.Sprintf(key_template[j], j)
		// 		v := common.common.GenRandomBytes(20, 50)
		// 		err := tx.Insert("kv", KV{Key: key, Value: string(*v)})
		// 		if err != nil {
		// 			panic(err)
		// 		}
		// 	}
		// 	tx.Commit()
		// 	wg.Done()
		// }()
		for j := 0; j < 16; j++ {
			wg.Add(1)
			go func(k int) {
				tx := db.Txn(false)
				key := fmt.Sprintf(key_template[k%4], k%4)
				raw, err := tx.First("kv", "id", key)
				d := ""
				if err == nil && raw != nil {
					d = raw.(KV).Key
				}
				d += "_"
				tx.Commit()
				wg.Done()
			}(j)
		}
		wg.Wait()

	}
	since := time.Since(start)
	taken := since.Nanoseconds() / int64(N)
	fmt.Println(taken, "ns/op")
}

func BenchmarkStoMemDB(b *testing.B) {

	type KV struct {
		Key   string
		Value string
	}
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
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var wg sync.WaitGroup
		// wg.Add(1)
		// wg.Add(1)
		// go func() {
		// 	tx := db.Txn(true)
		// 	for j := 0; j < 2; j++ {
		// 		key := fmt.Sprintf(key_template[j], j)
		// 		v := common.common.GenRandomBytes(20, 50)
		// 		err := tx.Insert("kv", KV{Key: key, Value: string(*v)})
		// 		if err != nil {
		// 			panic(err)
		// 		}
		// 	}
		// 	tx.Commit()
		// 	wg.Done()
		// }()
		// go func() {
		// 	tx := db.Txn(true)
		// 	for j := 2; j < 4; j++ {
		// 		key := fmt.Sprintf(key_template[j], j)
		// 		v := common.common.GenRandomBytes(20, 50)
		// 		err := tx.Insert("kv", KV{Key: key, Value: string(*v)})
		// 		if err != nil {
		// 			panic(err)
		// 		}
		// 	}
		// 	tx.Commit()
		// 	wg.Done()
		// }()
		for j := 0; j < 16; j++ {
			wg.Add(1)
			go func(k int) {
				tx := db.Txn(false)
				key := fmt.Sprintf(key_template[k%4], k%4)
				raw, err := tx.First("kv", "id", key)
				d := ""
				if err == nil && raw != nil {
					d = raw.(KV).Key
				}
				d += "_"
				tx.Commit()
				wg.Done()
			}(j)
		}
		wg.Wait()

	}
}

func BenchmarkStoMuRdx(b *testing.B) {

	tree := radix.New()
	var mu sync.Mutex
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mu.Lock()
		for j := 0; j < 4; j++ {
			key := fmt.Sprintf(key_template[j], j)
			v := common.GenRandomBytes(20, 50)
			tree.Insert(key, string(*v))
		}
		o := rand.Intn(4)
		key := fmt.Sprintf(key_template[o], o)
		data, _ := tree.Get(key)
		v := data.(string)
		v += "_"
		mu.Unlock()
	}
}
