package standalone_storage

import (
	"github.com/Connor1996/badger"		//cf_iterator: code to iterate over a whole column family in badger.
	"path/filepath"                                           //go standardib
	"github.com/pingcap-incubator/tinykv/kv/config"           //served
	"github.com/pingcap-incubator/tinykv/kv/storage"          //reserved
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"   //reserved
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util" //keeping engines required by unistorexib
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	engine *engine_util.Engines;
}
//new Stand Alone Storage database, but It's hard to understand
func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	//vnimaniye: decide the path of db on our disk, kv yi raft(useless) path
	dbpath:=conf.DBPath
	kvpath:=filepath.Join(dbpath,"kv")
	raftpath:=filepath.Join(dbpath,"raft")

	//vnimaniye: createDB(subpath, config) in engines.go and that's initial our db
	kvDB:=engine_util.CreateDB("kv",conf)
	raftDB:=engine_util.CreateDB("raft",conf)

	//vnimaniye: lab1 seems not to use raft path, refer to Shan's code and questioned
	return &StandAloneStorage{
		engine: engine_util.NewEngines(kvDB,raftDB,kvpath,raftpath),
	}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	return s.engine.Close()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	//vnimaniye: click the NewTransaction and get some commit
	var txn=s.engine.Kv.NewTransaction(false)
	//golang return type is interface but value is a struce
	//due to interface is realized by struct
	return &InstanceStorageReader{
		kvTxn:   txn,
	}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	//write include delete yi put that modify db
	//for batch
	for _,m :=range batch{
		switch m.Data.(type) {
		case storage.Put:
			put := m.Data.(storage.Put)
			err := engine_util.PutCF(s.engine.Kv, put.Cf, put.Key, put.Value)
			if err != nil {
				return err
			}
		case storage.Delete:
			delete:=m.Data.(storage.Delete)
			err:= engine_util.DeleteCF(s.engine.Kv,delete.Cf,delete.Key)
			if err!=nil{
				return err
			}
		}
	}

	return nil
}

type InstanceStorageReader struct{
	kvTxn *badger.Txn
}

func (r* InstanceStorageReader) GetCF(cf string, key []byte) ([]byte, error){
	value,err:=engine_util.GetCFFromTxn(r.kvTxn,cf,key)
	if err!=nil{
		return nil,nil
	}
	return value,nil
}

func (r* InstanceStorageReader) IterCF(cf string) engine_util.DBIterator{
	return engine_util.NewCFIterator(cf, r.kvTxn)
}

func (r *InstanceStorageReader) Close() {
	r.kvTxn.Discard()
}