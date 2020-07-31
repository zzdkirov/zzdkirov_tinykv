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
	config *config.Config;
}
//new Stand Alone Storage database, but It's hard to understand
func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	//vnimaniye: decide the path of db on our disk,
	dbpath:=conf.DBpath
	kvpath:=filepath.Join(dbpath,"kv")
	raftpath:=filepath.Join(dbpath,"raft")

	//vnimaniye: createDB(subpath, config) in engines.go and that's initial
	kvDB:=engine_util.CreateDB("kv",conf)
	raftDB:=engine_util.CreateDB("raft",conf)

	//vnimaniye: lab1 seems not to use raft path, refer to Shan's code and questioned
	return &StandAloneStorage{
		engine: engine_util.NewEngines(kvDB,raftDB,kvpath,raftpath),
		config: conf,
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
	return &StandAloneReader{
		kvTxn:   kvTxn,
	}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	return nil
}

type StandAloneReader{
	kvTxn *badger.Txn
}

func (s* StandAloneReader) GetCF(cf string, key []byte) ([]byte, error){
	value,err:=engine_util.GetCFFormTxn(s.kvTxn,cf,key)
	if err==badger.ErrKeyNotFound{
		return nil,nil
	}
	return value,nil
}