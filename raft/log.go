// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// RaftLog manage the log entries, its struct look like:
//
//  snapshot/first.....applied....committed....stabled.....last
//  --------|------------------------------------------------|
//                            log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	// all entries that have not yet compact.
	entries []pb.Entry



	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
	//pre entries index
	preindex uint64
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	prei,_:=storage.FirstIndex()
	stab,_:=storage.LastIndex()
	ent,_:=storage.Entries(prei,stab+1)
	return &RaftLog{
		storage: storage,
		entries: ent,
		preindex: prei,
		stabled: stab,
		applied: prei-1,
	}
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	if l.stabled+1>l.LastIndex(){
		return make([]pb.Entry,0)
	}
	ents:=l.GetLastEntries(l.stabled+1,l.LastIndex()+1)
	return ents
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() []pb.Entry {
	// Your Code Here (2A).

	if l.applied+1>l.LastIndex(){
		return make([]pb.Entry,0)
	}
	ents:=l.GetLastEntries(l.applied+1,l.committed+1)
	return ents
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	entrieslth:=len(l.entries)
	if entrieslth>0 {
		return l.preindex+uint64(entrieslth)-1
	}

	lastindex,_:=l.storage.LastIndex()
	return lastindex
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	if i>l.LastIndex(){
		return 0,nil
	}
	if len(l.entries)>0 && i>=l.preindex{
		return l.entries[i-l.preindex].Term,nil
	}
	return l.storage.Term(i)
}

func (l *RaftLog) LastTerm() uint64{
	lastterm,_:=l.Term(l.LastIndex())
	return lastterm
}


func (l *RaftLog) GetLastEntries(from uint64,to uint64) []pb.Entry{

	//log.Infof("%d,%d",from,to)
	if(from==to){
		return nil
	}
	var sents []pb.Entry
	var storageentries []pb.Entry
	var memoryentries []pb.Entry
	if len(l.entries)>0{
		if from<l.preindex{
			storageentries,_=l.storage.Entries(from,max(to,l.preindex))
			sents=storageentries
		}

		if to>l.preindex{
			memoryentries=l.entries[max(from,l.preindex)-l.preindex : to-l.preindex]
		}
		sents = append(sents, storageentries...)
		sents = append(sents, memoryentries...)
		return sents
	}else{
		storageentries,_:=l.storage.Entries(from,to)
		return storageentries
	}
}

func (l *RaftLog) appendEntries(entries ...pb.Entry){
	if len(entries)==0{
		return
	}
	lastindex:=entries[0].Index-1
	if len(l.entries)>0 {
		if lastindex==l.LastIndex(){
			//if append entries equals last logindex add now entries lth means normal situation and append
			l.entries=append(l.entries,entries...)
		}else if lastindex< l.preindex{
			//get new entries
			l.preindex=lastindex+1
			l.entries=entries
		}else{
			//update new entries instead of previous
			l.entries=append([]pb.Entry{},l.entries[:lastindex+1-l.preindex]...)
			l.entries=append(l.entries,entries...)
		}
	}else{
		//now log lth==0, assign
		l.preindex=lastindex+1
		l.entries=entries
	}
	if l.stabled>lastindex{
		l.stabled=lastindex
	}
}