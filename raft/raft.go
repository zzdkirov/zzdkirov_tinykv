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
	"errors"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"math/rand"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	//random time out points: avoiding tickets evenly partition
	//range:[electiontimeout,2*electiontimeout]
	randomelectionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// number of ticks since it reached last electionTimeout
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).

	//> - When start a new raft, get the last stabled state from `Storage` to initialize `raft.Raft` and `raft.RaftLog`


	raft := &Raft{}

	raft.Prs= make(map[uint64]*Progress)
	raft.id=c.ID
	raft.Lead=0

	raft.votes=make(map[uint64]bool)
	raft.State=StateFollower
	raft.heartbeatTimeout=c.HeartbeatTick
	raft.electionTimeout=c.ElectionTick
	raft.RaftLog=newLog(c.Storage)
	raft.msgs=make([]pb.Message,0)
	raft.randomelectionTimeout=c.ElectionTick + rand.Intn(c.ElectionTick)

	lasti:=raft.RaftLog.LastIndex()

	//match always initialize with its last matchlogindex(if not avaliable zero)
	//initialize every node's log progress and for others zero
	for i:=1;i<=len(c.peers);i++{
		if raft.id==uint64(i) {
			raft.Prs[uint64(i)]=&Progress{
				Match: lasti,
				Next: lasti+1,
			}
		}else{
				raft.Prs[uint64(i)]=&Progress{
					Match: 0,
					Next:  lasti + 1,
				}
			}
		}

	return raft
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).

	var logTerm uint64
	var index uint64
	ents:=make([]*pb.Entry,0)
	lasti:=r.RaftLog.LastIndex()
	//log.Infof("mention %d,%d",to,len(r.Prs))
	if lasti < r.Prs[to].Next{
		logTerm=r.RaftLog.LastTerm()
		index=lasti
	}else{
		entries:=r.RaftLog.GetLastEntries(r.Prs[to].Next,r.RaftLog.LastIndex()+1)
		index=r.Prs[to].Next-1
		logTerm,_=r.RaftLog.Term(index)
		for _,e:=range entries{
			temp:=e
			ents=append(ents,&temp)
		}
	}

	//send
	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		From:    r.id,
		LogTerm: logTerm,
		Index: index,
		To:      to,
		Term:    r.Term,
		Entries: ents,
		Commit: r.RaftLog.committed,
	}
	r.msgs = append(r.msgs, msg)
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		From:    r.id,
		To:      to,
		Term:    r.Term,
	}
	r.msgs = append(r.msgs, msg)

	return
}

func (r *Raft)sendRequestVote(to uint64){
	//My Code Here (2A)
	lastindex:=r.RaftLog.LastIndex()
	lastterm:=r.RaftLog.LastTerm()
	msg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVote,
		From:    r.id,
		To:      to,
		Term:	r.Term,
		LogTerm:    lastterm,
		Index: lastindex,

	}
	r.msgs = append(r.msgs, msg)

	return
}

func (r *Raft) sendRequestVoteResponse(to uint64, reject bool) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Reject:  reject,
	}
	r.msgs = append(r.msgs, msg)
	return
}


// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	//point everykinds of node
	//main aim at timeout handle
	if r.State==StateLeader {
		r.heartbeatElapsed++
		if r.heartbeatElapsed>=r.heartbeatTimeout{
			//timeout send local msg(msghup) and new a election
			r.heartbeatElapsed=0
			r.Step(pb.Message{
				MsgType: pb.MessageType_MsgBeat,
				From: r.id,

			})
		}
	}else if(r.State==StateCandidate||r.State==StateFollower){ //point follower and candidate
		r.electionElapsed++
		if r.electionElapsed >= r.randomelectionTimeout {
			r.electionElapsed = 0
			r.Step(pb.Message{
				MsgType: pb.MessageType_MsgHup,
				From:    r.id,

			})
		}
	}

}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.electionElapsed=0
	r.heartbeatElapsed=0
	r.randomelectionTimeout=r.electionTimeout + rand.Intn(r.electionTimeout)
	r.votes=make(map[uint64]bool,0)
	r.Lead=0

	r.State=StateFollower
	if r.Term!=term {
		r.Term=term
		r.Vote=0
	}

}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.electionElapsed=0
	r.heartbeatElapsed=0
	r.randomelectionTimeout=r.electionTimeout + rand.Intn(r.electionTimeout)
	r.votes=make(map[uint64]bool,0)
	r.Lead=0

	r.Term++
	r.State=StateCandidate
	//vote itself
	r.Vote=r.id
	r.votes[r.id]=true

	//if now peer's num <=1 (only this candicate) then become leader
	if len(r.Prs)<=1{
		r.becomeLeader()
	}
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	if r.State!=StateLeader{
		r.electionElapsed=0
		r.heartbeatElapsed=0
		r.randomelectionTimeout=r.electionTimeout + rand.Intn(r.electionTimeout)
		r.votes=make(map[uint64]bool,0)
		r.State=StateLeader
		r.Lead=r.id


		lastindex:=r.RaftLog.LastIndex()

		//propose a nop entry
		entry:=pb.Entry{
			Index: lastindex+1,
			Term: r.Term,
			EntryType: pb.EntryType_EntryNormal,
			Data: nil,
		}

		r.RaftLog.entries=append(r.RaftLog.entries,entry)

		//update nextindex yi match index
		for i:=range r.Prs{
			if i==r.id{
				r.Prs[i]=&Progress{
					Match: lastindex,
					Next: lastindex+1,
				}
			}else{
				r.Prs[i]=&Progress{
					Match: 0,
					Next: lastindex+1,
				}
			}
		}
		//Boardcast msgs to other Followers
		for j:=1;j<=len(r.Prs);j++{
			if uint64(j)!=r.id {
				r.sendAppend(uint64(j))
			}
		}

	}

}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	switch{
	case m.Term==0:
		//local msg, no actio
	case m.Term>r.Term:
		//node id==m recv a msg whose term is greater than its
		if m.MsgType == pb.MessageType_MsgAppend ||
		 m.MsgType == pb.MessageType_MsgHeartbeat ||
		 m.MsgType == pb.MessageType_MsgSnapshot {
			r.becomeFollower(m.Term, m.From)
		} else {
			r.becomeFollower(m.Term, None)
		}
	case m.Term<r.Term:
		//node id==m recv a msg whose term is less than its, ignore
		msg:=pb.Message{
			To: m.From,
			From: r.id,
			Term: r.Term,
		}
		if m.MsgType==pb.MessageType_MsgRequestVote {
			msg.MsgType=pb.MessageType_MsgRequestVoteResponse
			msg.Reject=true
			r.msgs=append(r.msgs,msg)
		}else if m.MsgType==pb.MessageType_MsgHeartbeat{
			msg.MsgType=pb.MessageType_MsgHeartbeatResponse
			r.msgs=append(r.msgs,msg)
		}

		return nil
	}

	//any kinds of node should votes(in response for msgrequestvotes)
	if m.MsgType==pb.MessageType_MsgRequestVote{
		//most important: vote for whom
		//1.now vote is none and now leaderis none (tick)
		//2.now vote == msg's node (tick)
		//3.now term < msg's log term
		//4.now log index <= msg's log index when terms are euqal
		votecond1:=r.Vote==0 && r.Lead==0
		votecond2:=r.Vote==m.From
		updatecond3:=r.RaftLog.LastTerm()<m.LogTerm
		updatecond4:=r.RaftLog.LastTerm()==m.LogTerm && r.RaftLog.LastIndex()<=m.Index
		vote:=(votecond1 || votecond2) && (updatecond3 ||updatecond4)
		if(vote){
			r.Vote=m.From
		}
		r.sendRequestVoteResponse(m.From,!vote)
		return nil
	}

	switch r.State {
	case StateFollower:
		switch m.MsgType{
		case pb.MessageType_MsgHup:
			//timeout, become candicate and boardcast voterequest
			r.becomeCandidate()
			for i:=1;i<=len(r.Prs);i++{
				if uint64(i)!=r.id{
					r.sendRequestVote(uint64(i))
				}
			}
		case pb.MessageType_MsgHeartbeat:
			r.becomeFollower(m.Term, m.From)
		case pb.MessageType_MsgAppend:
			r.becomeFollower(m.Term, m.From)
			r.handleAppendEntries(m)
		}
	case StateCandidate:
		switch m.MsgType{
		case pb.MessageType_MsgHup:
			//timeout, become candicate and boardcast voterequest
			r.becomeCandidate()
			for i:=range r.Prs{
				if i!=r.id{
					r.sendRequestVote(i)
				}
			}
		case pb.MessageType_MsgHeartbeat:
			r.becomeFollower(m.Term, m.From)
		case pb.MessageType_MsgAppend:
			r.becomeFollower(m.Term, m.From)
			r.handleAppendEntries(m)
		case pb.MessageType_MsgRequestVoteResponse:
			//in response for its vote request and handling the vote result
			r.votes[m.From]=!m.Reject
			//for every node count their attitude
			yes:=0
			no:=0
			for i:=1;i<=len(r.Prs);i++ {
				vote, ifvote := r.votes[uint64(i)]
				if ifvote == true {
					if vote == true {
						yes++
					} else {
						no++
					}
				}
			}
			//if Raft only has one node or yes votes more than node's num/2, then be leader
			if(len(r.Prs)==1||yes>len(r.Prs)/2){
				r.becomeLeader()
				for j:=1;j<=len(r.Prs);j++ {
					if(uint64(j)!=r.id){
						r.sendHeartbeat(uint64(j))
					}
				}
			}else if(no>len(r.Prs)/2){
				r.becomeFollower(r.Term,0)
			}
		}
	case StateLeader:
		switch m.MsgType {
		case pb.MessageType_MsgBeat:
			for i := range r.Prs {
				if i != r.id {
					r.sendHeartbeat(i)
				}
			}
		case pb.MessageType_MsgPropose:
			//append entries
			lastindex:=r.RaftLog.LastIndex()
			ents:=make([]pb.Entry,0)
			for i:=range m.Entries{
				m.Entries[i].Index=lastindex+1+uint64(i)
				m.Entries[i].Term=r.Term
				ents=append(ents,*m.Entries[i])
			}
			r.RaftLog.appendEntries(ents...)
			r.Prs[r.id].Match=r.RaftLog.LastIndex()
			r.Prs[r.id].Next=r.Prs[r.id].Match+1

			for j:=range r.Prs{
				if j != r.id {
					r.sendAppend(uint64(j))
				}
			}
		}
	}
	return nil
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	//Your choosen leader send msgs to you, you become leader's follower
	if r.Lead==0 && r.Term==m.Term && r.Vote==m.From{
		r.Lead=m.From
	}


	newindex:=m.Index+uint64(len(m.Entries))
	lastindex:=m.Index
	lastterm:=m.LogTerm

	msg:=pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		From: r.id,
		To: m.From,
		Term: r.Term,
		Index: m.Index,
		Reject: false,

	}
	tempterm,_:=r.RaftLog.Term(lastindex)
	if lastterm!=tempterm{
		msg.Reject=true
		r.msgs=append(r.msgs,msg)
		return
	}
	entries:=make([]pb.Entry,0,len(m.Entries))
	ifconf:=false
	for _, ent := range m.Entries {
		term, err := r.RaftLog.Term(ent.Index)
		if err != nil {
			panic(err)
		}
		if term != ent.Term {
			ifconf = true
		}
		if ifconf {
			entries = append(entries, *ent)
		}
	}

	r.RaftLog.appendEntries(entries ...)

	msg.Index = r.RaftLog.LastIndex()
	if r.RaftLog.committed < m.Commit {
		r.RaftLog.committed = min(newindex, m.Commit)
	}
	r.msgs=append(r.msgs,msg)
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}
