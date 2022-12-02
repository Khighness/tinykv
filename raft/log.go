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
	"github.com/pingcap-incubator/tinykv/log"

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
	firstIndex uint64
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	firstIndex, _ := storage.FirstIndex()
	lastIndex, _ := storage.LastIndex()
	entries, _ := storage.Entries(firstIndex, lastIndex+1)
	raftLog := &RaftLog{
		storage:    storage,
		committed:  0,
		applied:    firstIndex - 1,
		stabled:    lastIndex,
		entries:    entries,
		firstIndex: firstIndex,
	}
	return raftLog
}

// firstTo advances first index.0
func (l *RaftLog) firstTo(firstIndex uint64) {
	if firstIndex < l.firstIndex {
		log.Panicf("RaftLog|firstTo: newFirstIndex(%d) < firstIndex(%d)", firstIndex, l.firstIndex)
	}
	l.firstIndex = firstIndex
}

// stableTo advances stable index.
func (l *RaftLog) stableTo(stableIndex uint64) {
	if stableIndex > l.stabled {
		lastIndex := l.LastIndex()
		if stableIndex > lastIndex {
			log.Panicf("RaftLog|stableTo: stableIndex(%d) > lastIndex(%d)", stableIndex, lastIndex)
		}
		l.stabled = stableIndex
	}
}

// commitTo advances committed index.
func (l *RaftLog) commitTo(commitIndex uint64) {
	if commitIndex > l.committed {
		lastIndex := l.LastIndex()
		if commitIndex > lastIndex {
			log.Panicf("RaftLog|commitTo: commitIndex(%d) > lastIndex(%d)", commitIndex, lastIndex)
		}
		l.committed = commitIndex
	}
}

// applyTo advances applied index.
func (l *RaftLog) applyTo(applyIndex uint64) {
	if applyIndex == 0 {
		return
	}
	if applyIndex < l.applied || applyIndex > l.committed {
		log.Panicf("RaftLog|applyTo: applyIndex(%d) is out of range [applied(%d), committed(%d)]",
			applyIndex, l.applied, l.committed)
	}
	l.applied = applyIndex
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries.
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
	firstIndex, _ := l.storage.FirstIndex()
	// If older entries in storage module has been compacted into snapshot,
	// then `storage.FirstIndex()` must be bigger than `l.FirstIndex`.
	if firstIndex > l.firstIndex {
		if len(l.entries) > 0 {
			// Delete entries that have been compressed into snapshots but still exist in memory.
			entries := l.entries[l.toSliceIndex(firstIndex):]
			l.entries = make([]pb.Entry, len(entries))
			copy(l.entries, entries)
		}
		l.firstIndex = firstIndex
	}
}

// allEntries return all the entries not compacted.
// note, exclude any dummy entries from the return value.
// note, this is one of the test stub functions you need to implement.
func (l *RaftLog) allEntries() []pb.Entry {
	// Your Code Here (2A).
	return l.entries[1:]
}

// unstableEntries return all the unstable entries.
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	if len(l.entries) > 0 {
		return l.entries[l.stabled-l.firstIndex+1:]
	}
	return nil
}

// nextEnts returns all the committed but not applied entries.
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	if len(l.entries) > 0 {
		return l.entries[l.applied-l.firstIndex+1 : l.committed-l.firstIndex+1]
	}
	return nil
}

// FirstIndex returns the first index of the log entries.
func (l *RaftLog) FirstIndex() uint64 {
	return l.firstIndex
}

// LastIndex return the last index of the log entries.
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	// Get last index in memory.
	var lastIndexInMemory uint64
	if len(l.entries) > 0 {
		lastIndexInMemory = l.entries[len(l.entries)-1].Index
	} else {
		lastIndexInMemory, _ = l.storage.LastIndex()
	}
	// Get last index in snapshot.
	var lastIndexInSnapshot uint64
	if !IsEmptySnap(l.pendingSnapshot) {
		lastIndexInSnapshot = l.pendingSnapshot.Metadata.Index
	}
	return max(lastIndexInMemory, lastIndexInSnapshot)
}

// Term return the term of the entry in the given index.
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	// If the entry in the given index is in memory.
	if len(l.entries) > 0 && i >= l.firstIndex {
		return l.entries[i-l.firstIndex].Term, nil
	}
	// If the entry in the given index is in snapshot.
	term, err := l.storage.Term(i)
	if err == ErrUnavailable && !IsEmptySnap(l.pendingSnapshot) {
		if i == l.pendingSnapshot.Metadata.Index {
			term = l.pendingSnapshot.Metadata.Term
			err = nil
		} else if i < l.pendingSnapshot.Metadata.Index {
			err = ErrCompacted
		}
	}
	return term, err
}

// toSliceIndex return the log offset in corresponding to the log index.
func (l *RaftLog) toSliceIndex(logIndex uint64) int {
	idx := int(logIndex - l.firstIndex)
	if idx < 0 {
		log.Panicf("RaftLog|toSliceIndex: logIndex(%d) < firstIndex(%d)", logIndex, l.firstIndex)
	}
	return idx
}

// toEntryIndex returns the index of log entry corresponding to the offset.
func (l *RaftLog) toEntryIndex(offset int) uint64 {
	return uint64(offset) + l.firstIndex
}
