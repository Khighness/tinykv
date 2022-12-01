package raft

import (
	"testing"

	"github.com/stretchr/testify/assert"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

func generateEntries(term uint64, from uint64, to uint64) []pb.Entry {
	if to < from {
		panic("to < from")
	}
	entries := make([]pb.Entry, (to-from)+1)
	for i := 0; i <= int(to-from); i++ {
		entries[i] = pb.Entry{
			EntryType: pb.EntryType_EntryNormal,
			Term:      term,
			Index:     from + uint64(i),
		}
	}
	return entries
}

func printEntries(ents []pb.Entry, t *testing.T) {
	t.Log("===> entries")
	for idx, ent := range ents {
		t.Logf("Idx: %d, Entry(Term=%d, Index=%d, Data=%s)", idx, ent.Term, ent.Index, string(ent.Data))
	}
	t.Logf("Meta: FirstIndex=%d, LastIndex=%d, Len=%d", ents[0].Index, ents[len(ents)-1].Index, len(ents))
	t.Log("<=== entries")
}

func TestMemoryStorage_Compact(t *testing.T) {
	a := assert.New(t)

	storage := NewMemoryStorage()
	a.Equal(uint64(1), storage.firstIndex())
	printEntries(storage.ents, t)

	// append new entries
	entries := generateEntries(0, 1, 4)
	err := storage.Append(entries)
	if err != nil {
		t.Error(err)
	}
	a.Equal(uint64(4), storage.lastIndex())
	printEntries(storage.ents, t)

	// compact entries to index 2
	err = storage.Compact(2)
	if err != nil {
		t.Error(err)
	}
	a.Equal(uint64(3), storage.firstIndex())
	a.Equal(uint64(4), storage.lastIndex())
	printEntries(storage.ents, t)

	// append entries which contains some existed entries
	entries = generateEntries(2, 1, 5)
	err = storage.Append(entries)
	if err != nil {
		t.Error(err)
	}
	a.Equal(uint64(3), storage.firstIndex())
	a.Equal(uint64(5), storage.lastIndex())
	printEntries(storage.ents, t)
}
