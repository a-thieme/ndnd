package storage

import (
	"sync"

	"github.com/named-data/ndnd/repo/tlv"
	"github.com/named-data/ndnd/repo/types"
)

// TODO: to fully support timestamps, range-based queries we need more advanced data structures such as skip list.
// For now, this is just a wrapper of RandomAccessDoubleLinkedList which allows for efficient access and insertion/removal.
// Thread-safe
type CommandStore struct {
	mutex    sync.RWMutex
	commands *types.RandomAccessDoubleLinkedList[string, *tlv.RepoCommand]

	onInsertion func(command *tlv.RepoCommand)
}

func NewCommandStore() *CommandStore {
	return &CommandStore{
		commands: types.NewRandomAccessDoubleLinkedList[string, *tlv.RepoCommand](),
	}
}

func (s *CommandStore) Insert(command *tlv.RepoCommand) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	key := command.SrcName.Name.String()
	if s.commands.Contains(key) {
		// ignore duplicate commands
		return
	}

	s.commands.PushBack(key, command)
	if s.onInsertion != nil {
		s.onInsertion(command)
	}
}

func (s *CommandStore) Remove(key string) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.commands.Remove(key)
}

func (s *CommandStore) Contains(key string) bool {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	return s.commands.Contains(key)
}

func (s *CommandStore) Get(key string) (*tlv.RepoCommand, bool) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	return s.commands.Get(key)
}

func (s *CommandStore) Snap() []*tlv.RepoCommand {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	// return s.commands.GetAll()
	return nil // TODO: implement
}

func (s *CommandStore) Begin() *types.DoubleLinkedListNode[*tlv.RepoCommand] {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	return s.commands.Begin()
}

func (s *CommandStore) End() *types.DoubleLinkedListNode[*tlv.RepoCommand] {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	return s.commands.End()
}

func (s *CommandStore) SetOnInsertion(onInsertion func(command *tlv.RepoCommand)) {
	s.onInsertion = onInsertion
}
