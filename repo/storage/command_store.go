package storage

import (
	"sync"

	"github.com/named-data/ndnd/repo/tlv"
	"github.com/named-data/ndnd/repo/types"
)

// TODO: to fully support timestamps and range-based queries we need more advanced data structures such as time-based skip list.
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

	return s.getAllCommandsUnsafe()
}

// GetAllCommands returns all commands in the store in insertion order
// Thread-safe and returns a copy of the commands
func (s *CommandStore) GetAllCommands() []*tlv.RepoCommand {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	return s.getAllCommandsUnsafe()
}

// getAllCommandsUnsafe returns all commands in insertion order
// This method copies the entire history again
// NOT thread-safe and should only be called while holding the mutex
func (s *CommandStore) getAllCommandsUnsafe() []*tlv.RepoCommand {
	commands := make([]*tlv.RepoCommand, 0, s.commands.Size())

	current := s.commands.Begin()
	end := s.commands.End()

	for current != end {
		commands = append(commands, current.Value())
		current = current.Next()
	}

	return commands
}

func (s *CommandStore) Begin() *types.DoubleLinkedListNode[*tlv.RepoCommand] {
	return s.commands.Begin()
}

func (s *CommandStore) End() *types.DoubleLinkedListNode[*tlv.RepoCommand] {
	return s.commands.End()
}

func (s *CommandStore) SetOnInsertion(onInsertion func(command *tlv.RepoCommand)) {
	s.onInsertion = onInsertion
}

// Iterator provides a thread-safe way to iterate over commands in insertion order.
// The iterator becomes invalid if the underlying store is modified.
type CommandIterator struct {
	store   *CommandStore
	current *types.DoubleLinkedListNode[*tlv.RepoCommand]
	end     *types.DoubleLinkedListNode[*tlv.RepoCommand]
}

// Next advances the iterator to the next command.
// Returns false if there are no more commands.
func (it *CommandIterator) Next() bool {
	if it.current == it.end {
		return false
	}
	it.current = it.current.Next()
	return it.current != it.end
}

// Value returns the current command.
// Panics if called when the iterator is at the end.
func (it *CommandIterator) Value() *tlv.RepoCommand {
	return it.current.Value()
}

// IsValid returns true if the iterator is at a valid position.
func (it *CommandIterator) IsValid() bool {
	return it.current != it.end
}

// Iterator returns a thread-safe iterator for iterating over all commands in insertion order.
// The iterator becomes invalid if the underlying store is modified.
func (s *CommandStore) Iterator() *CommandIterator {
	s.mutex.RLock()
	// Note: We don't unlock here because the iterator needs to maintain the lock
	// The caller is responsible for calling Close() when done

	return &CommandIterator{
		store:   s,
		current: s.commands.Begin(),
		end:     s.commands.Back(),
	}
}

// Close releases the read lock held by the iterator.
// This method must be called when done with the iterator.
func (it *CommandIterator) Close() {
	it.store.mutex.RUnlock()
}
