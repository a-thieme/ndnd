package types

type DoubleLinkedListNode[V any] struct {
	prev, next *DoubleLinkedListNode[V]
	value      V
}

func (n *DoubleLinkedListNode[V]) Next() *DoubleLinkedListNode[V] {
	return n.next
}

func (n *DoubleLinkedListNode[V]) Prev() *DoubleLinkedListNode[V] {
	return n.prev
}

func (n *DoubleLinkedListNode[V]) Value() V {
	return n.value
}

type DoubleLinkedList[V any] struct {
	sentinel *DoubleLinkedListNode[V]
	size     int
}

func NewDoubleLinkedList[V any]() *DoubleLinkedList[V] {
	sentinel := &DoubleLinkedListNode[V]{}
	sentinel.prev = sentinel
	sentinel.next = sentinel
	return &DoubleLinkedList[V]{
		sentinel: sentinel,
		size:     0,
	}
}

func (l *DoubleLinkedList[V]) PushFront(value V) *DoubleLinkedListNode[V] {
	node := &DoubleLinkedListNode[V]{value: value}
	node.prev = l.sentinel
	node.next = l.sentinel.next
	l.sentinel.next.prev = node
	l.sentinel.next = node
	l.size++
	return node
}

func (l *DoubleLinkedList[V]) PushBack(value V) *DoubleLinkedListNode[V] {
	node := &DoubleLinkedListNode[V]{value: value}
	node.next = l.sentinel
	node.prev = l.sentinel.prev
	l.sentinel.prev.next = node
	l.sentinel.prev = node
	l.size++
	return node
}

func (l *DoubleLinkedList[V]) Remove(node *DoubleLinkedListNode[V]) {
	if node == l.sentinel {
		panic("Can not remove the dummy node")
	}

	node.prev.next = node.next
	node.next.prev = node.prev
	l.size--
}

func (l *DoubleLinkedList[V]) MoveFront(node *DoubleLinkedListNode[V]) *DoubleLinkedListNode[V] {
	if node == l.sentinel {
		panic("Can not remove the dummy node")
	}

	node.prev.next = node.next
	node.next.prev = node.prev

	node.prev = l.sentinel
	node.next = l.sentinel.next
	l.sentinel.next.prev = node
	l.sentinel.next = node
	return node
}

func (l *DoubleLinkedList[V]) MoveBack(node *DoubleLinkedListNode[V]) *DoubleLinkedListNode[V] {
	if node == l.sentinel {
		panic("Can not remove the dummy node")
	}

	node.prev.next = node.next
	node.next.prev = node.prev

	node.next = l.sentinel
	node.prev = l.sentinel.prev
	l.sentinel.prev.next = node
	l.sentinel.prev = node
	return node
}

func (l *DoubleLinkedList[V]) Size() int {
	return l.size
}

func (l *DoubleLinkedList[V]) Head() *DoubleLinkedListNode[V] {
	if l.size == 0 {
		return nil
	}

	return l.sentinel.next
}

func (l *DoubleLinkedList[V]) Back() *DoubleLinkedListNode[V] {
	if l.size == 0 {
		return nil
	}

	return l.sentinel.prev
}

func (l *DoubleLinkedList[V]) Begin() *DoubleLinkedListNode[V] {
	return l.Head()
}

func (l *DoubleLinkedList[V]) End() *DoubleLinkedListNode[V] {
	return l.sentinel
}

// Iterator represents a position in the list
type Iterator[V any] struct {
	current *DoubleLinkedListNode[V]
	list    *DoubleLinkedList[V]
}

// NewIterator creates a new iterator starting from the head
func (l *DoubleLinkedList[V]) NewIterator() *Iterator[V] {
	return &Iterator[V]{
		current: l.sentinel.next,
		list:    l,
	}
}

// Next advances the iterator to the next element
func (it *Iterator[V]) Next() bool {
	if it.current == it.list.sentinel {
		return false
	}
	it.current = it.current.next
	return it.current != it.list.sentinel
}

// Value returns the current value
func (it *Iterator[V]) Value() V {
	return it.current.value
}

// Node returns the current node
func (it *Iterator[V]) Node() *DoubleLinkedListNode[V] {
	return it.current
}

// IsValid returns true if the iterator is at a valid position
func (it *Iterator[V]) IsValid() bool {
	return it.current != it.list.sentinel
}
