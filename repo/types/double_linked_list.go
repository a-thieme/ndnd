package types

type DoubleLinkedListNode[V any] struct {
	prev, next *DoubleLinkedListNode[V]
	value      V
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

func (l *DoubleLinkedList[V]) Head() *DoubleLinkedListNode[V] {
	return l.sentinel.next
}

func (l *DoubleLinkedList[V]) Back() *DoubleLinkedListNode[V] {
	return l.sentinel.prev
}

func (l *DoubleLinkedList[V]) Size() int {
	return l.size
}
