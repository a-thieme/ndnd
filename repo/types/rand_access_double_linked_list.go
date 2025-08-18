package types

type RandomAccessDoubleLinkedList[K comparable, V any] struct {
	hash             map[K]*DoubleLinkedListNode[V]
	doubleLinkedList *DoubleLinkedList[V]
}

func NewRandomAccessDoubleLinkedList[K comparable, V any]() *RandomAccessDoubleLinkedList[K, V] {
	return &RandomAccessDoubleLinkedList[K, V]{
		hash:             make(map[K]*DoubleLinkedListNode[V]),
		doubleLinkedList: NewDoubleLinkedList[V](),
	}
}

func (l *RandomAccessDoubleLinkedList[K, V]) PushBack(key K, value V) {
	node := l.doubleLinkedList.PushBack(value)
	l.hash[key] = node
}

func (l *RandomAccessDoubleLinkedList[K, V]) PushFront(key K, value V) {
	node := l.doubleLinkedList.PushFront(value)
	l.hash[key] = node
}

func (l *RandomAccessDoubleLinkedList[K, V]) Remove(key K) {
	node := l.hash[key]
	if node == nil {
		return
	}
	l.doubleLinkedList.Remove(node)
	delete(l.hash, key)
}

func (l *RandomAccessDoubleLinkedList[K, V]) Get(key K) (V, bool) {
	node := l.hash[key]
	if node == nil {
		var zero V
		return zero, false
	}

	return node.value, true
}

func (l *RandomAccessDoubleLinkedList[K, V]) Contains(key K) bool {
	return l.hash[key] != nil
}

func (l *RandomAccessDoubleLinkedList[K, V]) Size() int {
	return l.doubleLinkedList.Size()
}

func (l *RandomAccessDoubleLinkedList[K, V]) Begin() *DoubleLinkedListNode[V] {
	return l.doubleLinkedList.Begin()
}

// This returns the last element in the linked list
func (l *RandomAccessDoubleLinkedList[K, V]) Back() *DoubleLinkedListNode[V] {
	return l.doubleLinkedList.Back()
}

// This returns the element after the last element in the linked list, checking != End() gives the whole content of the linked list
// Note: this is different from Back()
func (l *RandomAccessDoubleLinkedList[K, V]) End() *DoubleLinkedListNode[V] {
	return l.doubleLinkedList.End()
}
