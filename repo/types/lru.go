package awareness

type LRU[K comparable, V any] struct {
	hash    map[K]*DoubleLinkedListNode[V]
	storage *DoubleLinkedList[V]
}

func NewLRU[K comparable, V any]() *LRU[K, V] {
	return &LRU[K, V]{
		hash:    make(map[K]*DoubleLinkedListNode[V]),
		storage: NewDoubleLinkedList[V](),
	}
}

// Put adds or updates the value associated with the given key.
// If the key already exists, it updates the value and moves it to the back of the linked list.
// If move is true, it moves the node to the back of the linked list
func (l *LRU[K, V]) Put(key K, value V, move bool) {
	if _, ok := l.hash[key]; !ok {
		node := l.storage.PushBack(value)
		l.hash[key] = node
	} else {
		node := l.hash[key]
		node.value = value
		if move {
			l.storage.MoveBack(node)
		}
	}
}

// Get retrieves the value associated with the given key.
// If move is true, it moves the node to the back of the linked list.
func (l *LRU[K, V]) Get(key K, move bool) *V {
	if _, ok := l.hash[key]; !ok {
		return nil
	} else {
		node := l.hash[key]
		if move {
			l.storage.MoveBack(node)
		}
		return &node.value
	}
}

// Remove removes the value associated with the given key.
// If the key does not exist, it does nothing.
func (l *LRU[K, V]) Remove(key K) {
	if _, ok := l.hash[key]; !ok {
		return
	} else {
		node := l.hash[key]
		l.storage.Remove(node)
		delete(l.hash, key)
	}
}

func (l *LRU[K, V]) Contains(key K) bool {
	_, ok := l.hash[key]
	return ok
}

func (l *LRU[K, V]) Size() int {
	return l.storage.size
}
