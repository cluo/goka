package storage

type multiIterator struct {
	current int
	iters   []Iterator
}

// NewMultiIterator returns an iterator that iterates over the given iterators.
func NewMultiIterator(iters []Iterator) Iterator {
	if len(iters) == 0 {
		return &NullIter{}
	}

	return &multiIterator{current: 0, iters: iters}
}

func (m *multiIterator) Next() bool {
	next := m.iters[m.current].Next()
	if !next && len(m.iters)-1 > m.current {
		m.current++
		next = m.iters[m.current].Next()
	}

	return next
}

func (m *multiIterator) Key() []byte {
	return m.iters[m.current].Key()
}

func (m *multiIterator) Value() (interface{}, error) {
	return m.iters[m.current].Value()
}

func (m *multiIterator) Release() {
	for i := range m.iters {
		m.iters[i].Release()
	}
	m.current = 0
	m.iters = []Iterator{&NullIter{}}
}
