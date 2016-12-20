package storage

import "errors"

// Common errors that can be returned
var (
	ErrNoKeyExists = errors.New("no key exists")
)

// ReadOperator provides an interface for performing read operations.
type ReadOperator interface {
	// Retrieve a value.
	Get(key string) (*KeyValue, error)
	// Check if a key exists>
	Exists(key string) (bool, error)
	// List all values with given prefix.
	List(prefix string) ([]*KeyValue, error)
}

// WriteOperator provides an interface for performing write operations.
type WriteOperator interface {
	// Store a value.
	Put(key string, value []byte) error
	// Delete a key.
	// Deleting a non-existent key is not an error.
	Delete(key string) error
}

// ReadOnlyTx provides an interface for performing read operations in a single transaction.
type ReadOnlyTx interface {
	ReadOperator
	// Rollback signals that the transaction is complete.
	Rollback() error
}

// Tx provides an interface for performing read and write storage operations in a single transaction.
type Tx interface {
	ReadOnlyTx
	WriteOperator

	// Commit finalizes the transaction.
	Commit() error
}

// Common interface for interacting with a simple Key/Value storage
type Interface interface {
	ReadOperator
	WriteOperator

	// BeginReadOnlyTx starts a new read only transaction. The transaction must be rolledback.
	// Leaving a transaction open can block other operations and otherwise
	// significantly degrade the performance of the storage backend.
	// A single go routine should only have one transaction open at a time.
	BeginReadOnlyTx() (ReadOnlyTx, error)
	// BeginTx starts a new transaction for reads and writes. The transaction must be committed or rolledback.
	// Leaving a transaction open can block other operations and otherwise
	// significantly degrade the performance of the storage backend.
	// A single go routine should only have one transaction open at a time.
	BeginTx() (Tx, error)
}

// View manages a read only transaction.
func View(s Interface, f func(ReadOnlyTx) error) error {
	tx, err := s.BeginReadOnlyTx()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	return f(tx)
}

// Update manages a read/write transaction.
func Update(s Interface, f func(Tx) error) error {
	tx, err := s.BeginTx()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	err = f(tx)
	if err != nil {
		return err
	}
	return tx.Commit()
}

type KeyValue struct {
	Key   string
	Value []byte
}

// Return a list of values from a list of KeyValues using an offset/limit bound and a match function.
func DoListFunc(list []*KeyValue, match func(value []byte) bool, offset, limit int) [][]byte {
	l := len(list)
	upper := offset + limit
	if upper > l {
		upper = l
	}
	size := upper - offset
	if size <= 0 {
		// No more results
		return nil
	}
	matches := make([][]byte, 0, size)
	i := 0
	for _, kv := range list {
		if !match(kv.Value) {
			continue
		}
		// Count matched
		i++

		// Skip till offset
		if i <= offset {
			continue
		}

		matches = append(matches, kv.Value)

		// Stop once limit reached
		if len(matches) == size {
			break
		}
	}
	return matches
}
