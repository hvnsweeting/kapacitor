package storage

import (
	"encoding"
	"errors"
	"fmt"
	"path"
	"strings"
)

const (
	defaultDataPrefix    = "data"
	defaultIndexesPrefix = "indexes"

	DefaultIDIndex = "id"
)

var (
	ErrObjectExists   = errors.New("object already exists")
	ErrNoObjectExists = errors.New("no object exists")
)

type BinaryObject interface {
	encoding.BinaryMarshaler
	encoding.BinaryUnmarshaler
	ObjectID() string
}

type NewObjectF func() BinaryObject
type ValueF func(BinaryObject) (string, error)

type Index struct {
	Name   string
	ValueF ValueF
}

// Indexed provides basic CRUD operations and maintains indexes.
type IndexedStore struct {
	store Interface

	dataPrefix    string
	indexesPrefix string

	indexes []Index

	newObject NewObjectF
}

type IndexedStoreConfig struct {
	DataPrefix    string
	IndexesPrefix string
	NewObject     NewObjectF
	Indexes       []Index
}

func DefaultIndexedStoreConfig(newObject NewObjectF) IndexedStoreConfig {
	return IndexedStoreConfig{
		DataPrefix:    defaultDataPrefix,
		IndexesPrefix: defaultIndexesPrefix,
		NewObject:     newObject,
		Indexes: []Index{{
			Name: DefaultIDIndex,
			ValueF: func(o BinaryObject) (string, error) {
				return o.ObjectID(), nil
			},
		}},
	}
}

func validPath(p string) bool {
	return !strings.Contains(p, "/")
}

func (c IndexedStoreConfig) Validate() error {
	if !validPath(c.DataPrefix) {
		return fmt.Errorf("invalid data prefix %q", c.DataPrefix)
	}
	if !validPath(c.IndexesPrefix) {
		return fmt.Errorf("invalid indexes prefix %q", c.IndexesPrefix)
	}
	if c.NewObject == nil {
		return errors.New("must provide a NewObject function")
	}
	for _, idx := range c.Indexes {
		if !validPath(idx.Name) {
			return fmt.Errorf("invalid index name %q", idx.Name)
		}
		if idx.ValueF == nil {
			return fmt.Errorf("index %q does not have a ValueF function", idx.Name)
		}
	}
	return nil
}

func NewIndexedStore(store Interface, c IndexedStoreConfig) (*IndexedStore, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	return &IndexedStore{
		store:         store,
		dataPrefix:    c.DataPrefix,
		indexesPrefix: c.IndexesPrefix,
		indexes:       c.Indexes,
		newObject:     c.NewObject,
	}, nil
}

// Create a key for the object data
func (s *IndexedStore) dataKey(id string) string {
	return path.Join(s.dataPrefix, id)
}

// Create a key for a given index and value.
//
// Indexes are maintained via a 'directory' like system:
//
// /<dataPrefix>/<ID> -- contains encoded object data
// /<indexesPrefix>/id/<value> -- contains the object ID
//
// As such to list all handlers in ID sorted order use the /<indexesPrefix>/id/ directory.
func (s *IndexedStore) indexKey(index, value string) string {
	return path.Join(s.indexesPrefix, index, value) + "/"
}

func (s *IndexedStore) get(tx ReadOnlyTx, id string) (BinaryObject, error) {
	key := s.dataKey(id)
	if exists, err := tx.Exists(key); err != nil {
		return nil, err
	} else if !exists {
		return nil, ErrNoObjectExists
	}
	kv, err := tx.Get(key)
	if err != nil {
		return nil, err
	}
	o := s.newObject()
	err = o.UnmarshalBinary(kv.Value)
	return o, err

}

func (s *IndexedStore) Get(id string) (BinaryObject, error) {
	tx, err := s.store.BeginReadOnlyTx()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	return s.get(tx, id)
}

func (s *IndexedStore) Create(o BinaryObject) error {
	return s.put(o, false, false)
}

func (s *IndexedStore) Put(o BinaryObject) error {
	return s.put(o, true, false)
}

func (s *IndexedStore) Replace(o BinaryObject) error {
	return s.put(o, true, true)
}

func (s *IndexedStore) put(o BinaryObject, allowReplace, requireReplace bool) error {
	tx, err := s.store.BeginTx()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	key := s.dataKey(o.ObjectID())

	replacing := false
	old, err := s.get(tx, o.ObjectID())
	if err != nil {
		if err != ErrNoObjectExists || (requireReplace && err == ErrNoObjectExists) {
			return err
		}
	} else if !allowReplace {
		return ErrObjectExists
	} else {
		replacing = true
	}

	data, err := o.MarshalBinary()
	if err != nil {
		return err
	}

	// Put data
	err = tx.Put(key, data)
	if err != nil {
		return err
	}
	// Put all indexes
	for _, idx := range s.indexes {
		// Get new index key
		newValue, err := idx.ValueF(o)
		if err != nil {
			return err
		}
		newIndexKey := s.indexKey(idx.Name, newValue)

		// Get old index key, if we are replacing
		var oldValue string
		if replacing {
			var err error
			oldValue, err = idx.ValueF(old)
			if err != nil {
				return err
			}
		}
		oldIndexKey := s.indexKey(idx.Name, oldValue)

		if !replacing || (replacing && oldIndexKey != newIndexKey) {
			// Update new key
			err := tx.Put(newIndexKey, []byte(o.ObjectID()))
			if err != nil {
				return err
			}
			if replacing {
				// Remove old key
				err = tx.Delete(oldIndexKey)
				if err != nil {
					return err
				}
			}
		}
	}
	return tx.Commit()
}

func (s *IndexedStore) Delete(id string) error {
	tx, err := s.store.BeginTx()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	o, err := s.get(tx, id)
	if err == ErrNoObjectExists {
		// Nothing to do
		return nil
	} else if err != nil {
		return err
	}

	// Delete object
	key := s.dataKey(id)
	err = tx.Delete(key)
	if err != nil {
		return err
	}

	// Delete all indexes
	for _, idx := range s.indexes {
		value, err := idx.ValueF(o)
		if err != nil {
			return err
		}
		indexKey := s.indexKey(idx.Name, value)
		err = tx.Delete(indexKey)
		if err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *IndexedStore) List(index, pattern string, offset, limit int) ([]BinaryObject, error) {
	tx, err := s.store.BeginReadOnlyTx()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	// List all object ids sorted by index
	ids, err := tx.List(s.indexKey(index, ""))
	if err != nil {
		return nil, err
	}

	var match func([]byte) bool
	if pattern != "" {
		match = func(value []byte) bool {
			id := string(value)
			matched, _ := path.Match(pattern, id)
			return matched
		}
	} else {
		match = func([]byte) bool { return true }
	}
	matches := DoListFunc(ids, match, offset, limit)

	objects := make([]BinaryObject, len(matches))
	for i, id := range matches {
		data, err := tx.Get(s.dataKey(string(id)))
		if err != nil {
			return nil, err
		}
		o := s.newObject()
		err = o.UnmarshalBinary(data.Value)
		if err != nil {
			return nil, err
		}
		objects[i] = o
	}
	return objects, nil
}
