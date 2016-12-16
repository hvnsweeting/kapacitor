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
func (c *IndexedStore) dataKey(id string) string {
	return path.Join(c.dataPrefix, id)
}

// Create a key for a given index and value.
//
// Indexes are maintained via a 'directory' like system:
//
// /<dataPrefix>/<ID> -- contains encoded object data
// /<indexesPrefix>/id/<value> -- contains the object ID
//
// As such to list all handlers in ID sorted order use the /<indexesPrefix>/id/ directory.
func (c *IndexedStore) indexKey(index, value string) string {
	return path.Join(c.indexesPrefix, index, value) + "/"
}

func (c *IndexedStore) Get(id string) (BinaryObject, error) {
	key := c.dataKey(id)
	if exists, err := c.store.Exists(key); err != nil {
		return nil, err
	} else if !exists {
		return nil, ErrNoObjectExists
	}
	kv, err := c.store.Get(key)
	if err != nil {
		return nil, err
	}
	o := c.newObject()
	err = o.UnmarshalBinary(kv.Value)
	return o, err
}

func (c *IndexedStore) Create(o BinaryObject) error {
	return c.put(o, false, false)
}

func (c *IndexedStore) Put(o BinaryObject) error {
	return c.put(o, true, false)
}

func (c *IndexedStore) Replace(o BinaryObject) error {
	return c.put(o, true, true)
}

func (c *IndexedStore) put(o BinaryObject, allowReplace, requireReplace bool) error {
	key := c.dataKey(o.ObjectID())

	replacing := false
	old, err := c.Get(o.ObjectID())
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
	err = c.store.Put(key, data)
	if err != nil {
		return err
	}
	// Put all indexes
	for _, idx := range c.indexes {
		// TODO implement rollback

		// Get new index key
		newValue, err := idx.ValueF(o)
		if err != nil {
			return err
		}
		newIndexKey := c.indexKey(idx.Name, newValue)

		// Get old index key, if we are replacing
		var oldValue string
		if replacing {
			var err error
			oldValue, err = idx.ValueF(old)
			if err != nil {
				return err
			}
		}
		oldIndexKey := c.indexKey(idx.Name, oldValue)

		if !replacing || (replacing && oldIndexKey != newIndexKey) {
			// Update new key
			err := c.store.Put(newIndexKey, []byte(o.ObjectID()))
			if err != nil {
				return err
			}
			if replacing {
				// Remove old key
				err = c.store.Delete(oldIndexKey)
				if err != nil {
					return err
				}
			}
		}
	}
	return nil

}

func (c *IndexedStore) Delete(id string) error {
	o, err := c.Get(id)
	if err == ErrNoObjectExists {
		// Nothing to do
		return nil
	} else if err != nil {
		return err
	}

	// Keep track of first error
	var firstError error

	// Delete object
	key := c.dataKey(id)
	err = c.store.Delete(key)
	if err != nil {
		firstError = err
	}

	// Delete all indexes
	for _, idx := range c.indexes {
		value, err := idx.ValueF(o)
		if err != nil && firstError == nil {
			firstError = err
		}
		indexKey := c.indexKey(idx.Name, value)
		err = c.store.Delete(indexKey)
		if err != nil && firstError == nil {
			firstError = err
		}
	}
	return firstError
}

func (c *IndexedStore) List(index, pattern string, offset, limit int) ([]BinaryObject, error) {
	// List all object ids sorted by index
	ids, err := c.store.List(c.indexKey(index, ""))
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
		data, err := c.store.Get(c.dataKey(string(id)))
		if err != nil {
			return nil, err
		}
		o := c.newObject()
		err = o.UnmarshalBinary(data.Value)
		if err != nil {
			return nil, err
		}
		objects[i] = o
	}
	return objects, nil
}
