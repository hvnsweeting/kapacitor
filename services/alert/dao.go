package alert

import (
	"encoding/json"
	"errors"
	"path"

	"github.com/influxdata/kapacitor/services/storage"
)

var (
	ErrHandlerSpecExists   = errors.New("handler spec already exists")
	ErrNoHandlerSpecExists = errors.New("no handler spec exists")
)

// Data access object for HandlerSpec data.
type HandlerSpecDAO interface {
	// Retrieve a handler
	Get(id string) (HandlerSpec, error)

	// Create a handler.
	// ErrHandlerSpecExists is returned if a handler already exists with the same ID.
	Create(h HandlerSpec) error

	// Replace an existing handler.
	// ErrNoHandlerSpecExists is returned if the handler does not exist.
	Replace(h HandlerSpec) error

	// Delete a handler.
	// It is not an error to delete an non-existent handler.
	Delete(id string) error

	// List handlers matching a pattern.
	// The pattern is shell/glob matching see https://golang.org/pkg/path/#Match
	// Offset and limit are pagination bounds. Offset is inclusive starting at index 0.
	// More results may exist while the number of returned items is equal to limit.
	List(pattern string, offset, limit int) ([]HandlerSpec, error)
}

const (
	handlerSpecDataPrefix    = "/handler-specs/data/"
	handlerSpecIndexesPrefix = "/handler-specs/indexes/"

	// Name of ID index
	idIndex = "id/"
)

//--------------------------------------------------------------------
// The following structures are stored in a database via gob encoding.
// Changes to the structures could break existing data.
//
// Many of these structures are exact copies of structures found elsewhere,
// this is intentional so that all structures stored in the database are
// defined here and nowhere else. So as to not accidentally change
// the gob serialization format in incompatible ways.

// version is the current version of the HandlerSpec structure.
const version = 1

// HandlerSpec provides all the necessary information to create a handler.
type HandlerSpec struct {
	ID      string              `json:"id"`
	Topics  []string            `json:"topics"`
	Actions []HandlerActionSpec `json:"actions"`
}

// HandlerActionSpec defines an action an handler can take.
type HandlerActionSpec struct {
	Kind    string                 `json:"kind"`
	Options map[string]interface{} `json:"options"`
}

func encodeHandlerSpec(h HandlerSpec) ([]byte, error) {
	return storage.VersionJSONEncode(version, h)
}

func decodeHandlerSpec(data []byte) (h HandlerSpec, err error) {
	err = storage.VersionJSONDecode(data, func(version int, dec *json.Decoder) error {
		return dec.Decode(&h)
	})
	return h, err
}

// Key/Value store based implementation of the HandlerSpecDAO
type handlerSpecKV struct {
	store storage.Interface
}

func newHandlerSpecKV(store storage.Interface) *handlerSpecKV {
	return &handlerSpecKV{
		store: store,
	}
}

// Create a key for the handler spec data
func (d *handlerSpecKV) handlerSpecDataKey(id string) string {
	return handlerSpecDataPrefix + id
}

// Create a key for a given index and value.
//
// Indexes are maintained via a 'directory' like system:
//
// /handler-specs/data/ID -- contains encoded handler spec data
// /handler-specs/index/id/ID -- contains the handler spec ID
//
// As such to list all handlers in ID sorted order use the /handler-specs/index/id/ directory.
func (d *handlerSpecKV) handlerSpecIndexKey(index, value string) string {
	return handlerSpecIndexesPrefix + index + value
}

func (d *handlerSpecKV) Get(id string) (HandlerSpec, error) {
	key := d.handlerSpecDataKey(id)
	if exists, err := d.store.Exists(key); err != nil {
		return HandlerSpec{}, err
	} else if !exists {
		return HandlerSpec{}, ErrNoHandlerSpecExists
	}
	kv, err := d.store.Get(key)
	if err != nil {
		return HandlerSpec{}, err
	}
	return decodeHandlerSpec(kv.Value)
}

func (d *handlerSpecKV) Create(h HandlerSpec) error {
	key := d.handlerSpecDataKey(h.ID)

	exists, err := d.store.Exists(key)
	if err != nil {
		return err
	}
	if exists {
		return ErrHandlerSpecExists
	}

	data, err := encodeHandlerSpec(h)
	if err != nil {
		return err
	}
	// Put data
	err = d.store.Put(key, data)
	if err != nil {
		return err
	}
	// Put ID index
	indexKey := d.handlerSpecIndexKey(idIndex, h.ID)
	return d.store.Put(indexKey, []byte(h.ID))
}

func (d *handlerSpecKV) Replace(h HandlerSpec) error {
	key := d.handlerSpecDataKey(h.ID)

	exists, err := d.store.Exists(key)
	if err != nil {
		return err
	}
	if !exists {
		return ErrNoHandlerSpecExists
	}

	data, err := encodeHandlerSpec(h)
	if err != nil {
		return err
	}
	// Put data
	err = d.store.Put(key, data)
	if err != nil {
		return err
	}
	return nil
}

func (d *handlerSpecKV) Delete(id string) error {
	key := d.handlerSpecDataKey(id)
	indexKey := d.handlerSpecIndexKey(idIndex, id)

	dataErr := d.store.Delete(key)
	indexErr := d.store.Delete(indexKey)
	if dataErr != nil {
		return dataErr
	}
	return indexErr
}

func (d *handlerSpecKV) List(pattern string, offset, limit int) ([]HandlerSpec, error) {
	// HandlerSpecs are indexed via their ID only.
	// While handler specs are sorted in the data section by their ID anyway
	// this allows us to do offset/limits and filtering without having to read in all handler spec data.

	// List all handler spec ids sorted by ID
	ids, err := d.store.List(handlerSpecIndexesPrefix + idIndex)
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
	matches := storage.DoListFunc(ids, match, offset, limit)

	specs := make([]HandlerSpec, len(matches))
	for i, id := range matches {
		data, err := d.store.Get(d.handlerSpecDataKey(string(id)))
		if err != nil {
			return nil, err
		}
		t, err := decodeHandlerSpec(data.Value)
		specs[i] = t
	}
	return specs, nil
}
