package leaf

import (
    "bytes"
    "errors"
    "sort"

    "github.com/boltdb/bolt"
)

// ItemHandler represents a callback for processing a single key-value pair.
type ItemHandler func(k, v []byte) error

var (
    // ErrKeyNotFound is returned if a Keyspace did not contain the key
    ErrKeyNotFound = errors.New("Key does not exist")

    // ErrEmptyKeyList is returned if Keyspace.List() is called with no keys
    ErrEmptyKeyList = errors.New("Empty key list")
)

// TxCallback allows for more complex operations on a bucket. It is utilized in the ReadTx and WriteTx functions.
// type TxCallback func(*bolt.Bucket)

// Keyspace is an interface for Database keyspaces. It is used as a wrapper for database actions.
type Keyspace interface {

    // GetName returns the name of the keyspace
    GetName() string

    // List finds all the keys listed and calls the function provided with the key value pairs
    List([]string, func(k, v []byte)) error

    // Insert adds a key value to the keyspace
    Insert(string, []byte) error

    // Get returns a value with the associated key and returns an error if the key does not exist
    Get(string) ([]byte, error)

    // Update overrides the existing value associated with the given key
    Update(string, []byte) error

    // Delete removes a key from the keyspace
    Delete(string) error

    // Size returns the number of items in the keyspace
    Size() int64

    // ForEach iterates over all the keys in the keyspace
    ForEach(ItemHandler) error

    // Contains determines if the given key exists in the keyspace
    Contains(string) (bool, error)

    // ReadTx allows for more complicated read operations on a particular key, such as reading nested values.
    ReadTx(func(*bolt.Bucket)) error

    // WriteTx allows for more complicated write operations on a particular key, such as writing nested values.
    WriteTx(func(*bolt.Bucket)) error
}

// KeyValueDatabase is used as an interface for accessing multiple keyspaces.
type KeyValueDatabase interface {

    // GetOrCreatKeyspace returns a new keyspace instance from the database, creating it if it doesn't exist
    GetOrCreateKeyspace(string) (Keyspace, error)

    // DeleteKeyspace removes a keyspace from the database
    DeleteKeyspace(string) error

    // Close closes the database connection
    Close() error
}

// NewLeaf creates a connection to a BoltDB file
func NewLeaf(file string) (KeyValueDatabase, error) {
    db, err := bolt.Open(file, 0755, nil)
    if err != nil {
        return nil, err
    }
    return &DB{db}, nil
}

// DB wraps a BoltDB connection
type DB struct {
    db *bolt.DB
}

// GetOrCreateKeyspace returns a Keyspace implementation for the underlying BoltDB instance.
func (l *DB) GetOrCreateKeyspace(name string) (ks Keyspace, err error) {
    err = l.db.Update(func(tx *bolt.Tx) error {
        _, er := tx.CreateBucketIfNotExists([]byte(name))

        ks = &BoltKeyspace{name, l.db}
        return er
    })
    return ks, err
}

// Close closes the database connection
func (l *DB) Close() error {
    return l.db.Close()
}

// DeleteKeyspace removes a keyspace from the database
func (l *DB) DeleteKeyspace(name string) error {
    err := l.db.Update(func(tx *bolt.Tx) error {
        return tx.DeleteBucket([]byte(name))
    })
    return err
}

// BoltKeyspace implements the Keyspace interface on top of a boltdb connection
type BoltKeyspace struct {
    name string
    db   *bolt.DB
}

// GetName returns the name of the keyspace
func (b *BoltKeyspace) GetName() string {
    return b.name
}

// List iterates over the given keys and calls the ItemHandler with each key value pair
func (b *BoltKeyspace) List(keys []string, callback func(k, v []byte)) error {
    // if no keys are searched for then return error
    if len(keys) == 0 {
        return ErrEmptyKeyList
    }

    // inplace lexigraphical sort
    sort.Strings(keys)

    // create lookup table
    lookup := make(map[string]bool)
    for _, k := range keys {
        lookup[k] = true
    }

    // create db view
    err := b.db.View(func(tx *bolt.Tx) error {

        // open bucket
        b := tx.Bucket([]byte(b.name))

        // create cursor
        c := b.Cursor()

        // iterate over bucket keys from first key to last
        last := []byte(keys[len(keys)-1])
        for k, v := c.Seek([]byte(keys[0])); k != nil && bytes.Compare(k, last) <= 0; k, v = c.Next() {

            // if key is what we are looking for
            if _, ok := lookup[string(k)]; ok {

                // call callback
                callback(k, v)
                // fmt.Printf("key=%s, value=%s\n", k, v)
            }
        }
        return nil
    })
    return err
}

// Insert adds a key value pair to the databaes
func (b *BoltKeyspace) Insert(key string, value []byte) error {

    err := b.db.Update(func(tx *bolt.Tx) error {
        b := tx.Bucket([]byte(b.name))
        err := b.Put([]byte(key), value)
        return err
    })
    return err
}

// Get returns the value for the given key
func (b *BoltKeyspace) Get(key string) (value []byte, err error) {

    err = b.db.View(func(tx *bolt.Tx) error {
        b := tx.Bucket([]byte(b.name))
        value = b.Get([]byte(key))
        if value == nil {
            return ErrKeyNotFound
        }
        return nil
    })
    return
}

// Update overwrites an existing value
func (b *BoltKeyspace) Update(key string, value []byte) error {
    return b.Insert(key, value)
}

// Delete removes a key from the keyspace
func (b *BoltKeyspace) Delete(key string) error {
    return b.db.Update(func(tx *bolt.Tx) error {
        b := tx.Bucket([]byte(b.name))
        return b.Delete([]byte(key))
    })
}

// Size returns the number of keys in the keyspace
func (b *BoltKeyspace) Size() (value int64) {
    b.db.View(func(tx *bolt.Tx) error {
        bucket := tx.Bucket([]byte(b.name))
        stats := bucket.Stats()
        value = int64(stats.KeyN)
        return nil
    })
    return
}

// ForEach iterates over all the key value pairs in the keyspace
func (b *BoltKeyspace) ForEach(each ItemHandler) error {
    return b.db.View(func(tx *bolt.Tx) error {
        b := tx.Bucket([]byte(b.name))
        return b.ForEach(each)
    })
}

// Contains determines if a key already exists in the keyspace
func (b *BoltKeyspace) Contains(key string) (exists bool, err error) {

    err = b.db.View(func(tx *bolt.Tx) error {
        b := tx.Bucket([]byte(b.name))
        value := b.Get([]byte(key))
        if value != nil {
            exists = true
        }
        return nil
    })

    return exists, err
}

// ReadTx allows for more complex read operations on the keyspace
func (b *BoltKeyspace) ReadTx(callback func(*bolt.Bucket)) error {
    err := b.db.View(func(tx *bolt.Tx) error {
        bkt := tx.Bucket([]byte(b.name))

        callback(bkt)
        return nil
    })
    return err
}

// WriteTx allows for more complex write operations on the keyspace
func (b *BoltKeyspace) WriteTx(callback func(*bolt.Bucket)) error {
    err := b.db.Update(func(tx *bolt.Tx) error {
        bkt := tx.Bucket([]byte(b.name))

        callback(bkt)
        return nil
    })
    return err
}
