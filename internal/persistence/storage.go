// Copyright (c) 2022 Contributors to the Eclipse Foundation
//
// See the NOTICE file(s) distributed with this work for additional
// information regarding copyright ownership.
//
// This program and the accompanying materials are made available under the
// terms of the Eclipse Public License 2.0 which is available at
// http://www.eclipse.org/legal/epl-2.0
//
// SPDX-License-Identifier: EPL-2.0

package persistence

import (
	"bytes"
	"encoding/gob"
	"errors"
	"reflect"

	"go.etcd.io/bbolt"
)

// Database access interface
type Database interface {
	// GetName returns the database system name.
	GetName() (string, error)
	// SetName sets the database system name.
	SetName(name string) error

	// Get returns raw value.
	Get(key string) ([]byte, error)
	// GetAs reads the value decoded to the specified type.
	GetAs(key string, value interface{}) error
	// GetAllAs reads all values matching the key prefix and decoded to the specified type.
	GetAllAs(keyPrefix string, valuesType interface{}) ([]interface{}, error)

	// Set updates key data.
	Set(key string, data []byte) error
	// SetAs updates key data encoded using its type.
	SetAs(key string, value interface{}) error
	// SetAllAs updates a bunch of key-value pairs, using their types while encoding data.
	SetAllAs(data map[string]interface{}) error

	// UpdateAllAs combines clean and set of new data.
	// It's equivalent to Delete All and SetAllAs called as one operation.
	UpdateAllAs(keyPrefix string, data map[string]interface{}) error

	// Delete removes key and its data.
	Delete(key string) error
	// DeleteAll removes all keys matching the prefix and their data.
	DeleteAll(keyPrefix string) error

	// Close closes the opened database.
	Close() error
}

const systemKeyDbName = "@SYSTEM/NAME"

var bboltBucket = []byte("things")

// storage type
type storage struct {
	path   string
	db     *bbolt.DB
	closed bool
}

var (
	// ErrDatabaseNil for nil database.
	ErrDatabaseNil = errors.New("database is nil")

	// ErrDatabaseClosed is returned when the accessed database instance is closed.
	ErrDatabaseClosed = errors.New("database is closed")

	// ErrNotFound if the key does not exist.
	ErrNotFound = errors.New("not found")
)

// Decode utils

func init() {
	gob.Register(map[string]interface{}{})
	gob.Register([]interface{}{})
}

func encodeAs(value interface{}) ([]byte, error) {
	buffer := &bytes.Buffer{}
	encBuffer := gob.NewEncoder(buffer)
	if err := encBuffer.Encode(value); err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}

func decodeAs(data []byte, value interface{}) error {
	buffer := bytes.NewBuffer(data)
	decBuffer := gob.NewDecoder(buffer)
	return decBuffer.Decode(value)
}

// NewDatabase opens the database
func NewDatabase(path string) (Database, error) {
	db, err := bbolt.Open(path, 0600, nil)

	if err != nil {
		return nil, err
	}

	if err := db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(bboltBucket)
		return err
	}); err != nil {
		db.Close()
		return nil, err
	}

	return &storage{
		path:   path,
		db:     db,
		closed: false,
	}, nil
}

// Close closes the db
func (storage *storage) Close() error {
	if storage.db == nil {
		return ErrDatabaseNil
	}
	if storage.closed {
		return ErrDatabaseClosed
	}

	if err := storage.db.Close(); err != nil {
		return err
	}
	storage.closed = true
	return nil
}

func (storage *storage) dbOpened() error {
	if storage.db == nil {
		return ErrDatabaseNil
	}
	if storage.closed {
		return ErrDatabaseClosed
	}
	return nil
}

func (storage *storage) GetName() (string, error) {
	name, err := storage.Get(systemKeyDbName)
	if err != nil {
		return "", err
	}
	return string(name), nil
}

func (storage *storage) SetName(name string) error {
	return storage.Set(systemKeyDbName, []byte(name))
}

func (storage *storage) Get(key string) ([]byte, error) {
	if err := storage.dbOpened(); err != nil {
		return nil, err
	}

	var data []byte
	if err := storage.db.View(func(tx *bbolt.Tx) error {
		data = tx.Bucket(bboltBucket).Get([]byte(key))
		return nil
	}); err != nil {
		return nil, err
	}

	if data == nil {
		return nil, ErrNotFound
	}

	return data, nil
}

func (storage *storage) GetAs(key string, value interface{}) error {
	if err := storage.dbOpened(); err != nil {
		return err
	}

	f := func(val []byte) error {
		if vb, ok := value.([]byte); ok {
			if cap(vb) < len(val) {
				vb = make([]byte, len(val))
			}
			copy(vb, val)
			return nil
		}

		if err := decodeAs(val, value); err != nil {
			return err
		}
		return nil
	}

	if err := storage.db.View(func(tx *bbolt.Tx) error {
		data := tx.Bucket(bboltBucket).Get([]byte(key))
		if data == nil {
			return ErrNotFound
		}
		if err := f(data); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}
	return nil
}

func (storage *storage) GetAllAs(prefix string, value interface{}) ([]interface{}, error) {
	if err := storage.dbOpened(); err != nil {
		return nil, err
	}
	valueType := reflect.ValueOf(value).Elem().Type()

	var values []interface{}
	f := func(val []byte) error {
		nextValue := reflect.New(valueType).Interface()
		if vb, ok := nextValue.([]byte); ok {
			if cap(vb) < len(val) {
				vb = make([]byte, len(val))
			}
			copy(vb, val)
			return nil
		}

		if err := decodeAs(val, nextValue); err != nil {
			return err
		}
		values = append(values, nextValue)
		return nil
	}

	if err := storage.db.View(func(tx *bbolt.Tx) error {
		it := tx.Bucket(bboltBucket).Cursor()

		keyPrefix := []byte(prefix)
		for k, v := it.Seek(keyPrefix); k != nil && bytes.HasPrefix(k, keyPrefix); k, v = it.Next() {
			if err := f(v); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return values, nil
}

func (storage *storage) Set(key string, value []byte) error {
	if err := storage.dbOpened(); err != nil {
		return err
	}

	return storage.db.Update(func(tx *bbolt.Tx) error {
		return tx.Bucket(bboltBucket).Put([]byte(key), value)
	})
}

// SetAs updates key data
func (storage *storage) SetAs(key string, value interface{}) error {
	if err := storage.dbOpened(); err != nil {
		return err
	}

	f := func(tx *bbolt.Tx) error {
		b := tx.Bucket(bboltBucket)
		valueBytes, err := encodeAs(value)
		if err != nil {
			return err
		}
		if err := b.Put([]byte(key), valueBytes); err != nil {
			return err
		}
		return nil
	}

	return storage.db.Update(f)
}

func (storage *storage) SetAllAs(values map[string]interface{}) error {
	if err := storage.dbOpened(); err != nil {
		return err
	}

	f := func(tx *bbolt.Tx) error {
		b := tx.Bucket(bboltBucket)
		for key, value := range values {
			valueBytes, err := encodeAs(value)
			if err != nil {
				return err
			}
			if err := b.Put([]byte(key), valueBytes); err != nil {
				return err
			}
		}
		return nil
	}

	return storage.db.Update(f)
}

func (storage *storage) UpdateAllAs(prefix string, values map[string]interface{}) error {
	if err := storage.dbOpened(); err != nil {
		return err
	}

	f := func(tx *bbolt.Tx) error {
		b := tx.Bucket(bboltBucket)

		it := b.Cursor()
		keyPrefix := []byte(prefix)
		for k, _ := it.Seek(keyPrefix); k != nil && bytes.HasPrefix(k, keyPrefix); k, _ = it.Next() {
			err := b.Delete(k)
			if err != nil {
				return err
			}
		}

		for key, value := range values {
			valueBytes, err := encodeAs(value)
			if err != nil {
				return err
			}
			if err := b.Put([]byte(key), valueBytes); err != nil {
				return err
			}
		}
		return nil
	}

	return storage.db.Update(f)
}

func (storage *storage) Delete(key string) error {
	if err := storage.dbOpened(); err != nil {
		return err
	}

	return storage.db.Update(func(tx *bbolt.Tx) error {
		return tx.Bucket(bboltBucket).Delete([]byte(key))
	})
}

func (storage *storage) DeleteAll(prefix string) error {
	if err := storage.dbOpened(); err != nil {
		return err
	}
	f := func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(bboltBucket)
		it := bucket.Cursor()

		keyPrefix := []byte(prefix)
		for k, _ := it.Seek(keyPrefix); k != nil && bytes.HasPrefix(k, keyPrefix); k, _ = it.Next() {
			err := bucket.Delete(k)
			if err != nil {
				return err
			}
		}
		return nil
	}
	return storage.db.Update(f)

}
