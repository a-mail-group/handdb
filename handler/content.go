/*
Copyright (c) 2018 Simon Schmidt

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/

package handler

import bolt "github.com/coreos/bbolt"
import "fmt"
import "github.com/vmihailenco/msgpack"

var ENotFound  = fmt.Errorf("Key Not Found")
var EBadRecord = fmt.Errorf("Bad Record"   )

type tab1 struct {
	tn []byte
}
func (t *tab1) Insert(tx *bolt.Tx, record [][]byte) error {
	if len(record)!=2 { return EBadRecord }
	tab,err := tx.CreateBucketIfNotExists(t.tn)
	if err!=nil { return err }
	return tab.Put(record[0],record[1])
}
func (t *tab1) Lookup(tx *bolt.Tx, record [][]byte) (be [][]byte,err error) {
	if len(record)!=1 { return nil,EBadRecord }
	tab := tx.Bucket(t.tn)
	if tab==nil { return nil,ENotFound }
	ele := tab.Get(record[0])
	if len(ele)==0 { return nil,ENotFound }
	return [][]byte{ele},nil
}
func (t *tab1) Delete(tx *bolt.Tx, record [][]byte) error {
	if len(record)!=1 { return EBadRecord }
	tab := tx.Bucket(t.tn)
	if tab==nil { return ENotFound }
	ele := tab.Get(record[0])
	if len(ele)==0 { return ENotFound }
	return tab.Delete(record[0])
}
func (t *tab1) Count(tx *bolt.Tx, record [][]byte) (uint64,error) {
	if len(record)!=1 { return 0,EBadRecord }
	tab := tx.Bucket(t.tn)
	if tab==nil { return 0,nil }
	ele := tab.Get(record[0])
	if len(ele)==0 { return 0,nil }
	return 1,nil
}
func (t *tab1) Expire(tx *bolt.Tx, record [][]byte) error {
	if len(record)!=1 { return EBadRecord }
	return nil
}

type tab2 struct {
	tn []byte
}
func (t *tab2) Insert(tx *bolt.Tx, record [][]byte) error {
	var count uint64
	if len(record)!=3 { return EBadRecord }
	tab,err := tx.CreateBucketIfNotExists(t.tn)
	if err!=nil { return err }
	rc := append([]byte("@"),record[0]...)
	part,err := tab.CreateBucketIfNotExists(rc)
	rc[0] = '$'
	msgpack.Unmarshal(tab.Get(rc),&count)
	if err!=nil { return err }
	err = part.Put(record[1],record[2])
	if err!=nil { return err }
	count++
	blob,_ := msgpack.Marshal(count)
	return tab.Put(rc,blob)
}
func (t *tab2) Lookup(tx *bolt.Tx, record [][]byte) (be [][]byte,err error) {
	if len(record)!=2 { return nil,EBadRecord }
	tab := tx.Bucket(t.tn)
	if tab==nil { return nil,ENotFound }
	rc := append([]byte("@"),record[0]...)
	part := tab.Bucket(rc)
	if part==nil { return nil,ENotFound }
	ele := part.Get(record[1])
	if len(ele)==0 { return nil,ENotFound }
	return [][]byte{ele},nil
}
func (t *tab2) Delete(tx *bolt.Tx, record [][]byte) error {
	var count uint64
	if len(record)!=2 { return EBadRecord }
	tab := tx.Bucket(t.tn)
	if tab==nil { return ENotFound }
	rc := append([]byte("@"),record[0]...)
	part := tab.Bucket(rc)
	rc[0] = '$'
	if part==nil { return ENotFound }
	msgpack.Unmarshal(tab.Get(rc),&count)
	ele := part.Get(record[1])
	if len(ele)==0 { return ENotFound }
	count--
	blob,_ := msgpack.Marshal(count)
	err := tab.Put(rc,blob)
	if err!=nil { return err }
	return part.Delete(record[1])
}
func (t *tab2) Count(tx *bolt.Tx, record [][]byte) (uint64,error) {
	var count uint64
	if len(record)!=1 { return 0,EBadRecord }
	tab := tx.Bucket(t.tn)
	if tab==nil { return 0,nil }
	rc := append([]byte("$"),record[0]...)
	msgpack.Unmarshal(tab.Get(rc),&count)
	return count,nil
}
func (t *tab2) Expire(tx *bolt.Tx, record [][]byte) error {
	if len(record)!=1 { return EBadRecord }
	return nil
}

