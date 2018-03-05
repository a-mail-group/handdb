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
import "github.com/vmihailenco/msgpack"
import "bytes"

var cnst1 = []byte("FWD")
var cnst2 = []byte("BAK")

type rtab1 struct {
	tn []byte
}
func (t *rtab1) Insert(tx *bolt.Tx, record [][]byte) error {
	if len(record)!=3 { return EBadRecord }
	ptab,err := tx.CreateBucketIfNotExists(t.tn)
	if err!=nil { return err }
	tab,err := ptab.CreateBucketIfNotExists(cnst1)
	if err!=nil { return err }
	rtab,err := ptab.CreateBucketIfNotExists(cnst2)
	if err!=nil { return err }
	ztab,err := rtab.CreateBucketIfNotExists(record[2])
	if err!=nil { return err }
	ztab.Put(record[0],cnst2)
	
	elem,_ := msgpack.Marshal(record[1],record[2])
	return tab.Put(record[0],elem)
}
func (t *rtab1) Lookup(tx *bolt.Tx, record [][]byte) (be [][]byte,err error) {
	var a1,a2 []byte
	if len(record)!=1 { return nil,EBadRecord }
	ptab := tx.Bucket(t.tn)
	if ptab==nil { return nil,ENotFound }
	tab := ptab.Bucket(cnst1)
	if tab==nil { return nil,ENotFound }
	ele := tab.Get(record[0])
	if len(ele)==0 { return nil,ENotFound }
	err = msgpack.Unmarshal(ele,&a1,&a2)
	if err!=nil { return }
	return [][]byte{a1,a2},nil
}
func (t *rtab1) Delete(tx *bolt.Tx, record [][]byte) error {
	var a1,a2 []byte
	if len(record)!=1 { return EBadRecord }
	ptab := tx.Bucket(t.tn)
	if ptab==nil { return ENotFound }
	tab := ptab.Bucket(cnst1)
	if tab==nil { return ENotFound }
	rtab := ptab.Bucket(cnst2)
	if rtab==nil { return ENotFound }
	ele := tab.Get(record[0])
	if len(ele)==0 { return ENotFound }
	msgpack.Unmarshal(ele,&a1,&a2)
	err := rtab.Delete(a2)
	if err!=nil { return err }
	ztab,err := rtab.CreateBucketIfNotExists(a2)
	if err!=nil { return err }
	err = ztab.Delete(record[0])
	if err!=nil { return err }
	return tab.Delete(record[0])
}
func (t *rtab1) Count(tx *bolt.Tx, record [][]byte) (uint64,error) {
	if len(record)!=1 { return 0,EBadRecord }
	ptab := tx.Bucket(t.tn)
	if ptab==nil { return 0,ENotFound }
	tab := ptab.Bucket(cnst1)
	if tab==nil { return 0,nil }
	ele := tab.Get(record[0])
	if len(ele)==0 { return 0,nil }
	return 1,nil
}
func (t *rtab1) Expire(tx *bolt.Tx, record [][]byte) error {
	if len(record)!=1 { return EBadRecord }
	ptab := tx.Bucket(t.tn)
	if ptab==nil { return ENotFound }
	tab  := ptab.Bucket(cnst1)
	if tab==nil  { return ENotFound }
	rtab := ptab.Bucket(cnst2)
	if rtab==nil { return ENotFound }
	cur := rtab.Cursor()
	for k,_ := cur.First(); len(k)>0 && bytes.Compare(k,record[0])<=0 ; k,_ = cur.Next() {
		ztab := rtab.Bucket(k)
		zcur := ztab.Cursor()
		for v,_ := zcur.First(); len(v)>0 ; v,_ = zcur.Next() {
			zcur.Delete()
			tab.Delete(v)
		}
		rtab.Delete(k)
	}
	return nil
}

type rtab2 struct {
	tab2
}
func (t *rtab2) Insert(tx *bolt.Tx, record [][]byte) error {
	var count uint64
	if len(record)!=4 { return EBadRecord }
	tab,err := tx.CreateBucketIfNotExists(t.tn)
	if err!=nil { return err }
	rc := append([]byte("@"),record[0]...)
	dt := append([]byte("&"),record[3]...)
	key,_ := msgpack.Marshal(record[0],record[1])
	rec,_ := msgpack.Marshal(record[2],record[3])
	part,err := tab.CreateBucketIfNotExists(rc)
	if err!=nil { return err }
	bak,err := tab.CreateBucketIfNotExists(dt)
	if err!=nil { return err }
	rc[0] = '$'
	msgpack.Unmarshal(tab.Get(rc),&count)
	err = part.Put(record[1],rec)
	if err!=nil { return err }
	err = bak.Put(key,cnst2)
	if err!=nil { return err }
	count++
	blob,_ := msgpack.Marshal(count)
	err = tab.Put(rc,blob)
	if err!=nil { return err }
	return nil
}
func (t *rtab2) Lookup(tx *bolt.Tx, record [][]byte) (be [][]byte,err error) {
	var a1,a2 []byte
	if len(record)!=2 { return nil,EBadRecord }
	tab := tx.Bucket(t.tn)
	if tab==nil { return nil,ENotFound }
	rc := append([]byte("@"),record[0]...)
	part := tab.Bucket(rc)
	if part==nil { return nil,ENotFound }
	ele := part.Get(record[1])
	if len(ele)==0 { return nil,ENotFound }
	err = msgpack.Unmarshal(ele,&a1,&a2)
	if err!=nil { return }
	return [][]byte{a1,a2},nil
}
func (t *rtab2) Delete(tx *bolt.Tx, record [][]byte) error {
	var a1,a2 []byte
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
	msgpack.Unmarshal(ele,&a1,&a2)
	dt := append([]byte("&"),a2...)
	count--
	blob,_ := msgpack.Marshal(count)
	err := tab.Put(rc,blob)
	if err!=nil { return err }
	err = part.Delete(record[1])
	if err!=nil { return err }
	return tab.Delete(dt)
}
func (t *rtab2) Expire(tx *bolt.Tx, record [][]byte) error {
	if len(record)!=1 { return EBadRecord }
	tab := tx.Bucket(t.tn)
	cur := tab.Cursor()
	subbase := [][]byte{nil,nil}
	for k,_ := cur.Seek([]byte("&")) ; true ; k,_ = cur.Next() {
		if len(k)==0 { break }
		if k[0]!='&' { break }
		if bytes.Compare(k[1:],record[0])>0 { break }
		
		ztab := tab.Bucket(k)
		if ztab==nil { continue }
		zcur := ztab.Cursor()
		for v,_ := zcur.First(); len(v)>0 ; v,_ = zcur.Next() {
			msgpack.Unmarshal(v,&subbase[0],&subbase[1])
			zcur.Delete()
			t.tab2.Delete(tx,subbase)
		}
		tab.DeleteBucket(k)
	}
	return nil
}




