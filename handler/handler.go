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

import "net/textproto"
import bolt "github.com/coreos/bbolt"
import "fmt"

func escape(x []byte) []byte {
	y := make([]byte,0,len(x)+2+16)
	y = append(y,'"')
	for _,b := range x {
		if b=='"' { y = append(y,'"') }
		y = append(y,b)
	}
	y = append(y,'"')
	return y
}

type table interface{
	Insert(tx *bolt.Tx, record [][]byte) error
	Lookup(tx *bolt.Tx, record [][]byte) (be [][]byte,err error)
	Delete(tx *bolt.Tx, record [][]byte) error
	Count(tx *bolt.Tx, record [][]byte) (uint64,error)
	Expire(tx *bolt.Tx, record [][]byte) error
}

type Master struct{
	DB *bolt.DB
}
func (m *Master) get(tn []byte) table {
	switch string(tn){
	case "bag": return &tab1{tn}
	case "group": return &tab2{tn}
	case "bug": return &rtab1{tn}
	}
	return nil
}

func (m *Master) Handle(c *textproto.Conn) {
	(&Handler{m,c}).Handle()
}

type Handler struct{
	*Master
	C *textproto.Conn
}

func (h *Handler) Handle() {
	defer h.C.Close()
	for {
		s,e := h.C.ReadLine()
		if e!=nil { break }
		switch s {
		case "PING": h.C.PrintfLine("PONG")
		case "QUIT": return
		case "RCD":{
				res := parseQuery(h.C.DotReader())
				dmk := make([]string,len(res))
				h.DB.Batch(func(tx *bolt.Tx) error {
					for i,que := range res {
						if len(que)==0 { continue }
						dmk[i] = "!table"
						tab := h.get(que[0])
						if tab==nil { continue }
						dmk[i] = "fail"
						err := tab.Insert(tx,que[1:])
						if err==ENotFound {
							dmk[i] = "!found"
						} else if err==EBadRecord {
							dmk[i] = "!valid"
						} else if err==nil {
							dmk[i] = "ok"
						}
					}
					return nil
				})
				h.C.PrintfLine("LINES")
				wc := h.C.DotWriter()
				for _,s := range dmk {
					fmt.Fprintln(wc,s)
				}
				wc.Close()
			}
		case "LKUP":{
				res := parseQuery(h.C.DotReader())
				dmk := make([]string,len(res))
				result := make([][][]byte,len(res))
				h.DB.View(func(tx *bolt.Tx) error {
					for i,que := range res {
						if len(que)==0 { continue }
						dmk[i] = "!table"
						tab := h.get(que[0])
						if tab==nil { continue }
						dmk[i] = "fail"
						line,err := tab.Lookup(tx,que[1:])
						if err==ENotFound {
							dmk[i] = "!found"
						} else if err==EBadRecord {
							dmk[i] = "!valid"
						} else if err==nil {
							dmk[i] = "ok"
							result[i] = line
						}
					}
					return nil
				})
				h.C.PrintfLine("RSTS")
				wc := h.C.DotWriter()
				for i,s := range dmk {
					for _,elem := range result[i] {
						fmt.Fprintf(wc,"%s ",escape(elem))
					}
					fmt.Fprintln(wc,s)
				}
				wc.Close()
			}
		case "DEL":{
				res := parseQuery(h.C.DotReader())
				dmk := make([]string,len(res))
				h.DB.Batch(func(tx *bolt.Tx) error {
					for i,que := range res {
						if len(que)==0 { continue }
						dmk[i] = "!table"
						tab := h.get(que[0])
						if tab==nil { continue }
						dmk[i] = "fail"
						err := tab.Delete(tx,que[1:])
						if err==ENotFound {
							dmk[i] = "!found"
						} else if err==EBadRecord {
							dmk[i] = "!valid"
						} else if err==nil {
							dmk[i] = "ok"
						}
					}
					return nil
				})
				h.C.PrintfLine("LINES")
				wc := h.C.DotWriter()
				for _,s := range dmk {
					fmt.Fprintln(wc,s)
				}
				wc.Close()
			}
		case "CNT":{
				res := parseQuery(h.C.DotReader())
				dmk := make([]string,len(res))
				result := make([]uint64,len(res))
				h.DB.View(func(tx *bolt.Tx) error {
					for i,que := range res {
						if len(que)==0 { continue }
						dmk[i] = "!table"
						tab := h.get(que[0])
						if tab==nil { continue }
						dmk[i] = "fail"
						line,err := tab.Count(tx,que[1:])
						if err==ENotFound {
							dmk[i] = "!found"
						} else if err==EBadRecord {
							dmk[i] = "!valid"
						} else if err==nil {
							dmk[i] = "ok"
							result[i] = line
						}
					}
					return nil
				})
				h.C.PrintfLine("RSTS")
				wc := h.C.DotWriter()
				for i,s := range dmk {
					fmt.Fprintf(wc,"%d ",result[i])
					fmt.Fprintln(wc,s)
				}
				wc.Close()
			}
		case "XPR":{
				res := parseQuery(h.C.DotReader())
				dmk := make([]string,len(res))
				h.DB.Batch(func(tx *bolt.Tx) error {
					for i,que := range res {
						if len(que)==0 { continue }
						dmk[i] = "!table"
						tab := h.get(que[0])
						if tab==nil { continue }
						dmk[i] = "fail"
						err := tab.Expire(tx,que[1:])
						if err==ENotFound {
							dmk[i] = "!found"
						} else if err==EBadRecord {
							dmk[i] = "!valid"
						} else if err==nil {
							dmk[i] = "ok"
						}
					}
					return nil
				})
				h.C.PrintfLine("LINES")
				wc := h.C.DotWriter()
				for _,s := range dmk {
					fmt.Fprintln(wc,s)
				}
				wc.Close()
			}
		default:
			h.C.PrintfLine("Unknown: %s",s)
		}
	}
}

