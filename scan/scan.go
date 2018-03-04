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

package scan

//import "fmt"

func grow(token []byte) []byte {
	if cap(token) > len(token) { return token }
	l := 128
	if len(token) < l { l = len(token) }
	if l==0 { l = 8 }
	ntoken := make([]byte,len(token),len(token)+l)
	copy(ntoken,token)
	return ntoken
}

func ScanElem(data []byte, atEOF bool) (advance int, token []byte, err error) {
	t := 0
	for i,b := range data {
		switch t {
		case 0:
			switch b {
			case '\r','\n':
				token = append(grow(token),'N')
				t = 4
				continue
			}
			if b<=' ' { continue }
			token = append(grow(token),':')
			switch b {
			case '"': t = 1
			default:
				token = append(grow(token),b)
				t = 3
			}
		case 1:
			if b=='"' { t = 2; continue }
			token = append(grow(token),b)
		case 2:
			if b!='"' { advance = i; return }
			token = append(grow(token),b)
			t = 1
		case 3:
			if b<=' ' { advance = i; return }
			token = append(grow(token),b)
		case 4:
			switch b {
			case '\r','\n': continue
			}
			advance = i
			return
		}
	}
	if atEOF { advance = len(data) }
	return
}




