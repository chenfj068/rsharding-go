package main

import (
	"bufio"
	//"fmt"
//	"net"
	//"time"
	"strconv"
//	"bytes"
	"errors"
	"io"
)

type Decoder struct{
	
	reader *bufio.Reader
}


func NewRespDecoder(reader *bufio.Reader)*Decoder {
	cw := Decoder{reader:reader}
	return &cw
}

func btoi(b []byte) (int64, error) {
	if len(b) != 0 && len(b) < 10 {
		var neg, i = false, 0
		switch b[0] {
		case '-':
			neg = true
			fallthrough
		case '+':
			i++
		}
		if len(b) != i {
			var n int64
			for ; i < len(b) && b[i] >= '0' && b[i] <= '9'; i++ {
				n = int64(b[i]-'0') + n*10
			}
			if len(b) == i {
				if neg {
					n = -n
				}
				return n, nil
			}
		}
	}

	if n, err := strconv.ParseInt(string(b), 10, 64); err != nil {
		return 0,err
	} else {
		return n, nil
	}
}


func(d *Decoder) readLineBytes()([]byte,error){
	b,err:=d.reader.ReadBytes('\n')	
	if err != nil {
		return nil, err
	}
	if n := len(b) - 2; n < 0 || b[n] != '\r' {
		return nil, errors.New("Bad CRLF END")
	} else {
		return b[:n], nil
	}
	return nil,nil
}

func(d *Decoder) readInt()(int64,error){
	b, err := d.readLineBytes()
	if err != nil {
		return 0, err
	}
	return btoi(b)
}


func (d *Decoder)readBulkBytes()([]byte,error){
	n,err:=d.readInt()
	if(err!=nil){
		return nil,err
	}
	if n < -1 {
		return nil, errors.New("bad resp length")
	} else if n == -1 {
		return nil, nil
	}
	b := make([]byte, n+2)
	if _,err = io.ReadFull(d.reader,b); err != nil {
		return nil, err
	}
	if b[n] != '\r' || b[n+1] != '\n' {
		return nil, errors.New("bad resp CRLF")
	}
	return b[:n], nil
	return nil,nil
}

func (d *Decoder) decodeResp(depth int) (*Resp, error) {
	b, err := d.reader.ReadByte()
	if err != nil {
		return nil, err
	}
	switch t := RespType(b); t {
	case TypeString, TypeError, TypeInt:
		r := &Resp{Type: t}
		r.Value, err = d.readLineBytes()
		return r, err
	case TypeBulkBytes:
		r := &Resp{Type: t}
		r.Value, err = d.readBulkBytes()
		return r, err
	case TypeArray:
		r := &Resp{Type: t}
		r.Array, err = d.decodeArray(depth)
		return r, err
	default:
		if depth != 0 {
			return nil, errors.New("bad resp type "+string(t))
		}
		if err := d.reader.UnreadByte(); err != nil {
			return nil, err
		}
		r := &Resp{Type: TypeArray}
		r.Array, err = d.readSingleLineBulkBytesArray()
		return r, err
	}
}

func (d *Decoder) decodeArray(depth int) ([]*Resp, error) {
	n, err := d.readInt()
	if err != nil {
		return nil, err
	}
	if n < -1 {
		return nil, err
	} else if n == -1 {
		return nil, nil
	}
	a := make([]*Resp, n)
	for i := 0; i < len(a); i++ {
		if a[i], err = d.decodeResp(depth + 1); err != nil {
			return nil, err
		}
	}
	return a, nil
}


func (d *Decoder) readSingleLineBulkBytesArray() ([]*Resp, error) {
	b, err := d.readLineBytes()
	if err != nil {
		return nil, err
	}
	a := make([]*Resp, 0, 4)
	for l, r := 0, 0; r <= len(b); r++ {
		if r == len(b) || b[r] == ' ' {
			if l < r {
				a = append(a, &Resp{
					Type:  TypeBulkBytes,
					Value: b[l:r],
				})
			}
			l = r + 1
		}
	}
	return a, nil
}
