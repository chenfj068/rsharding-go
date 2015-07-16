package main

/**
Simple redis protocol parser

**/
import (
	"bufio"
	"fmt"
	"net"
	//"time"
	"strconv"
)

type RespReaderWriter struct {
	readerWriter bufio.ReadWriter
}

func NewRespReadWriter(conn net.Conn) RespReaderWriter {
	writer := bufio.NewWriter(conn)
	reader := bufio.NewReader(conn)
	rw := bufio.NewReadWriter(reader, writer)
	cw := RespReaderWriter{readerWriter: *rw}
	return cw
}

//read raw string data from server、client
func (rw *RespReaderWriter) ProxyRead() (string, error) {
	b, err := rw.readerWriter.ReadByte()
	if err != nil {
		return "", err
	}
	s := string(b)
	switch s {
	case "+", "-", ":":
		ss, er := readPart(rw)
		if er != nil {
			return "", er
		}
		return s + ss + "\r\n", nil
	case "$":
		ss, length, err := readBulkString(rw)
		if err != nil {
			return "", err
		}
		res := ""
		if length > 0 {
			res = s + strconv.Itoa(length) + "\r\n" + ss.(string) + "\r\n"
		} else if length == -1 {
			res = res + strconv.Itoa(length) + "\r\n"
		} else {
			res = res + strconv.Itoa(length) + "\r\n\r\n"
		}

		return res, nil
	case "*":
		p, er := readPart(rw)
		if er != nil {
			return "", er
		}
		res := ""
		size, _ := strconv.Atoi(p)
		res = "*" + strconv.Itoa(size) + "\r\n"
		for i := 0; i < size; i++ {
			b, er := rw.readerWriter.ReadByte()
			if er != nil {
				return "", er
			}
			_s := string(b)
			switch _s {
			case "$":
				v, length, err := readBulkString(rw)
				if err != nil {
					return "", err
				}
				if length > 0 {
					res = res + _s + strconv.Itoa(length) + "\r\n" + v.(string) + "\r\n"
				} else if length == -1 {
					res = res + strconv.Itoa(length) + "\r\n"
				} else {
					res = res + strconv.Itoa(length) + "\r\n\r\n"
				}
			case "+":
				fallthrough
			case "-":
				fallthrough
			case ":":
				v, err := readPart(rw)
				if err != nil {
					return "", err
				}
				res = res + _s + v + "\r\n"
			}

		}
		return res, nil

	}
	return "", nil
}

func (rw *RespReaderWriter) ProxyWrite(encoded string) error {
	rw.readerWriter.WriteString(encoded)
	rw.readerWriter.Flush()
	return nil
}
func (w *RespReaderWriter) Write(command string, key string, params ...interface{}) error {
	//create bulkstring array
	//write bulkstring to conn output
	cmdLen := 2 + len(params)
	ss := "*" + strconv.Itoa(cmdLen) + "\r\n"
	ss += "$" + strconv.Itoa(len(command)) + "\r\n" + command + "\r\n"
	ss += "$" + strconv.Itoa(len(key)) + "\r\n" + key + "\r\n"

	for i := 0; i < len(params); i++ {
		p := params[i]
		s := ""
		switch p.(type) {
		case int, int32, int64, float32, float64:
			s = fmt.Sprint(p)
		case string:
			s = p.(string)
		case bool:
			if p.(bool) {
				s = fmt.Sprint(1)
			} else {
				s = fmt.Sprint(0)
			}
		}
		s0 := "$" + strconv.Itoa(len(s)) + "\r\n" + s + "\r\n"
		ss += s0

	}
	//	fmt.Println("write string:" + ss)
	_, err := w.readerWriter.WriteString(ss)
	w.readerWriter.Flush()
	if err != nil {
		return err
	}
	return nil
}

func (r *RespReaderWriter) LoopRead(cch chan<- int) (dch <-chan []interface{}) {
	ch := make(chan []interface{})
	go func() {
		for {
			d, err := r.Read()
			if err != nil {
				fmt.Printf("%v",err)
				cch <- 1
				close(cch)
				close(ch)
				break
			} else if(len(d)>0){
				ch <- d
			}
		}
	}()
	return ch
}
func (r *RespReaderWriter) Read() ([]interface{}, error) {
	b, err := r.readerWriter.ReadByte()
	if err != nil {
		return nil, err
	}
	s := string(b)
	switch s {
	case "+":
		s, er := readPart(r)
		if er != nil {
			return nil, er
		}
		return []interface{}{s}, nil
	case "-":
		s, er := readPart(r)
		if er != nil {
			return nil, er
		}
		return []interface{}{s}, nil
	case ":":
		s, er := readPart(r)
		if er != nil {
			return nil, er
		}
		return []interface{}{s}, nil
	case "$":
		s, _, err := readBulkString(r)
		if err != nil {
			return nil, err
		}
		return []interface{}{s}, nil
	case "*":
		array, err := readArray(r)
		if err != nil {
			return nil, err
		}
		return array, nil

	}
	fmt.Println("return nil")
	return nil, nil
}

func readArray(r *RespReaderWriter) ([]interface{}, error) {
	p, er := readPart(r)
	if er != nil {
		return nil, er
	}
	size, _ := strconv.Atoi(p)
	res := make([]interface{}, 0, size)
	for ; size > 0; size-- {
		b, er := r.readerWriter.ReadByte()
		if er != nil {
			return nil, er
		}
		s := string(b)
		switch s {
		case "$":
			v, _, err := readBulkString(r)
			if err != nil {
				return nil, err
			}
			res = append(res, v)
		case "+":
			fallthrough
		case "-":
			v, err := readPart(r)
			if err != nil {
				return nil, err
			}
			res = append(res, v)
		case ":":
			v, err := readPart(r)
			if err != nil {
				return nil, err
			}
			intv, _ := strconv.Atoi(v)
			res = append(res, intv)
		}
	}

	return res, nil
}
func readBulkString(r *RespReaderWriter) (interface{}, int, error) {
	s, err := readPart(r)
	if err != nil {
		return "", -1, err
	}
	length, _ := strconv.Atoi(s)
	if length == -1 { //null value
		r.readerWriter.ReadByte()
		r.readerWriter.ReadByte()
		return nil, length, nil
	}
	if length == 0 { //empty string
		r.readerWriter.ReadByte()
		r.readerWriter.ReadByte()
		r.readerWriter.ReadByte()
		r.readerWriter.ReadByte()
		return "", length, nil
	}
	p := make([]byte, length, length)
	i, er := r.readerWriter.Read(p)
	if er != nil {
		return "", -1, er
	}
	c := i
	for c < length {
		p1 := make([]byte, length-c, length-c)
		i, er = r.readerWriter.Read(p1)
		c = c + i
		for a := 0; a < i; a++ {
			p = append(p, p1[a])
		}
	}
	//skip CRLF
	r.readerWriter.ReadByte()
	r.readerWriter.ReadByte()
	return string(p), length, nil
}

//read line
func readPart(r *RespReaderWriter) (string, error) {
	s, er := r.readerWriter.ReadString(byte('\r'))
	if er != nil {
		return "", er
	}
	b, err := r.readerWriter.ReadByte()
	if err != nil {
		return "", nil
	}
	if b == byte('\n') {
		return s[0 : len(s)-1], nil
	} else {
		s1, err1 := readPart(r)
		if err1 != nil {
			return "", err1
		} else {
			s = s + string(b) + s1
			return s, nil
		}
	}
}
