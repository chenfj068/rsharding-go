package main

import (
	"fmt"
	"net"
)

func main() {
	conn, err := net.Dial("tcp", "54.223.201.162:6479")
	if err != nil {
		fmt.Printf("%v", err)
	}
	readWriter := NewRespReadWriter(conn)
	//	readWriter.Write("set", "hxl", "are you ok,\r\n 👌")
	//	r, er := readWriter.Read()
	//	if er != nil {
	//		fmt.Printf("%v", er)
	//	}
	//	fmt.Println(r)
	//	readWriter.Write("get", "hxl")
	//	r, _ = readWriter.Read()
	//	fmt.Println(r)
	//	hset(readWriter)
	zadd(readWriter)
}

func hset(rw RespReaderWriter) {
	rw.Write("hset", "HXL", "hxl", "")
	s, _ := rw.Read()
	fmt.Println(s)
	rw.Write("hget", "HXL", "hxl")
	s, _ = rw.Read()
	fmt.Printf("array lengt:%d\n", len(s))
	if s[0] == "" {
		fmt.Println("empty response ok")
	}
	fmt.Println(s)
}

func zadd(rw RespReaderWriter) {
	rw.Write("zadd", "lxy", 20, "hxl2")
	s, _ := rw.Read()
	fmt.Println(s)
	rw.Write("ZRANGEBYSCORE", "lxy", 1, 120)
	s, _ = rw.Read()
	fmt.Println(s)
}

func zrangeWithScore() {

}
