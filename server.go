package main

import (
	"net"
)

type ProxyServer struct {
	ListenHost string
}

func NewProxyServer(listen_host string) ProxyServer {
	return ProxyServer{ListenHost: listen_host}
}

func (s ProxyServer) Start() error {
	ln, err := net.Listen("tcp", s.ListenHost)
	if err != nil {
		return err
	}
	go func() {
		for {
			conn, er := ln.Accept()
			if er != nil {
				//
			}
//			out, _ := net.Dial("tcp", s.TargetHost)
//			go procRequest(conn, out)
			go HandleConn(conn)

		}

	}()

	return nil
}

func procRequest(conn net.Conn, outConn net.Conn) {
	readWriter := NewRespReadWriter(conn)
	out := NewRespReadWriter(outConn)
	for {
		str, err := readWriter.ProxyRead()
		if err != nil {
			return
		}
		out.ProxyWrite(str)
		resp, _ := out.ProxyRead()
		readWriter.ProxyWrite(resp)
	}

}
