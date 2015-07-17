package main

//presharding
//compute hash ,find target redis instance,construct new key with hashtag
import (
	"fmt"
	"hash/crc32"
	"net"
	//	"time"
)

var (
	crcTable *crc32.Table
	target   RespReaderWriter
	pool     ObjectPool
)

func Hash(key string) uint32 {
	crc := crc32.New(crcTable)
	crc.Write([]byte(key))
	v := crc.Sum32()
	return v
}

type Sharding struct {
	Slots   int
	Servers []Server
}
type Server struct {
	Host  string
	Slot0 int
	Slot1 int
}

type ServerManager struct {
}

func GetReadWriter(hash int) RespReaderWriter {
	pool.Borrow()
	return target
}

func GetPooledReadWriter(hash int) *PooledObject {
	rwobj, err := pool.Borrow()
	if err != nil {
		fmt.Println("wocao")
	}
	return rwobj
}
func HandleConn(conn net.Conn) {
	//	timeout := time.Now()
	//	timeout.Add(10 *time.Minute )
	//	conn.SetReadDeadline(timeout)
	client := NewRespReadWriter(conn)
	cch := make(chan int) //close flag chan
	dch := client.LoopRead(cch)
	mp := make(map[uint32]*PooledObject)
	var lastHash uint32
	defer func() {
		o := mp[lastHash]
		if o != nil {
			o.Broken = true
			rw := o.Value.(RespReaderWriter)
			rw.Close()
		}
		for _, o := range mp {
			pool.Return(o)
		}
		client.Close()

	}()
	for {
		select {
		case <-cch:
			return
		case params := <-dch:
			if len(params) == 0 {
				continue
			}
			cmd := params[0].(string)
			if cmd == "PING" {
				client.ProxyWrite("+PONG\r\n")
			} else {
				key := params[1].(string)
				hash := Hash(key) % uint32(1024)
				lastHash = hash
				key = fmt.Sprint(hash) + "_" + key
				s := "*" + fmt.Sprint(len(params)) + "\r\n"
				s += "$" + fmt.Sprint(len(cmd)) + "\r\n" + cmd + "\r\n"
				s += "$" + fmt.Sprint(len(key)) + "\r\n" + key + "\r\n"

				//here should checke nil and empty string
				for i := 2; i < len(params); i++ {
					s += "$" + fmt.Sprint(len(params[i].(string))) + "\r\n" + params[i].(string) + "\r\n"
				}
				if o, ok := mp[hash]; !ok {
					o = GetPooledReadWriter(int(hash))
					mp[hash] = o
				}
				server := mp[hash]
				ss := server.Value.(RespReaderWriter)
				err := ss.ProxyWrite(s)
				if err != nil {
					fmt.Printf("backend server write error:%v\n", err)
					return
				}
				resp, err := ss.ProxyRead()
				
				if err != nil {
					fmt.Printf("backend server read error:%v\n", err)
					return
				}

				err = client.ProxyWrite(resp)

				if err != nil {
					fmt.Println("clent error")
				}
			}

		}

	}

}

func init() {
	//	conn, err := net.Dial("tcp", "54.223.201.162:6479")
	//	if err != nil {
	//		fmt.Printf("%v\n", err)
	//	}
	pool = NewProxyClientPool("54.223.201.162:6479")
	//starget = NewRespReadWriter(conn)
}
