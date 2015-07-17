package main

//presharding
//compute hash ,find target redis instance,construct new key with hashtag
import (
	"fmt"
	"hash/crc32"
	"net"
)

var (
	crcTable *crc32.Table
	target   RespReaderWriter

	shards Shards
)

func Hash(key string) uint32 {
	crc := crc32.New(crcTable)
	crc.Write([]byte(key))
	v := crc.Sum32()
	return v
}

type Shard struct {
	Host          string
	Slot0         uint32
	Slot1         uint32
	ShardRespPool *ObjectPool
}

type Shards struct {
	Slots        uint32
	ShardServers []Shard
}

func NewShards() Shards {
	sh := Shards{}
	s1 := Shard{}
	s1.Host = "54.223.201.162:6479"
	s1.Slot0 = 0
	s1.Slot1 = 512
	s2 := Shard{}
	s2.Host = "54.223.184.194:6479"
	s2.Slot0 = 512
	s2.Slot1 = 1024
	s1.ShardRespPool = NewProxyClientPool(s1.Host)
	s2.ShardRespPool = NewProxyClientPool(s2.Host)
	sh.ShardServers = []Shard{s1, s2}
	return sh

}
func (shard Shards) GetPooledObject(hash uint32) (*PooledObject, error) {
	for _, shard := range shard.ShardServers {
		if hash >= shard.Slot0 && hash < shard.Slot1 {
			fmt.Printf("get shard:%v\n", shard)
			obj, er := shard.ShardRespPool.Borrow()
			return obj, er
		}
	}
	return nil, nil
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
			o.Release()
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
				fmt.Println(key)
				s := "*" + fmt.Sprint(len(params)) + "\r\n"
				s += "$" + fmt.Sprint(len(cmd)) + "\r\n" + cmd + "\r\n"
				s += "$" + fmt.Sprint(len(key)) + "\r\n" + key + "\r\n"

				//here should checke nil and empty string
				for i := 2; i < len(params); i++ {
					s += "$" + fmt.Sprint(len(params[i].(string))) + "\r\n" + params[i].(string) + "\r\n"
				}
				if _, ok := mp[hash]; !ok {
					o1, er := shards.GetPooledObject(hash)
					if er != nil {
						client.Close()
						return
					}
					mp[hash] = o1
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
	shards = NewShards()
}
