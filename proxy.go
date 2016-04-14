package main

//presharding
//compute hash ,find target redis instance,construct new key with hashtag
import (
	"errors"
	"fmt"
	"hash/crc32"
	"net"
	"os"
	"encoding/json"
//	"strconv"
	"io/ioutil"
)

var (
	shards   Shards
)



type Shard struct {
	Host          string  	`json:"host"`
	Slot0         uint32  	`json:"slot0"`
	Slot1         uint32  	`json:"slot1"`
	ShardRespPool *ObjectPool `json:"-"`
}


type Shards struct {
	Slots        uint32  `json:"slots"`
	ShardServers []Shard `json:"servers"`
	}
//just for config parsing
type ConfigShards struct{
	Slots        int  `json:"slots"`
	ShardServers []struct {
		Host   string  `json:"host"`
		Slot0  int  `json:"slot0"`
		Slot1  int  `json:"slot1"`
		} `json:"shardServers"`
}


func (shards Shards) GetPooledObject(hash uint32) (obj *PooledObject, shard Shard, err error) {
	for _, shard := range shards.ShardServers {
		if hash >= shard.Slot0 && hash < shard.Slot1 {
			fmt.Printf("get shard:%v\n", shard)
			obj, er := shard.ShardRespPool.Borrow()
			return obj, shard, er
		}
	}
	return nil, Shard{}, nil
}

func (shards Shards) GetShard(hash uint32) (Shard, error) {
	for i, shard := range shards.ShardServers {
		if hash >= shard.Slot0 && hash < shard.Slot1 {
			return shards.ShardServers[i], nil
		}
	}
	fmt.Println("no shard found")
	return Shard{}, errors.New("no shard found")
}

func HandleConn(conn net.Conn) {
	client := NewRespReadWriter(conn)
	cch := make(chan int) //close flag chan
	dch := client.LoopRead2(cch)
	shardMap := make(map[Shard]*PooledObject)
	var lastHash uint32
	defer func() {
		for _, o := range shardMap {
				serverConn:=o.Value.(*RespReaderWriter)
				s:="*1\r\n$4\r\nQUIT\r\n"
				err:=serverConn.ProxyWrite(s)
				if err!=nil{
					fmt.Printf("send quit error %v\n",err)
				}
				o.Broken = true //set last one to broken
				fmt.Printf("set conn to broken,lastHash :%d\n", lastHash)
			o.Release()
			

		}
		client.Close()
	}()
	for {
		select {
		case <-cch:
			return
		case d := <-dch:
			if(d.Value==nil&&d.Array==nil||len(d.Array)==0){
				continue
			}
			cmd := string(d.Array[0].Value)
//			fmt.Println("command "+cmd)
			if cmd == "PING" {
				client.ProxyWrite("+PONG\r\n")
			} else if cmd == "QUIT" {
				client.ProxyWrite("+OK\r\n")
				client.Close()
				return
			} else if cmd == "ECHO" {
				s:=fmt.Sprintf("$%d%s\r\n",len(d.Array[1].Value),string(d.Array[1].Value))
				client.ProxyWrite(s)
			} else {
//				if len(params) < 2 {
//					fmt.Println(params)
//				}
				key := d.Array[1].Value
				hash := crc32.ChecksumIEEE(key)% uint32(1024)
//				hash := Hash(key) % uint32(1024)
				lastHash = hash
//				key = fmt.Sprint(hash) + "_" + key
				//fmt.Println(key)
//				s := "*" + fmt.Sprint(len(params)) + "\r\n"
//				s += "$" + fmt.Sprint(len(cmd)) + "\r\n" + cmd + "\r\n"
//				s += "$" + fmt.Sprint(len(key)) + "\r\n" + key + "\r\n"
				
				//here should check nil and empty string
//				for i := 2; i < len(params); i++ {
//					s += "$" + fmt.Sprint(len(params[i].(string))) + "\r\n" + params[i].(string) + "\r\n"
//				}
				shard, err := shards.GetShard(hash)
				if err != nil {
					//handle error
					client.Close()
					return
				}
				if ob, ok := shardMap[shard]; !ok {
					fmt.Println("get new shard")
					ob, err = shard.ShardRespPool.Borrow()
					if err != nil {
						client.Close()
						return
					} else if ob != nil {
						shardMap[shard] = ob
					}
				}

				server := shardMap[shard]
				ss := server.Value.(*RespReaderWriter)
				err = ss.encoder.Encode(d,true)
//				fmt.Println("cmd sent")
				if err != nil {
					fmt.Printf("backend server write error:%v\n", err)
					server.Broken = true
					server.Release()
					return
				}
				
//				fmt.Println("cmd sent")
				resp, err := ss.decoder.decodeResp(0)
				if err != nil {
					fmt.Printf("backend server read error:%v\n", err)
					server.Broken = true
					return
				}
//				fmt.Println("decode resp")
				err = client.encoder.Encode(resp,true)//.ProxyWrite(resp)
				if err != nil {
					fmt.Println("clent error")
					server.Broken = true
					server.Release()
				}
			}

		}

	}

}

func init() {
	f,er:=os.Open("sharding.json")
	if er!=nil{
		s:=fmt.Sprintf("%v",er)
		panic(s)
	}
	b,_:=ioutil.ReadAll(f)
	confshards := ConfigShards{}
	json.Unmarshal(b,&confshards)
	shards=Shards{}
	shArray:=make([]Shard,0,len(confshards.ShardServers))
	shards.Slots=uint32(confshards.Slots)
	for _,shard:=range confshards.ShardServers{
		sh:=Shard{}
		sh.Host=shard.Host
		sh.Slot0=uint32(shard.Slot0)
		sh.Slot1=uint32(shard.Slot1)
		sh.ShardRespPool=NewProxyClientPool(sh.Host)
		shArray=append(shArray,sh)
		fmt.Printf("get shard info %v \n",sh)
	}
	shards.ShardServers=shArray
}
