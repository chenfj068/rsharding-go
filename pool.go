package main

import (
	"container/list"
	"fmt"
	"net"
	"sync"
	//	"time"
)

type ObjectPool struct {
	MaxCount        int
	IncrStep        int
	NewObjectFunc   NewPoolObject
	ObjectCheckFunc ChechPooledObject
	IdelList        *list.List
	BusyList        *list.List

	IdelArray []*PooledObject
	BusyArray []*PooledObject
	Lock      *sync.Mutex
}

type NewPoolObject func() (*PooledObject, error)
type ChechPooledObject func(obj *PooledObject) bool

type PooledObject struct {
	Value   interface{}
	Broken  bool
	ObjPool *ObjectPool
}

func (poolObj *PooledObject) Release() {
	poolObj.ObjPool.Return(poolObj)
}

func (p *ObjectPool) Init() {
	p.IdelArray = make([]*PooledObject, 0, 50)
	p.BusyArray = make([]*PooledObject, 0, 50)
	for i := 0; i < 4; i++ {
		obj, _ := p.NewObjectFunc()
		obj.ObjPool=p
		if obj != nil {
			p.IdelList.PushFront(obj)
		}
		//p.IdelArray = append(p.IdelArray, obj)
	}
}
func (p *ObjectPool) Borrow() (*PooledObject, error) {
	p.Lock.Lock()
	defer p.Lock.Unlock()
	if p.IdelList.Len() == 0 {
		//no idel object left
		//invoke NewPoolObject function to create new Conn
		for i := 0; i < 3; i++ {
			obj, er := p.NewObjectFunc()
			obj.ObjPool=p
			if er == nil {
				p.IdelList.PushBack(obj)
			}
		}
	}
	ele := p.IdelList.Front()
	p.BusyList.PushBack(ele.Value)
	p.IdelList.Remove(ele)
	return ele.Value.(*PooledObject), nil
	
}

func (p ObjectPool) Return(po *PooledObject) {
	p.Lock.Lock()
	defer p.Lock.Unlock()
	head := p.BusyList.Front()
	for head.Value != po {
		head = head.Next()
		if head == nil {
			break
		}
	}
	p.BusyList.Remove(head)
	if !po.Broken {
		p.IdelList.PushBack(head.Value)
	}else{
		po.Value.(*RespReaderWriter).Close()
	}
	fmt.Printf("Idel:%d,Busy:%d, %v\n", p.IdelList.Len(), p.BusyList.Len(),p)

}

func NewProxyClientPool(host string) *ObjectPool {
	pool := ObjectPool{}
	pool.BusyList = list.New()
	pool.IdelList = list.New()
	pool.Lock = &sync.Mutex{}
	pool.NewObjectFunc = func() (*PooledObject, error) {
		o := PooledObject{}
		conn, err := net.Dial("tcp", host)
		if err != nil {
			fmt.Printf("dial server failed:%v\n", err)
			return nil, err
		}
		rw := NewRespReadWriter(conn)
		o.Value = &rw
		o.Broken = false
		return &o, nil
	}
	pool.ObjectCheckFunc = CheckRespReadWriter
	pool.Init()
	return &pool
}

func CheckRespReadWriter(obj *PooledObject) bool {
	rw := obj.Value.(RespReaderWriter)
	err := rw.ProxyWrite("*1\r\n$4\r\nPING\r\n")
	if err != nil {
		return false
	}
	_, err = rw.ProxyRead()
	if err != nil {
		rw.Close()
		return false
	}
	return true
}
