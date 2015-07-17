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
	Value  interface{}
	Broken bool
}

func (p *ObjectPool) Init() {
	p.IdelArray = make([]*PooledObject, 0, 50)
	p.BusyArray = make([]*PooledObject, 0, 50)
	for i := 0; i < 4; i++ {
		obj, _ := p.NewObjectFunc()
		if obj != nil {
			p.IdelList.PushFront(obj)
		}
		//p.IdelArray = append(p.IdelArray, obj)
	}
}
func (p ObjectPool) Borrow() (*PooledObject, error) {
	p.Lock.Lock()
	defer p.Lock.Unlock()
	if p.IdelList.Len() == 0 {
		//no idel object left
		//invoke NewPoolObject function to create new Conn
		for i := 0; i < 3; i++ {
			obj, er := p.NewObjectFunc()
			if er == nil {
				p.IdelList.PushBack(obj)
			}
		}
	}
	ele := p.IdelList.Front()
	p.BusyList.PushBack(ele.Value)
	p.IdelList.Remove(ele)
	return ele.Value.(*PooledObject), nil
	//	if len(p.IdelArray) == 0 {
	//		//no idel object left
	//		//invoke NewPoolObject function to create new Conn
	//		for i := 0; i < 3; i++ {
	//			obj, _ := p.NewObjectFunc()
	//			if obj != nil {
	//				p.IdelArray = append(p.IdelArray, obj)
	//			}
	//		}
	//	}
	//	for i := 0; i < len(p.IdelArray); i++ {
	//		obj := pool.IdelArray[i]
	//		if ok := pool.ObjectCheckFunc(obj); ok {
	//			pool.BusyArray = append(pool.BusyArray, obj)
	//			if i < len(p.IdelArray)-1 {
	//				pool.IdelArray = pool.IdelArray[i+1:]
	//			} else {
	//				pool.IdelArray = make([]*PooledObject, 0, 50)
	//			}
	//			return obj, nil
	//		}
	//	}
	//	return nil, nil

}

func (p ObjectPool) Return(po *PooledObject) {
	//	p.Lock.Lock()
	//	defer p.Lock.Unlock()
	//	for i := 0; i < len(p.BusyArray); i++ {
	//		if po == p.BusyArray[i] {
	//			a := make([]*PooledObject, 0, 50)
	//			a = append(p.BusyArray[0:i])
	//			if i+1 < len(p.BusyArray) {
	//				a = append(p.BusyArray[i+1:])
	//			}
	//			p.BusyArray = a
	//			p.IdelArray=append(p.IdelArray,po)
	//		}
	//	}

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
	}
	fmt.Printf("Idel:%d,Busy:%d\n", p.IdelList.Len(), p.BusyList.Len())

}

func NewProxyClientPool(host string) ObjectPool {
	pool := ObjectPool{}
	//var mutex = &sync.Mutex{}
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
		o.Value = rw
		o.Broken = false
		return &o, nil
	}
	pool.ObjectCheckFunc = CheckRespReadWriter
	pool.Init()
	return pool
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
