package main

import (
	"container/list"
	"fmt"
	"net"
	"sync"
)

type ObjectPool struct {
	IdelCount     int
	TotalCount    int
	MaxCount      int
	IncrStep      int
	NewObjectFunc NewPoolObject
	IdelList      *list.List
	BusyList      *list.List
	Lock          sync.Mutex
}

type NewPoolObject func() PooledObject

type PooledObject struct {
	Value  interface{}
	Broken bool
}

func (p *ObjectPool) Init(){
	p.BusyList=list.New()
	p.IdelList=list.New()
	}
func (p *ObjectPool) Borrow() (PooledObject, error) {
	p.Lock.Lock()
	defer p.Lock.Unlock()
	if p.IdelList.Len() == 0 {
		//no idel object left
		//invoke NewPoolObject function to create new Conn
		for i := 0; i < 3; i++ {
			obj := p.NewObjectFunc()
			p.IdelList.PushBack(obj)
		}
	}
	ele := p.IdelList.Front()
	p.BusyList.PushBack(ele.Value)
	p.IdelList.Remove(ele)
	return ele.Value.(PooledObject), nil

}

func (p *ObjectPool) Return(po PooledObject) {
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

}

func NewProxyClientPool(host string) ObjectPool {
	pool := ObjectPool{}
	pool.Init()
	pool.NewObjectFunc = func() PooledObject {
		o := PooledObject{}
		conn, err := net.Dial("tcp", host)
		if err != nil {
			fmt.Printf("%v\n", err)
		}
		rw := NewRespReadWriter(conn)
		o.Value = rw
		o.Broken = false
		return o
	}
	return pool
}
