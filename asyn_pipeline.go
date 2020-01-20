package asyn_pipeline

import (
	"errors"
	"log"
	"runtime"
	"sync"
)

var (
	ErrCacheChanFull = errors.New("cache chan full")
)

// Worker async save data by chan.
type Worker struct {
	workerChan  chan func()
	workerCount int
	waiter      sync.WaitGroup
}

func New(count, length int) *Worker {
	if count <= 0 {
		count = 1
	}
	c := &Worker{
		workerChan:  make(chan func(), length),
		workerCount: count,
	}
	c.waiter.Add(count)
	for i := 0; i < count; i++ {
		go c.consume()
	}
	return c
}

func (c *Worker) consume() {
	defer c.waiter.Done()
	for {
		f := <-c.workerChan
		if f == nil {
			return
		}
		wrapFunc(f)()
	}
}

func wrapFunc(f func()) (res func()) {
	res = func() {
		defer func() {
			if r := recover(); r != nil {
				buf := make([]byte, 64*1024)
				buf = buf[:runtime.Stack(buf, false)]
				log.Println(buf)
			}
		}()
		f()
	}
	return
}

// Do save a callback cache func.
func (c *Worker) Do(f func()) (err error) {
	if f == nil {
		return
	}
	select {
	case c.workerChan <- f:
	default:
		err = ErrCacheChanFull
	}
	return
}

// Close close cache
func (c *Worker) Close() (err error) {
	for i := 0; i < c.workerCount; i++ {
		c.workerChan <- nil
	}
	c.waiter.Wait()
	return
}
