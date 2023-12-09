package monitor

import (
	"fmt"
	"sync"
)

var (
	monitor map[string]*sync.WaitGroup
	lck     sync.RWMutex
)

func init() {
	monitor = make(map[string]*sync.WaitGroup)
}

func SubmitToWorkerQueue(addr any, fn func() error) {
	memoryLocation := fmt.Sprintf("%p", addr)
	lck.RLock()
	wg, ok := monitor[memoryLocation]
	lck.RUnlock()
	if !ok {
		wg = &sync.WaitGroup{}
		lck.Lock()
		monitor[memoryLocation] = wg
		lck.Unlock()
	}
	wg.Add(1)
	fmt.Println("DEBUG:", fmt.Sprintf("async function called for %s", memoryLocation))
	go func() {
		fn()
		wg.Done()
	}()
}

func WaitToFinish(addr any) {
	memoryLocation := fmt.Sprintf("%p", addr)
	fmt.Println("DEBUG:", fmt.Sprintf("waiting for %s to finish", memoryLocation))
	lck.RLock()
	wg := monitor[memoryLocation]
	lck.RUnlock()
	if wg == nil {
		return
	}
	wg.Wait()
	lck.Lock()
	delete(monitor, memoryLocation)
	lck.Unlock()
}
