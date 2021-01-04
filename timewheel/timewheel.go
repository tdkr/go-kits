package timewheel

import (
	"container/list"
	"sync"
	"time"
)

type Handler interface {
	Handle(args ...interface{})
}

type HandlerFunc func(args ...interface{})

func (h HandlerFunc) Handle(args ...interface{}) {
	h(args...)
}

type TimeWheel struct {
	sync.Mutex
	ticker   *time.Ticker
	stopCh   chan interface{}
	curPos   int
	interval time.Duration
	slotNum  int
	handler  Handler
	slotMap  map[int]*slot
}

type slot struct {
	lock sync.Mutex
	list *list.List
}

type Timer struct {
	circle  int
	data    []interface{}
	element *list.Element
	slot    *slot
}

func (t *Timer) Stop() {
	t.slot.lock.Lock()
	defer t.slot.lock.Unlock()
	t.slot.list.Remove(t.element)
}

func New(interval time.Duration, slotNum int, handler Handler) *TimeWheel {
	if interval <= 0 || slotNum <= 0 || handler == nil {
		return nil
	}
	t := &TimeWheel{
		interval: interval,
		slotNum:  slotNum,
		handler:  handler,
		curPos:   1,
		stopCh:   make(chan interface{}, 0),
	}
	t.slotMap = make(map[int]*slot, slotNum)
	for i := 1; i <= slotNum; i++ {
		t.slotMap[i] = &slot{
			list: list.New(),
		}
	}
	return t
}

func (tw *TimeWheel) Run() {
	tw.Lock()
	defer tw.Unlock()
	tw.ticker = time.NewTicker(tw.interval)
	for {
		select {
		case <-tw.ticker.C:
			tw.onTick()
		case <-tw.stopCh:
			tw.ticker.Stop()
			return
		}
	}
}

func (tw *TimeWheel) Stop() {
	tw.stopCh <- struct{}{}
}

func (tw *TimeWheel) NewTimer(delay time.Duration, data ...interface{}) *Timer {
	slot, circle := tw.getTimeSlotAndCircle(delay)
	slot.lock.Lock()
	defer slot.lock.Unlock()
	t := &Timer{
		slot:   slot,
		circle: circle,
		data:   data,
	}
	t.element = slot.list.PushBack(t)
	return t
}

func (tw *TimeWheel) getTimeSlotAndCircle(delay time.Duration) (*slot, int) {
	circle := int(delay) / int(tw.interval) / tw.slotNum
	pos := (tw.curPos + int(delay/tw.interval)) % tw.slotNum
	return tw.slotMap[pos], circle
}

func (tw *TimeWheel) onTick() {
	slot := tw.slotMap[tw.curPos]
	slot.lock.Lock()
	defer slot.lock.Unlock()
	ptr := slot.list.Front()
	for ptr != nil {
		t := ptr.Value.(*Timer)
		if t.circle > 0 {
			t.circle--
			continue
		}
		go tw.handler.Handle(t.data...)
		next := ptr.Next()
		slot.list.Remove(ptr)
		ptr = next
	}
	if tw.curPos < tw.slotNum {
		tw.curPos++
	} else {
		tw.curPos = 1
	}
}
