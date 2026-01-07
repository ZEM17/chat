package timewheel

import (
	"container/list"
	"sync"
	"time"
)

// Task wrapping the callback
type Task struct {
	delay  time.Duration
	circle int
	key    string
	job    func()
	slot   int // Added to track which slot this task belongs to
}

// TimeWheel implementation
type TimeWheel struct {
	interval time.Duration // internal ticker duration
	ticker   *time.Ticker
	slots    []*list.List             // circular slots
	timer    map[string]*list.Element // map key to list element
	current  int                      // current slot index
	slotNum  int                      // total slots
	mu       sync.Mutex
	stopCh   chan struct{}
}

// New creates a time wheel
func New(interval time.Duration, slotNum int) *TimeWheel {
	if interval <= 0 || slotNum <= 0 {
		return nil
	}
	tw := &TimeWheel{
		interval: interval,
		slots:    make([]*list.List, slotNum),
		timer:    make(map[string]*list.Element),
		current:  0,
		slotNum:  slotNum,
		stopCh:   make(chan struct{}),
	}
	for i := 0; i < slotNum; i++ {
		tw.slots[i] = list.New()
	}
	return tw
}

// Start starts the ticker
func (tw *TimeWheel) Start() {
	tw.ticker = time.NewTicker(tw.interval)
	go tw.run()
}

// Stop stops the ticker
func (tw *TimeWheel) Stop() {
	tw.stopCh <- struct{}{}
}

func (tw *TimeWheel) run() {
	for {
		select {
		case <-tw.ticker.C:
			tw.tickHandler()
		case <-tw.stopCh:
			tw.ticker.Stop()
			return
		}
	}
}

func (tw *TimeWheel) tickHandler() {
	tw.mu.Lock()
	l := tw.slots[tw.current]
	tw.current = (tw.current + 1) % tw.slotNum
	tw.mu.Unlock()

	tw.scanAndRunTask(l)
}

func (tw *TimeWheel) scanAndRunTask(l *list.List) {
	tw.mu.Lock()
	defer tw.mu.Unlock()

	for e := l.Front(); e != nil; {
		task := e.Value.(*Task)
		if task.circle > 0 {
			task.circle--
			e = e.Next()
			continue
		}

		go task.job() // Exec

		next := e.Next()
		l.Remove(e)
		if task.key != "" {
			delete(tw.timer, task.key)
		}
		e = next
	}
}

// Add adds a task. If key exists, it updates (removes old, adds new).
func (tw *TimeWheel) Add(delay time.Duration, key string, job func()) {
	if delay < 0 {
		return
	}
	tw.mu.Lock()
	defer tw.mu.Unlock()

	// 1. Remove existing task if key is provided
	if key != "" {
		if ele, ok := tw.timer[key]; ok {
			existTask := ele.Value.(*Task)
			// Remove from the old slot
			tw.slots[existTask.slot].Remove(ele)
			delete(tw.timer, key)
		}
	}

	// 2. Add new task
	pos := (tw.current + int(delay/tw.interval)) % tw.slotNum
	circle := int(delay/tw.interval) / tw.slotNum

	task := &Task{
		delay:  delay,
		circle: circle,
		key:    key,
		job:    job,
		slot:   pos, // Record the slot
	}

	l := tw.slots[pos]
	ele := l.PushBack(task)

	if key != "" {
		tw.timer[key] = ele
	}
}

// Remove removes a task by key
func (tw *TimeWheel) Remove(key string) {
	tw.mu.Lock()
	defer tw.mu.Unlock()

	if ele, ok := tw.timer[key]; ok {
		task := ele.Value.(*Task)
		tw.slots[task.slot].Remove(ele)
		delete(tw.timer, key)
	}
}
