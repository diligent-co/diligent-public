package general

import (
	"log/slog"
	"sync"
	"time"
)

type IimeBufferable interface {
	GetId() string
	GetTimestamp() time.Time
}

type TimedBuffer[T IimeBufferable] struct {
	buffer []T
	bufferSize int
	idToIndex map[string]int
	mutex  sync.RWMutex
}

func NewTimedBuffer[T IimeBufferable](bufferSize int) *TimedBuffer[T] {
	return &TimedBuffer[T]{
		buffer: make([]T, 0),
		bufferSize: bufferSize,
		idToIndex: make(map[string]int),
	}
}

func (tb *TimedBuffer[T]) AddElement(element T) {
	startTime := time.Now()
	defer func() {
		slog.Debug("Time to add element to timed buffer", "time", time.Since(startTime))
	}()
	tb.mutex.Lock()
	defer tb.mutex.Unlock()
	if len(tb.buffer) >= tb.bufferSize {
		oldestElement := tb.buffer[0]
		delete(tb.idToIndex, oldestElement.GetId())
		tb.buffer = tb.buffer[1:]
	}
	// check if timestamp is after last element, if so, add it
	if len(tb.buffer) == 0 {
		tb.buffer = append(tb.buffer, element)
		tb.idToIndex[element.GetId()] = 0
		return
	}

	// Search backwards through buffer to find insertion point
	for i := len(tb.buffer) - 1; i >= 0; i-- {
		if element.GetTimestamp().Equal(tb.buffer[i].GetTimestamp()) || element.GetTimestamp().After(tb.buffer[i].GetTimestamp()) {
			tb.buffer = append(tb.buffer[:i+1], append([]T{element}, tb.buffer[i+1:]...)...)
			tb.idToIndex[element.GetId()] = i + 1
			return
		}
	}
}

func (tb *TimedBuffer[T]) GetById(id string) (T, bool) {
	tb.mutex.RLock()
	defer tb.mutex.RUnlock()
	index, ok := tb.idToIndex[id]
	if !ok {
		var zeroValue T
		return zeroValue, false
	}
	return tb.buffer[index], true
}

func (tb *TimedBuffer[T]) GetFirstElementAfterId(id string) (T, bool) {
	tb.mutex.RLock()
	defer tb.mutex.RUnlock()
	index, ok := tb.idToIndex[id]
	if !ok {
		var zeroValue T
		return zeroValue, false
	}
	if index == len(tb.buffer) - 1 {
		var zeroValue T
		return zeroValue, false
	}
	return tb.buffer[index+1], true
}

func (tb *TimedBuffer[T]) GetFirstElementBeforeId(id string) (T, bool) {
	tb.mutex.RLock()
	defer tb.mutex.RUnlock()
	index, ok := tb.idToIndex[id]
	if !ok {
		var zeroValue T
		return zeroValue, false
	}
	if index == 0 {
		var zeroValue T
		return zeroValue, false
	}
	return tb.buffer[index-1], true
}

func (tb *TimedBuffer[T]) GetElementsOlderThan(time time.Time) []T {
	// returns all elements older than the given time, ordered from oldest to newest
	tb.mutex.RLock()
	defer tb.mutex.RUnlock()
	
	// Binary search to find first element newer than time
	left, right := 0, len(tb.buffer)-1
	for left <= right {
		mid := (left + right) / 2
		if tb.buffer[mid].GetTimestamp().Before(time) {
			left = mid + 1
		} else {
			right = mid - 1
		}
	}
	
	// All elements before 'left' are older than time
	if left == 0 {
		return []T{}
	}
	return tb.buffer[:left]
}

func (tb *TimedBuffer[T]) GetElementsNewerThan(time time.Time) []T {
	// returns all elements newer than the given time, ordered from newest to oldest
	tb.mutex.RLock()
	defer tb.mutex.RUnlock()
	
	// Binary search to find first element newer than time
	left, right := 0, len(tb.buffer)-1
	for left <= right {
		mid := (left + right) / 2
		if tb.buffer[mid].GetTimestamp().After(time) {
			right = mid - 1
		} else {
			left = mid + 1
		}
	}
	
	// All elements from 'left' onwards are newer than time
	if left >= len(tb.buffer) {
		return []T{}
	}
	return tb.buffer[left:]
}

func (tb *TimedBuffer[T]) GetElementClosestTo(time time.Time) (T, bool) {
	// returns the element closest to the given time, either before or after
	tb.mutex.RLock()
	defer tb.mutex.RUnlock()
	
	var zeroValue T
	if len(tb.buffer) == 0 {
		return zeroValue, false
	}
	
	// Binary search to find closest element
	left, right := 0, len(tb.buffer)-1
	for left <= right {
		mid := (left + right) / 2
		if tb.buffer[mid].GetTimestamp().Equal(time) {
			return tb.buffer[mid], true
		}
		if tb.buffer[mid].GetTimestamp().Before(time) {
			left = mid + 1
		} else {
			right = mid - 1
		}
	}
	
	// At this point, right points to the closest element before time
	// and left points to the closest element after time
	// Compare these two elements to find the closest one
	if right < 0 {
		return tb.buffer[0], true
	}
	if left >= len(tb.buffer) {
		return tb.buffer[len(tb.buffer)-1], true
	}
	
	beforeDiff := time.Sub(tb.buffer[right].GetTimestamp())
	afterDiff := tb.buffer[left].GetTimestamp().Sub(time)
	if beforeDiff < afterDiff {
		return tb.buffer[right], true
	}
	return tb.buffer[left], true
}

func (tb *TimedBuffer[T]) GetAllElements() []T {
	tb.mutex.RLock()
	defer tb.mutex.RUnlock()
	return tb.buffer
}

func (tb *TimedBuffer[T]) GetLatestElement() (T, bool) {
	tb.mutex.RLock()
	defer tb.mutex.RUnlock()

	var zeroValue T
	if len(tb.buffer) == 0 {
		return zeroValue, false
	}
	return tb.buffer[len(tb.buffer)-1], true
}

func (tb *TimedBuffer[T]) GetEarliestElement() (T, bool) {
	tb.mutex.RLock()
	defer tb.mutex.RUnlock()

	var zeroValue T
	if len(tb.buffer) == 0 {
		return zeroValue, false
	}
	return tb.buffer[0], true
}

func (tb *TimedBuffer[T]) GetSize() int {
	tb.mutex.RLock()
	defer tb.mutex.RUnlock()
	return len(tb.buffer)
}

func (tb *TimedBuffer[T]) GetCapacity() int {
	tb.mutex.RLock()
	defer tb.mutex.RUnlock()
	return tb.bufferSize
}

func (tb *TimedBuffer[T]) GetLoad() float64 {
	tb.mutex.RLock()
	defer tb.mutex.RUnlock()
	return 1.0*float64(len(tb.buffer))/float64(tb.bufferSize)
}

func (tb *TimedBuffer[T]) GetIsFull() bool {
	tb.mutex.RLock()
	defer tb.mutex.RUnlock()
	return len(tb.buffer) >= tb.bufferSize
}


