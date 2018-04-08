package mq

import (
	"errors"
	"runtime"
	"sync"
	"time"
)

var (
	_EMPTY_RESULT         = [2]int64{0, 0}
	_EMPTY_RANGE_RESULT   = []IdType{}
	_ERROR_CLOCK_BACKWARD = errors.New("clock backward")
	_MAX_VALUE_INT32      = int32(0x7fffffff)
)

const (
	_START_TIME_MILLIS int64 = 1521639000000 // 20180321213000
)

type IdType [2]int64

func (this IdType) CompareTo(id2 IdType) int {
	if this[0] > id2[0] {
		return 1
	} else if this[0] < id2[0] {
		return -1
	} else if this[1] > id2[1] {
		return 1
	} else if this[1] < id2[1] {
		return -1
	} else {
		return 0
	}
}

type IdGen struct {
	lock sync.Mutex

	time int64
	seq  int32

	nodeIdMask    int64
	elementIdMask int64
}

// 48 bits time + 16 bits nodeId + 32 bits elementId + 32 bits inc
func NewIdGen(nodeId int16, elementId int32) *IdGen {
	return &IdGen{
		time:          0,
		seq:           0,
		nodeIdMask:    int64(nodeId) & 0x000000000000FFFF,
		elementIdMask: (int64(elementId) & 0x00000000FFFFFFFF) << 32,
	}
}

func (id *IdGen) getTimeMillis() int64 {
	n := time.Now()
	return int64((n.Unix()*1000 + (int64(n.Nanosecond()%1000000000) / 1000000)) & 0x7fffffffffffffff)
}

func (this *IdGen) NextN(cnt int) ([]IdType, error) {
	result := make([]IdType, cnt)
	timeInMills := this.getTimeMillis()
	this.lock.Lock()

	if timeInMills > this.time {
		// set seq to zero & return result
		this.time = timeInMills
		this.seq = int32(cnt - 1)
		this.lock.Unlock()
		return makeIdRange(timeInMills, this.nodeIdMask, this.elementIdMask, result, 0, int32(cnt-1)), nil
	} else if timeInMills == this.time {
		newSeq := this.seq + int32(cnt-1)
		// inc seq or wait until next time
		if newSeq < _MAX_VALUE_INT32 {
			// inc seq
			prevSeq := this.seq
			this.seq = newSeq
			this.lock.Unlock()
			return makeIdRange(timeInMills, this.nodeIdMask, this.elementIdMask, result, prevSeq, newSeq-1), nil
		} else {
			// wait until next time
			newtime := this.tillNextMillisecond(timeInMills)
			// success
			this.time = newtime
			this.seq = int32(cnt - 1)
			this.lock.Unlock()
			return makeIdRange(newtime, this.nodeIdMask, this.elementIdMask, result, 0, int32(cnt-1)), nil
		}
	} else {
		// error: clock backward
		this.lock.Unlock()
		return _EMPTY_RANGE_RESULT, _ERROR_CLOCK_BACKWARD
	}
}

func (id *IdGen) Next() (IdType, error) {
	timeInMills := id.getTimeMillis()
	id.lock.Lock()

	if timeInMills > id.time {
		// set seq to zero & return result
		id.time = timeInMills
		id.seq = 0
		id.lock.Unlock()
		return makeId(timeInMills, id.nodeIdMask, id.elementIdMask, 0), nil
	} else if timeInMills == id.time {
		// inc seq or wait until next time
		if id.seq < _MAX_VALUE_INT32 {
			// inc seq
			id.seq = id.seq + 1
			newseq := id.seq
			id.lock.Unlock()
			return makeId(timeInMills, id.nodeIdMask, id.elementIdMask, newseq), nil
		} else {
			// wait until next time
			newtime := id.tillNextMillisecond(timeInMills)
			// success
			id.time = newtime
			id.seq = 0
			id.lock.Unlock()
			return makeId(newtime, id.nodeIdMask, id.elementIdMask, 0), nil
		}
	} else {
		// error: clock backward
		id.lock.Unlock()
		return _EMPTY_RESULT, _ERROR_CLOCK_BACKWARD
	}
}

func makeIdRange(time, nodeIdMask int64, elementId int64, result []IdType, seqStart int32, seqEnd int32) []IdType {
	for idx, start := 0, seqStart; start <= seqEnd; idx, start = idx+1, start+1 {
		l := elementId | (int64(start) & 0x00000000ffffffff)
		result[idx] = [2]int64{((time - _START_TIME_MILLIS) << 16) | nodeIdMask, l}
	}
	return result
}

func makeId(time, nodeIdMask int64, elementId int64, seq int32) IdType {
	l := elementId | (int64(seq) & 0x00000000ffffffff)
	return [2]int64{((time - _START_TIME_MILLIS) << 16) | nodeIdMask, l}
}

func (id *IdGen) tillNextMillisecond(time int64) int64 {
	for {
		newtime := id.getTimeMillis()
		if newtime > time {
			return newtime
		}
		runtime.Gosched()
	}
}
