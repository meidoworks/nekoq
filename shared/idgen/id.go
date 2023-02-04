package idgen

import (
	"encoding/binary"
	"encoding/hex"
	"errors"
	"runtime"
	"sync"
	"time"
)

var (
	emptyResult      = [2]int64{0, 0}
	emptyRangeResult []IdType
	maxValueInt32    = int32(0x7fffffff)

	ErrClockBackward = errors.New("clock backward")
)

const (
	startTimeMillis int64 = 1521639000000 // 20180321213000
)

type IdType [2]int64

func (i IdType) CompareTo(id2 IdType) int {
	if i[0] > id2[0] {
		return 1
	} else if i[0] < id2[0] {
		return -1
	} else if i[1] > id2[1] {
		return 1
	} else if i[1] < id2[1] {
		return -1
	} else {
		return 0
	}
}

func (i IdType) HexString() string {
	data := make([]byte, 16)
	binary.BigEndian.PutUint64(data[0:8], uint64(i[0]))
	binary.BigEndian.PutUint64(data[8:16], uint64(i[1]))
	return hex.EncodeToString(data)
}

func FromHexString(str string) (IdType, error) {
	b, err := hex.DecodeString(str)
	if err != nil {
		return IdType{}, err
	}
	if len(b) != 16 {
		return IdType{}, errors.New("raw string length is not 16")
	}
	r := IdType{}
	r[0] = int64(binary.BigEndian.Uint64(b[0:8]))
	r[1] = int64(binary.BigEndian.Uint64(b[8:16]))
	return r, nil
}

type IdGen struct {
	lock sync.Mutex

	time int64
	seq  int32

	nodeIdMask    int64
	elementIdMask int64
}

// NewIdGen creates ID Generator
// Format of Id: 48 bits time + 16 bits nodeId + 32 bits elementId + 32 bits inc
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
	return (n.Unix()*1000 + (int64(n.Nanosecond()%1000000000) / 1000000)) & 0x7fffffffffffffff
}

func (id *IdGen) NextN(cnt int) ([]IdType, error) {
	result := make([]IdType, cnt)
	timeInMills := id.getTimeMillis()
	id.lock.Lock()

	if timeInMills > id.time {
		// set seq to zero & return result
		id.time = timeInMills
		id.seq = int32(cnt - 1)
		id.lock.Unlock()
		return makeIdRange(timeInMills, id.nodeIdMask, id.elementIdMask, result, 0, int32(cnt-1)), nil
	} else if timeInMills == id.time {
		newSeq := id.seq + int32(cnt-1)
		// inc seq or wait until next time
		if newSeq < maxValueInt32 {
			// inc seq
			prevSeq := id.seq
			id.seq = newSeq
			id.lock.Unlock()
			return makeIdRange(timeInMills, id.nodeIdMask, id.elementIdMask, result, prevSeq, newSeq-1), nil
		} else {
			// wait until next time
			newTime := id.tillNextMillisecond(timeInMills)
			// success
			id.time = newTime
			id.seq = int32(cnt - 1)
			id.lock.Unlock()
			return makeIdRange(newTime, id.nodeIdMask, id.elementIdMask, result, 0, int32(cnt-1)), nil
		}
	} else {
		// error: clock backward
		id.lock.Unlock()
		return emptyRangeResult, ErrClockBackward
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
		if id.seq < maxValueInt32 {
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
		return emptyResult, ErrClockBackward
	}
}

func makeIdRange(time, nodeIdMask int64, elementId int64, result []IdType, seqStart int32, seqEnd int32) []IdType {
	for idx, start := 0, seqStart; start <= seqEnd; idx, start = idx+1, start+1 {
		l := elementId | (int64(start) & 0x00000000ffffffff)
		result[idx] = [2]int64{((time - startTimeMillis) << 16) | nodeIdMask, l}
	}
	return result
}

func makeId(time, nodeIdMask int64, elementId int64, seq int32) IdType {
	l := elementId | (int64(seq) & 0x00000000ffffffff)
	return [2]int64{((time - startTimeMillis) << 16) | nodeIdMask, l}
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
