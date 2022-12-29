package impl

import (
	"bufio"
	"errors"
	"io"
)

const (
	LOG_ENTRY_TYPE_RECORD_START   byte = 1
	LOG_ENTRY_TYPE_RECORD_COMMIT  byte = 2
	LOG_ENTRY_TYPE_RECORD_ABORT   byte = 3
	LOG_ENTRY_TYPE_RECORD_CONTENT byte = 4
)

type LogRecord struct {
	Type      byte
	SubType   byte
	Reserved2 byte
	Reserved3 byte
	LogId     [2]int64
	Data      []byte
}

func Int32ToBytesBE(i int32) []byte {
	panic("implement me")
}

func Int64ToBytesBE(i int64) []byte {
	panic("implement me")
}

func BytesBigEndianToInt32(b []byte) (int32, error) {
	panic("implement me")
}

func BytesBigEndianToInt64(b []byte) (int64, error) {
	panic("implement me")
}

func (this *LogRecord) WriteTo(w *bufio.Writer) error {
	l := 4 + 16 + len(this.Data)
	_, err := w.Write(Int32ToBytesBE(int32(l)))
	if err != nil {
		return err
	}
	b := [4]byte{this.Type, this.SubType, this.Reserved2, this.Reserved3}
	_, err = w.Write(b[:])
	if err != nil {
		return err
	}
	_, err = w.Write(Int64ToBytesBE(this.LogId[0]))
	if err != nil {
		return err
	}
	_, err = w.Write(Int64ToBytesBE(this.LogId[1]))
	if err != nil {
		return err
	}
	if len(this.Data) > 0 {
		_, err = w.Write(this.Data)
		if err != nil {
			return err
		}
	}
	return nil
}

func FromData(reader io.Reader) (*LogRecord, error, bool) {
	h := [8]byte{}

	n, err := io.ReadFull(reader, h[:4])
	if err != nil {
		if n == 0 && err == io.EOF {
			return nil, err, true // EOF, normally finish
		}
		return nil, err, false
	}
	length, err := BytesBigEndianToInt32(h[:4])
	if err != nil {
		return nil, err, false
	}
	bodyLength := length - 4 - 16
	if bodyLength < 0 {
		return nil, errors.New("calculated body length < 0"), false
	}

	record := &LogRecord{}

	_, err = io.ReadFull(reader, h[:4])
	if err != nil {
		return nil, err, false
	}
	record.Type = h[0]
	record.SubType = h[1]
	record.Reserved2 = h[2]
	record.Reserved3 = h[3]

	_, err = io.ReadFull(reader, h[:])
	if err != nil {
		return nil, err, false
	}
	i, err := BytesBigEndianToInt64(h[:])
	if err != nil {
		return nil, err, false
	}
	record.LogId[0] = i
	_, err = io.ReadFull(reader, h[:])
	if err != nil {
		return nil, err, false
	}
	i, err = BytesBigEndianToInt64(h[:])
	if err != nil {
		return nil, err, false
	}
	record.LogId[1] = i

	if bodyLength > 0 {
		data := make([]byte, bodyLength)
		_, err = io.ReadFull(reader, data)
		if err != nil {
			return nil, err, false
		}
		record.Data = data
	}

	return record, nil, true
}
