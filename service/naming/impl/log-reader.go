package impl

import (
	"bufio"
	"errors"
	"io"
	"os"
)

type ProcessCallback interface {
	OnLogRecord(*LogRecord) error
	OnError(error)
	OnFinish()
}

func ProcessLogFile(path string, callback ProcessCallback) error {
	f, err := os.OpenFile(path, os.O_RDONLY, 0666)
	if err != nil {
		return err
	}
	file := f
	defer file.Close()
	var r = bufio.NewReader(f)
	header := [8]byte{}
	_, err = io.ReadFull(r, header[:])
	if err != nil {
		return err
	}
	err = checkFileHeader(header)
	if err != nil {
		return err
	}
	for {
		record, err, ok := FromData(r)
		if err != nil && err != io.EOF {
			callback.OnError(err)
			return err
		} else if err == io.EOF {
			if ok {
				callback.OnFinish()
				return nil
			} else {
				callback.OnError(err)
				return err
			}
		} else {
			err := callback.OnLogRecord(record)
			if err != nil {
				return err
			}
		}
	}
}

func checkFileHeader(header [8]byte) error {
	if header[0] != 0x77 || header[1] != 0x88 {
		return errors.New("magic number unknown. may not be log file.")
	}
	if header[2] != 0x01 || header[3] != 0x01 {
		return errors.New("version unsupported.")
	}
	if header[4] != 0x01 || header[5] != 0x01 {
		return errors.New("file type is not log file.")
	}
	return nil
}
