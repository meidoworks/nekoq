package httpaddons

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
)

const (
	messageStarter = "=-=-=-=-=-=-=-=-="
	splitter       = ","
	lineEnd        = "\r\n"

	maxPacketSize = 100 * 1024 * 1024
)

func SendMessage(w interface{}, data []byte) error {
	writer, ok := w.(io.Writer)
	if !ok {
		return errors.New("expect io.Writer")
	}
	if len(data) <= 0 {
		return errors.New("data is empty")
	}
	if len(data) > maxPacketSize {
		return errors.New("data is oversize")
	}
	lineData := []byte(headerLine(len(data)))
	if _, err := writer.Write(lineData); err != nil {
		return err
	}
	if _, err := writer.Write(data); err != nil {
		return err
	}
	if flusher, ok := w.(http.Flusher); ok {
		flusher.Flush()
	}
	return nil
}

func PollingMessage(reader *bufio.Reader) ([]byte, error) {
	hLine, err := reader.ReadString('\n')
	if err != nil {
		return nil, err
	}
	packetSize, err := parseHeaderLine(hLine)
	if err != nil {
		return nil, err
	}
	data := make([]byte, packetSize)
	if _, err := io.ReadFull(reader, data); err != nil {
		fmt.Println(string(data))
		return nil, err
	}

	return data, nil
}

func headerLine(packetSize int) string {
	return fmt.Sprint(messageStarter, splitter, packetSize, lineEnd)
}

func parseHeaderLine(line string) (int, error) {
	splits := strings.Split(strings.TrimSpace(line), splitter)
	if len(splits) < 2 {
		return 0, errors.New("header does not have enough data")
	}
	if splits[0] != messageStarter {
		return 0, errors.New("header start illegal")
	}
	packetSize, err := strconv.Atoi(splits[1])
	if err != nil {
		return 0, err
	}
	if packetSize > maxPacketSize {
		return 0, errors.New("packet size is oversize")
	}

	return packetSize, nil
}
