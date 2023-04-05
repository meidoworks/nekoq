package cellarclient

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/meidoworks/nekoq/shared/netaddons/httpaddons"
)

type DataProcessor func([]WatchKey, []*WatchData) error
type UpdateDataProcessor func([]*WatchData) error

type WatchKey struct {
	Area    string `json:"area"`
	DataKey string `json:"data_key"`
	Version int    `json:"version"`
}

type WatchData struct {
	Area    string `json:"area"`
	DataKey string `json:"data_key"`
	Version int    `json:"version"`
	Data    []byte `json:"data"`
}

type CellarClient struct {
	r    *http.Client
	addr string
}

func NewCellarClient(cellarAddr string) (*CellarClient, error) {
	c := &CellarClient{
		r:    new(http.Client),
		addr: fmt.Sprint("http://", cellarAddr),
	}
	// set timeout to 120, slightly more over than server blocking time
	c.r.Timeout = 128 * time.Second

	return c, nil
}

func (c CellarClient) GetAndWatch(watchKeys []WatchKey, dataProcessor DataProcessor, updateProcessor UpdateDataProcessor) error {
	const url = "/naming/cellar/watchers"

	req, err := json.Marshal(watchKeys)
	if err != nil {
		return err
	}
	resp, err := c.r.Post(c.addr+url, "application/json", bytes.NewBuffer(req))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	bReader := bufio.NewReader(resp.Body)
	// data message
	{
		msg, err := httpaddons.PollingMessage(bReader)
		if err != nil {
			return err
		}
		var watchData []*WatchData
		if err := json.Unmarshal(msg, &watchData); err != nil {
			return err
		}
		if dataProcessor != nil {
			if err := dataProcessor(watchKeys, watchData); err != nil {
				return err
			}
		}
	}
	// update message
	{
		msg, err := httpaddons.PollingMessage(bReader)
		if err != nil {
			return err
		}
		var watchData []*WatchData
		if err := json.Unmarshal(msg, &watchData); err != nil {
			return err
		}
		if updateProcessor != nil {
			if err := updateProcessor(watchData); err != nil {
				return err
			}
		}
	}

	return nil
}
