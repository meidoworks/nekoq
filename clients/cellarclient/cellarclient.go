package cellarclient

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/meidoworks/nekoq/shared/netaddons/httpaddons"

	"github.com/go-resty/resty/v2"
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

type PutData struct {
	Area    string `json:"area"`
	DataKey string `json:"data_key"`
	Data    []byte `json:"data"`
	Version int    `json:"version"`
	Group   string `json:"group"`
}

type CellarClient struct {
	r    *http.Client
	rr   *resty.Client
	addr string

	//TODO local failover storage
}

func NewCellarClient(cellarAddr string) (*CellarClient, error) {
	c := &CellarClient{
		r:    new(http.Client),
		rr:   resty.New(),
		addr: fmt.Sprint("http://", cellarAddr),
	}
	// set timeout to 120, slightly more over than server blocking time
	c.r.Timeout = 128 * time.Second

	return c, nil
}

func (c *CellarClient) Get(area, dataKey string) (*WatchData, error) {
	var url = fmt.Sprintf("/naming/cellar/%s/%s", area, dataKey)

	resp, err := c.rr.R().Get(c.addr + url)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode() == 200 {
		wd := new(WatchData)
		if err := json.Unmarshal(resp.Body(), wd); err != nil {
			return nil, err
		} else {
			return wd, nil
		}
	} else {
		return nil, errors.New("get cellar data failed")
	}
}

func (c *CellarClient) Put(area, dataKey string, data []byte, version int, group string) error {
	const url = "/naming/cellar/item"

	putData := &PutData{
		Area:    area,
		DataKey: dataKey,
		Data:    data,
		Version: version,
		Group:   group,
	}

	resp, err := c.rr.R().
		SetBody(putData).
		Put(c.addr + url)
	if err != nil {
		return err
	}
	if resp.StatusCode() == 200 {
		return nil
	} else {
		return errors.New("put failed")
	}
}

func (c *CellarClient) GetAndWatch(watchKeys []WatchKey, dataProcessor DataProcessor, updateProcessor UpdateDataProcessor) error {
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
