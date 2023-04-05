package cellar

import (
	"errors"
	"time"

	"github.com/meidoworks/nekoq/shared/logging"

	"gorm.io/gorm"
)

const (
	CellarDataChangeOperationPut = 1
)

var (
	ErrCellarDatabaseNoSuchRecord        = errors.New("no such record")
	ErrCellarDataStringHasInvalidFormat  = errors.New("CellarData string has invalid format")
	ErrCellarDataVersionShouldBePositive = errors.New("CellarData version should be positive")
)

type CellarData struct {
	DataId      int `gorm:"primaryKey;autoIncrement"`
	Area        string
	DataKey     string
	DataVersion int
	DataContent []byte
	GroupKey    string // cannot be changed once set
	DataStatus  int
	TimeCreated int `gorm:"autoCreateTime:milli"`
	TimeUpdated int `gorm:"autoUpdateTime:milli"`
}

func (d *CellarData) UniqueKey() string {
	return d.Area + "/" + d.DataKey
}

func (*CellarData) TableName() string {
	return "cellar_data"
}

func (d *CellarData) validate() error {
	if !checkAllowedCharacters(d.Area) {
		return ErrCellarDataStringHasInvalidFormat
	}
	if !checkAllowedCharacters(d.DataKey) {
		return ErrCellarDataStringHasInvalidFormat
	}
	if !checkAllowedCharacters(d.GroupKey) {
		return ErrCellarDataStringHasInvalidFormat
	}
	if d.DataVersion <= 0 {
		return ErrCellarDataVersionShouldBePositive
	}
	return nil
}

type CellarDataChange struct {
	ChangeId    int `gorm:"primaryKey;autoIncrement"`
	Area        string
	DataKey     string
	DataVersion int
	Operation   int
	TimeCreated int `gorm:"autoCreateTime:milli"`
}

func (*CellarDataChange) TableName() string {
	return "cellar_data_change"
}

var _cellarDbLogger = logging.NewLogger("CellarDB")

func (c *Cellar) PutData(data CellarData) error {
	if err := data.validate(); err != nil {
		return err
	}

	nowTime := time.Now().UnixMilli()

	return c.db.Transaction(func(tx *gorm.DB) error {
		var oldVersion int
		putRes := tx.Raw(`
insert into cellar_data (area, data_key, data_content, data_version, group_key, time_created, time_updated)
VALUES ($1, $2, $3, $4, $5, $6, $7)
on conflict (area, data_key)
    do update set data_content = excluded.data_content,
                  data_version = excluded.data_version,
                  time_updated = excluded.time_updated
where cellar_data.data_version < excluded.data_version
returning cellar_data.data_version
`, data.Area, data.DataKey, data.DataContent, data.DataVersion, data.GroupKey, nowTime, nowTime).Scan(&oldVersion)
		if putRes.Error != nil {
			return putRes.Error
		}
		if putRes.RowsAffected > 0 {
			changeEntry := &CellarDataChange{
				Area:        data.Area,
				DataKey:     data.DataKey,
				DataVersion: data.DataVersion,
				Operation:   CellarDataChangeOperationPut,
			}
			saveRes := tx.Save(changeEntry)
			if saveRes.Error != nil {
				return saveRes.Error
			}
			return nil
		} else {
			_cellarDbLogger.Debugf("PutData with no update")
			return nil
		}
	})
}

func (c *Cellar) GetData(area, dataKey string) (CellarData, error) {
	data := CellarData{}
	r := c.db.Raw(`
select * from cellar_data where area = $1 and data_key = $2
`, area, dataKey).Scan(&data)
	if r.Error != nil {
		return data, r.Error
	}
	if r.RowsAffected <= 0 {
		return data, ErrCellarDatabaseNoSuchRecord
	}
	return data, nil
}

func (c *Cellar) queryLatestChange() (CellarDataChange, error) {
	change := CellarDataChange{}
	r := c.db.Order("change_id desc").Find(&change)
	if r.Error != nil {
		return change, r.Error
	}
	if r.RowsAffected <= 0 {
		return change, ErrCellarDatabaseNoSuchRecord
	}
	return change, nil
}

func (c *Cellar) queryCellarDataPaging(startDataId int, pageSize int) (result []*CellarData, err error) {
	r := c.db.Order("data_id asc").Where("data_id > ?", startDataId).Limit(pageSize).
		Find(&result)
	if r.Error != nil {
		err = r.Error
		return
	}
	return
}

func (c *Cellar) queryCellarDataByAreaDataKeyPairList(req [][]interface{}) (result []*CellarData, err error) {
	r := c.db.Where("(area, data_key) IN ?", req).Find(&result)
	if r.Error != nil {
		err = r.Error
		return
	}
	return
}

func (c *Cellar) queryCellarDataChangesPaging(startId, endId int, maxSize int) (result []CellarDataChange, err error) {
	r := c.db.Order("change_id asc").Where("change_id > ? and change_id <= ?", startId, endId).Limit(maxSize).
		Find(&result)
	if r.Error != nil {
		err = r.Error
		return
	}
	return
}
