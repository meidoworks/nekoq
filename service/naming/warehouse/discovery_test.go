package warehouse

import (
	"testing"

	"github.com/meidoworks/nekoq/service/naming/warehouseapi"
)

func TestRootRelated(t *testing.T) {
	var d warehouseapi.DiscoveryUse = newLocalMemory()

	if l, err := d.AreaLevels(warehouseapi.RootArea); err != nil {
		t.Fatal(err)
	} else if len(l) != 1 {
		t.Fatal("size != 1")
	}

	if err := d.PutArea(warehouseapi.RootArea, warehouseapi.RootArea); err != warehouseapi.ErrAreaSelfReference {
		t.Fatal("should report ErrAreaSelfReference")
	}

	if err := d.PutArea("cn", warehouseapi.RootArea); err != nil {
		t.Fatal("should success")
	}

	if l, err := d.AreaLevels("cn"); err != nil {
		t.Fatal("should success")
	} else if len(l) != 2 {
		t.Fatal("should have 2 levels")
	} else if l[0].Area() != "cn" || l[1].Area() != warehouseapi.RootArea {
		t.Fatal("area levels not match")
	}

	if _, err := d.PutAreaAttr(warehouseapi.RootArea, "config1", []byte("value1")); err != nil {
		t.Fatal("should success")
	}
	if _, err := d.AreaAttr("cn", "config1"); err != warehouseapi.ErrAreaAttrNotFound {
		t.Fatal("should report ErrAreaAttrNotFound")
	}
	if v, err := d.AreaAttr(warehouseapi.RootArea, "config1"); err != nil {
		t.Fatal("should success")
	} else if string(v) != "value1" {
		t.Fatal("value not match")
	}
	if v, err := d.AreaAttrRecursively(warehouseapi.RootArea, "config1"); err != nil {
		t.Fatal("should success")
	} else if string(v) != "value1" {
		t.Fatal("value not match")
	}
	if _, err := d.AreaAttrRecursively(warehouseapi.RootArea, "config2"); err != warehouseapi.ErrAreaAttrNotFound {
		t.Fatal("should report ErrAreaAttrNotFound")
	}

}
