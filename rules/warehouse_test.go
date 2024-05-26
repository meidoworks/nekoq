package rules

import "testing"

func TestValidateWarehouseKey(t *testing.T) {
	if !ValidateWarehouseKey("a.b.c") {
		t.Fatal("unexpected")
	}
	if !ValidateWarehouseKey("c") {
		t.Fatal("unexpected")
	}
	if !ValidateWarehouseKey("a.b") {
		t.Fatal("unexpected")
	}
}
