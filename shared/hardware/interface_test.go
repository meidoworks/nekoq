package hardware

import "testing"

func TestGetNicIP(t *testing.T) {
	l, err := GetMachineIpList()
	if err != nil && len(l) == 0 {
		t.Fatal("no ip address")
	}
	t.Log(l)
}
