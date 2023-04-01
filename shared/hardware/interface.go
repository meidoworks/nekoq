package hardware

import (
	"errors"
	"net"
	"strings"
)

type IpEntry struct {
	ifaceName string
	addr      string
	raw       []byte
}

type MachineIpFilter func(ifaceName string) bool

func GetMachineIpList() ([]IpEntry, error) {
	return SelectMachineIpList(func(ifaceName string) bool {
		return true
	})
}

func SelectMachineIpList(filter MachineIpFilter) ([]IpEntry, error) {
	ifaces, err := net.Interfaces()
	if err != nil {
		return nil, err
	}
	r := make([]IpEntry, 0, 16)
	var errs []error
	// handle err
	for _, i := range ifaces {
		addrs, err := i.Addrs()
		if err != nil {
			errs = append(errs, err)
			continue
		}
		// handle err
		for _, addr := range addrs {
			var ip net.IP
			switch v := addr.(type) {
			case *net.IPNet:
				ip = v.IP
			case *net.IPAddr:
				ip = v.IP
			default:
				continue
			}
			isIpv4 := strings.Contains(ip.String(), ".")
			// process IP address
			if isIpv4 {
				r = append(r, IpEntry{
					ifaceName: i.Name,
					addr:      ip.String(),
					raw:       ([]byte(ip))[12:],
				})
			} else {
				r = append(r, IpEntry{
					ifaceName: i.Name,
					addr:      ip.String(),
					raw:       ([]byte(ip))[:16],
				})
			}
		}
	}

	if len(errs) > 0 {
		return r, errors.Join(errs...)
	} else {
		return r, nil
	}
}
