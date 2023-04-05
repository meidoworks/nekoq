package namingclient

import (
	"encoding/binary"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/meidoworks/nekoq/api"
	"github.com/meidoworks/nekoq/shared/hardware"
	"github.com/meidoworks/nekoq/shared/logging"
	"github.com/meidoworks/nekoq/shared/netaddons/localswitch"

	"github.com/go-resty/resty/v2"
)

var _NamingClient = logging.NewLogger("NamingClient")

type NamingClient struct {
	r          *resty.Client
	namingHost string

	keepAliveInterval int

	node string

	local bool
}

func (n *NamingClient) sendKeepAlive(host, service, area string) error {
	if resp, err := n.r.R().EnableTrace().
		Head(fmt.Sprintf("%s/node/%s/%s/%s", n.namingHost, n.node, service, area)); err != nil {
		return err
	} else if resp.StatusCode() != 200 {
		return errors.New("keepalive service failed")
	} else {
		return nil
	}
}

func (n *NamingClient) addKeepAliveTask(host, service, area string) error {
	go func() {
		for {
			// Note: This operation may take long. Need handle timeout case(e.g. re-register service)
			if err := n.sendKeepAlive(host, service, area); err != nil {
				_NamingClient.Errorf("send keep alive error:%s", err)
			}
			time.Sleep(time.Duration(n.keepAliveInterval) * time.Second)
		}
	}()
	return nil
}

type serviceInfo struct {
	// Host 4b+2b/ipv4, 16b+2b/ipv6
	HostAndPort     []byte `json:"host_and_port"`
	IPv6HostAndPort []byte `json:"ipv6_host_and_port"`
}

type ServiceDesc struct {
	ip   []byte
	ipv6 []byte
	Port int
}

func convertHostAndPort(host []byte, port int) []byte {
	r := make([]byte, len(host)+2)
	copy(r[:len(host)], host)
	binary.BigEndian.PutUint16(r[len(host):], uint16(port))
	return r
}

func (n *NamingClient) register0(service, area string, desc ServiceDesc) error {
	si := &serviceInfo{
		HostAndPort:     convertHostAndPort(desc.ip, desc.Port),
		IPv6HostAndPort: convertHostAndPort(desc.ipv6, desc.Port),
	}
	if resp, err := n.r.R().EnableTrace().
		SetBody(si).
		Put(fmt.Sprintf("%s/node/%s/%s/%s", n.namingHost, n.node, service, area)); err != nil {
		return err
	} else if resp.StatusCode() != 200 {
		return errors.New("register service failed")
	} else {
		return nil
	}
}

func (n *NamingClient) Register(serviceName, area string, desc ServiceDesc) error {
	if n.local {
		// fetch ip from NIC and choose the first one
		// If a chosen ip is needed, it has to be specified in getting ip list from NICs
		iplist, err := hardware.GetMachineIpList()
		if err != nil {
			return err
		} else if len(iplist) == 0 {
			return errors.New("no ip found on local NICs")
		} else {
			var ifaceName string
			for _, v := range iplist {
				if ifaceName == "" {
					ifaceName = v.GetName()
				} else if ifaceName != v.GetName() {
					// only get the addresses of the first iface
					break
				}
				if len(v.GetRawIP()) == 16 && len(desc.ipv6) == 0 {
					desc.ipv6 = v.GetRawIP()
				} else if len(v.GetRawIP()) == 4 && len(desc.ip) == 0 {
					desc.ip = v.GetRawIP()
				}
			}
		}
	} else {
		//step1 get ip from selfip api
		//step2 register service
		panic("unsupported")
	}
	if err := n.register0(serviceName, area, desc); err != nil {
		return err
	}
	if err := n.addKeepAliveTask(n.namingHost, serviceName, area); err != nil {
		return err
	}
	return nil
}

func NewLocalSwitchNamingClient(lswitch *localswitch.LocalSwitch, node string) (*NamingClient, error) {
	c := localswitch.NewLocalSwitchHttpClient(lswitch, api.LocalSwitchDiscoveryAndCellar)
	nc := &NamingClient{
		r:                 resty.NewWithClient(c),
		local:             true,
		namingHost:        "http://localhost",
		keepAliveInterval: 5,
		node:              node,
	}

	return nc, nil
}

func NewNamingClient(namingAddr string, node string) (*NamingClient, error) {
	nc := &NamingClient{
		r:                 resty.New(),
		local:             false,
		namingHost:        fmt.Sprintf("http://%s", namingAddr),
		keepAliveInterval: 5,
		node:              node,
	}

	return nc, nil
}

func ParsePortFromHost(addr string) int {
	sp := strings.Split(addr, ":")
	r, _ := strconv.Atoi(sp[1])
	return r
}
