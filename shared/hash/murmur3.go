package hash

func rotl32(x uint32, r uint8) uint32 {
	return (x << r) | (x >> (32 - r))
}

type Murmur3 struct {
	h1     uint32
	length uint32
	t      [4]byte
	rem    int
}

const c1 = uint32(0xcc9e2d51)
const c2 = uint32(0x1b873593)

func (m *Murmur3) update(k1 uint32) {
	k1 *= c1
	k1 = rotl32(k1, 15)
	k1 *= c2
	m.h1 ^= k1
	m.h1 = rotl32(m.h1, 13)
	m.h1 = m.h1*5 + 0xe6546b64
}

func (m *Murmur3) _Write(data []byte) {
	datalen := len(data)
	length := datalen
	m.length += uint32(length)
	if m.rem != 0 {
		need := 4 - m.rem
		if length < need {
			copy(m.t[m.rem:], data[:length])
			m.rem += length
			return
		}
		var k1 uint32
		switch need {
		case 1:
			k1 = uint32(m.t[0]) | uint32(m.t[1])<<8 | uint32(m.t[2])<<16 | uint32(data[0])<<24
		case 2:
			k1 = uint32(m.t[0]) | uint32(m.t[1])<<8 | uint32(data[0])<<16 | uint32(data[1])<<24
		case 3:
			k1 = uint32(m.t[0]) | uint32(data[0])<<8 | uint32(data[1])<<16 | uint32(data[2])<<24
		}
		m.update(k1)
		length -= need
		m.rem = 0
		data = data[need:]
	}
	rem := length & 3
	b := length - rem
	for i := 0; i < b; i += 4 {
		k1 := uint32(data[i]) | uint32(data[i+1])<<8 | uint32(data[i+2])<<16 | uint32(data[i+3])<<24
		m.update(k1)
	}
	copy(m.t[:rem], data[b:])
	m.rem = rem
	return
}

func (m *Murmur3) _Sum32() uint32 {
	k1 := uint32(0)
	h1 := m.h1
	switch m.rem {
	case 3:
		k1 ^= uint32(m.t[2]) << 16
		fallthrough
	case 2:
		k1 ^= uint32(m.t[1]) << 8
		fallthrough
	case 1:
		k1 ^= uint32(m.t[0])
		k1 *= c1
		k1 = rotl32(k1, 15)
		k1 *= c2
		h1 ^= k1
	}
	h1 ^= m.length
	h1 ^= h1 >> 16
	h1 *= uint32(0x85ebca6b)
	h1 ^= h1 >> 13
	h1 *= uint32(0xc2b2ae35)
	h1 ^= h1 >> 16
	return h1
}

func DoMurmur3(data []byte) uint32 {
	m := new(Murmur3)
	m._Write(data)
	return m._Sum32()
}
