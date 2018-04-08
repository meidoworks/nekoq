package tool

import (
	"bytes"
	"encoding/gob"
	"io"
	"log"
)

type NodeInfoRecord struct {
	RandomData []byte // 128 bytes

	// 4096
	RsaPrivateKey []byte
	RsaPublicKey  []byte
	// P521
	EcdsaPrivateKey []byte
	EcdsaPublicKey  []byte

	NodeId string
	UniqId int32
}

func (this *NodeInfoRecord) ToBytes() []byte {
	var network bytes.Buffer
	enc := gob.NewEncoder(&network)
	err := enc.Encode(*this)
	if err != nil {
		log.Fatalln("encode NodeInfoRecord error.", err)
	}
	return network.Bytes()
}

func FromBytes(data []byte) (*NodeInfoRecord, error) {
	b := bytes.NewBuffer(data)
	dec := gob.NewDecoder(b)
	var q NodeInfoRecord
	err := dec.Decode(&q)
	return &q, err
}

func FromReader(r io.Reader) (*NodeInfoRecord, error) {
	dec := gob.NewDecoder(r)
	var q NodeInfoRecord
	err := dec.Decode(&q)
	return &q, err
}
