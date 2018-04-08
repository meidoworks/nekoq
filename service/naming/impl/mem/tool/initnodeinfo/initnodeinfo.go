package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"flag"
	"log"
	"os"
)

import (
	"goimport.moetang.info/nekoq/service/naming/impl/mem/tool"
)

var (
	uniqId int64
	nodeId string
)

func init() {
	flag.StringVar(&nodeId, "nodeId", "", "nodeId=XXX")
	flag.Int64Var(&uniqId, "uniqId", 0, "uniqId=12345")
}

func main() {
	flag.Parse()

	if nodeId == "" {
		log.Fatalln("nodeId is emtpy.")
	}
	if uniqId <= 0 {
		log.Fatalln("uniqId invalid. value:", uniqId)
	}

	record := new(tool.NodeInfoRecord)

	randomData := make([]byte, 128)
	_, err := rand.Read(randomData)
	if err != nil {
		log.Fatalln(err)
	}
	record.RandomData = randomData

	pri, err := rsa.GenerateKey(rand.Reader, 4096)
	if err != nil {
		log.Fatalln(err)
	}
	rsaPriKey := x509.MarshalPKCS1PrivateKey(pri)
	rsaPubKey, err := x509.MarshalPKIXPublicKey(pri.Public())
	if err != nil {
		log.Fatalln("marshal rsa public key error.", err)
	}
	record.RsaPrivateKey = rsaPriKey
	record.RsaPublicKey = rsaPubKey

	ecpri, err := ecdsa.GenerateKey(elliptic.P521(), rand.Reader)
	if err != nil {
		log.Fatalln("generate ecdsa key error.", err)
	}
	ecdsaPriKey, err := x509.MarshalECPrivateKey(ecpri)
	if err != nil {
		log.Fatalln("marshal ec private key error.", err)
	}
	ecdsaPubKey, err := x509.MarshalPKIXPublicKey(ecpri.Public())
	if err != nil {
		log.Fatalln("marshal ec public key error.", err)
	}
	record.EcdsaPrivateKey = ecdsaPriKey
	record.EcdsaPublicKey = ecdsaPubKey

	record.NodeId = nodeId
	record.UniqId = int32(uniqId)

	f, err := os.OpenFile(nodeId+".info", os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		log.Fatalln("create node info file error.", err)
	}
	_, err = f.Write(record.ToBytes())
	if err != nil {
		log.Fatalln("write data to node info file error.", err)
	}
	err = f.Close()
	if err != nil {
		log.Fatalln("close node info file error.", err)
	}
}
