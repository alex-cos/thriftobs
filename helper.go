package thriftobs

import (
	"fmt"

	"github.com/apache/thrift/lib/go/thrift"
)

type ProtocolType int

const (
	Binary ProtocolType = iota
	Compact
	JSON
	SimpleJSON
)

var protocolTypeName = map[ProtocolType]string{
	Binary:     "binary",
	Compact:    "compact",
	JSON:       "json",
	SimpleJSON: "simplejson",
}

func (pt ProtocolType) String() string {
	return protocolTypeName[pt]
}

func BuildProtocolFactory(
	pt ProtocolType,
	conf *thrift.TConfiguration,
) (thrift.TProtocolFactory, error) {
	switch pt {
	case Compact:
		return thrift.NewTCompactProtocolFactoryConf(conf), nil
	case SimpleJSON:
		return thrift.NewTSimpleJSONProtocolFactoryConf(conf), nil
	case JSON:
		return thrift.NewTJSONProtocolFactory(), nil
	case Binary:
		return thrift.NewTBinaryProtocolFactoryConf(conf), nil
	default:
		return nil, fmt.Errorf("invalid specified protocol name '%s'", pt.String())
	}
}

func BuildClientProtocolFactory(
	pt ProtocolType,
	conf *thrift.TConfiguration,
	metrics *Metrics,
) (thrift.TProtocolFactory, error) {
	protocolFactory, err := BuildProtocolFactory(pt, nil)
	if err != nil {
		return nil, err
	}

	return NewClientProtocolFactory(protocolFactory, metrics), nil
}
