package thriftobs_test

import (
	"testing"
	"time"

	"github.com/alex-cos/thriftobs"
	"github.com/apache/thrift/lib/go/thrift"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestProtocolType_String(t *testing.T) {
	t.Parallel()

	tests := []struct {
		pt     thriftobs.ProtocolType
		expect string
	}{
		{thriftobs.Binary, "binary"},
		{thriftobs.Compact, "compact"},
		{thriftobs.JSON, "json"},
		{thriftobs.SimpleJSON, "simplejson"},
		{thriftobs.ProtocolType(99), ""}, // unknown
	}

	for _, tc := range tests {
		assert.Equal(t, tc.expect, tc.pt.String())
	}
}

func TestBuildProtocolFactory_Binary(t *testing.T) {
	t.Parallel()

	factory, err := thriftobs.BuildProtocolFactory(thriftobs.Binary, &thrift.TConfiguration{
		ConnectTimeout: 5 * time.Second,
		SocketTimeout:  5 * time.Second,
	})

	require.NoError(t, err)
	assert.NotNil(t, factory)
	assert.IsType(t, &thrift.TBinaryProtocolFactory{}, factory)
}

func TestBuildProtocolFactory_Compact(t *testing.T) {
	t.Parallel()

	factory, err := thriftobs.BuildProtocolFactory(thriftobs.Compact, &thrift.TConfiguration{
		ConnectTimeout: 5 * time.Second,
		SocketTimeout:  5 * time.Second,
	})

	require.NoError(t, err)
	assert.NotNil(t, factory)
	assert.IsType(t, &thrift.TCompactProtocolFactory{}, factory)
}

func TestBuildProtocolFactory_JSON(t *testing.T) {
	t.Parallel()

	factory, err := thriftobs.BuildProtocolFactory(thriftobs.JSON, nil)

	require.NoError(t, err)
	assert.NotNil(t, factory)
	assert.IsType(t, &thrift.TJSONProtocolFactory{}, factory)
}

func TestBuildProtocolFactory_SimpleJSON(t *testing.T) {
	t.Parallel()

	factory, err := thriftobs.BuildProtocolFactory(thriftobs.SimpleJSON, &thrift.TConfiguration{
		ConnectTimeout: 5 * time.Second,
		SocketTimeout:  5 * time.Second,
	})

	require.NoError(t, err)
	assert.NotNil(t, factory)
	assert.IsType(t, &thrift.TSimpleJSONProtocolFactory{}, factory)
}

func TestBuildProtocolFactory_Invalid(t *testing.T) {
	t.Parallel()

	factory, err := thriftobs.BuildProtocolFactory(thriftobs.ProtocolType(99), nil)

	assert.Error(t, err)
	assert.Nil(t, factory)
	assert.Contains(t, err.Error(), "invalid specified protocol name")
}

func TestBuildClientProtocolFactory(t *testing.T) {
	t.Parallel()

	metrics := thriftobs.GetMetrics()

	factory, err := thriftobs.BuildClientProtocolFactory(thriftobs.Binary, nil, metrics)

	require.NoError(t, err)
	assert.NotNil(t, factory)
	assert.IsType(t, &thriftobs.ClientProtocolFactory{}, factory)
}

func TestBuildClientProtocolFactory_InvalidProtocol(t *testing.T) {
	t.Parallel()

	metrics := thriftobs.GetMetrics()

	factory, err := thriftobs.BuildClientProtocolFactory(thriftobs.ProtocolType(99), nil, metrics)

	assert.Error(t, err)
	assert.Nil(t, factory)
}

func TestBuildClientProtocolFactory_WithConfig(t *testing.T) {
	t.Parallel()

	metrics := thriftobs.GetMetrics()

	factory, err := thriftobs.BuildClientProtocolFactory(thriftobs.Compact, &thrift.TConfiguration{
		ConnectTimeout: 10 * time.Second,
		SocketTimeout:  10 * time.Second,
	}, metrics)

	require.NoError(t, err)
	assert.NotNil(t, factory)
}
