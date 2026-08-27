package thriftobs_test

import (
	"testing"

	"github.com/alex-cos/thriftobs"
	"github.com/apache/thrift/lib/go/thrift"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConvertTransport(t *testing.T) {
	t.Parallel()

	mt := &thriftobs.MetricsTransport{}
	tTransport := thrift.TTransport(mt)

	result := thriftobs.ConvertTransport(tTransport)

	require.NotNil(t, result)
	assert.Equal(t, mt, result)
}

func TestConvertTransport_NonMetricsTransport(t *testing.T) {
	t.Parallel()

	type otherTransport struct {
		thrift.TTransport
	}
	ot := &otherTransport{}

	result := thriftobs.ConvertTransport(ot)

	assert.Nil(t, result)
}

func TestConvertTransport_Nil(t *testing.T) {
	t.Parallel()

	var tTransport thrift.TTransport = nil

	result := thriftobs.ConvertTransport(tTransport)

	assert.Nil(t, result)
}

func TestSafeObserve_Success(t *testing.T) {
	t.Parallel()

	called := false
	thriftobs.SafeObserve(func() {
		called = true
	})
	assert.True(t, called)
}

func TestSafeObserve_Panic(t *testing.T) {
	t.Parallel()

	// Should not panic, should recover
	thriftobs.SafeObserve(func() {
		panic("test panic")
	})
}

func TestSafeObserve_NestedPanic(t *testing.T) {
	t.Parallel()

	thriftobs.SafeObserve(func() {
		thriftobs.SafeObserve(func() {
			panic("inner panic")
		})
	})
	// Should not panic
}
