package thriftobs

import (
	"fmt"
	"os"

	"github.com/apache/thrift/lib/go/thrift"
)

func ConvertTransport(t thrift.TTransport) *MetricsTransport {
	mTransport, ok := t.(*MetricsTransport)
	if ok {
		return mTransport
	}

	return nil
}

func SafeObserve(fn func()) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Fprintf(os.Stderr, "Recovered from error: %v\n", r)
		}
	}()

	fn()
}
