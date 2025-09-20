package bus

import (
	"log/slog"
)

type HandleFuncOpts struct {
	HandleFunc func(*Message)
	Queue      string
	Prefetch   int
}

// RegisterAsyncHandleFunc registers 1 func to handle msgs from 1 queue
func (b *Bus) RegisterAsyncHandleFunc(o HandleFuncOpts) {
	if _, ok := b.inChans[o.Queue]; ok {
		Slog.Error("queue already has a handler/handleFunc", "queue", o.Queue)
		return
	}

	b.handleFuncs[o.Queue] = o.HandleFunc

	b.inQueues[o.Queue] = struct{ Prefetch int }{
		Prefetch: o.Prefetch,
	}
	b.inQueues["noprefetch:"+o.Queue] = struct{ Prefetch int }{
		Prefetch: 0,
	}

	slog.Info("registered handleFunc", "queue", o.Queue, "prefetch", o.Prefetch)
}
