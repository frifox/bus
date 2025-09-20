package bus

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/frifox/fifo"
	"github.com/puzpuzpuz/xsync/v4"
	"github.com/rabbitmq/amqp091-go"
)

type Bus struct {
	AppCtx     context.Context
	connCtx    context.Context
	connCancel context.CancelFunc
	context.Context
	context.CancelFunc

	DSN string

	// replies
	CallbackQueue string        // where to listen for replies
	Prefetch      int           // replies consuming Prefetch
	MsgTTL        time.Duration // TTL for replies sent to producer

	// incoming
	InConn   *amqp091.Connection
	inChans  map[string]*channel
	inQueues map[string]struct {
		Prefetch int
	}
	inContextCancels *xsync.Map[string, context.CancelFunc]

	// outgoing
	outConn     *amqp091.Connection
	outChan     *channel
	outBacklog  *fifo.Queue[string, *Message, any]
	outClosures *xsync.Map[string, func(any)]

	// queue => handler/handleFunc
	handlers    map[string]Handler
	handleFuncs map[string]func(*Message)
}

type Handler map[string]HandlerFunc

type HandlerFunc struct {
	Func    reflect.Value
	Args    []reflect.Type
	Returns []reflect.Type
}

type Opts struct {
	Context       context.Context
	DSN           string
	CallbackQueue string
	Prefetch      int
	//MsgTimeout time.Duration
	MsgTTL time.Duration
}

func NewBus(o Opts) *Bus {
	Slog.Info("NewBus", "opts", o)

	// defaults
	if o.Context == nil {
		o.Context = context.Background()
	}

	bus := Bus{
		AppCtx:     o.Context,
		outBacklog: fifo.NewQueue[string, *Message, any](o.Context),

		DSN:           o.DSN,
		CallbackQueue: o.CallbackQueue,
		Prefetch:      o.Prefetch,

		handlers:    make(map[string]Handler),
		handleFuncs: make(map[string]func(*Message)),

		inChans:          make(map[string]*channel),
		inQueues:         make(map[string]struct{ Prefetch int }),
		inContextCancels: xsync.NewMap[string, context.CancelFunc](),
		outClosures:      xsync.NewMap[string, func(any)](),
	}
	bus.Context, bus.CancelFunc = context.WithCancel(context.Background())

	return &bus
}

func (b *Bus) Run() {
	for {
		if b.AppCtx.Err() != nil {
			Slog.Warn("app done, returning")
			break
		}

		// connect
		err := b.Connect()
		if err != nil {
			Slog.Warn("couldn't connect. Trying again in 1s")
			time.Sleep(time.Second)
			continue
		}

		// incoming: callbacks
		err = b.OpenInChannel(b.CallbackQueue, b.Prefetch)
		if err != nil {
			Slog.Error("OpenInChannel: callbacks", "queue", b.CallbackQueue, "err", err)
			continue
		}
		// incoming: service + service:noprefetch
		for queue, conf := range b.inQueues {
			err := b.OpenInChannel(queue, conf.Prefetch)
			if err != nil {
				Slog.Error("OpenInChannel: services", "queue", queue, "err", err)
				continue
			}
		}

		// outgoing channel
		err = b.openOutChannel()
		if err != nil {
			Slog.Error("openOutChannel", "err", err)
			continue
		}

		// connected
		b.connCtx, b.connCancel = context.WithCancel(context.Background())
		go b.publishBacklog()

		Slog.Info("starting consumers")
		wg := sync.WaitGroup{}
		wg.Add(1)
		go b.Consume(&wg, b.CallbackQueue)
		for queue, _ := range b.inQueues {
			wg.Add(1)
			go b.Consume(&wg, queue)
		}

		Slog.Info("bus up and running")

		// wait for consumers to die
		wg.Wait()

		// clean up channels
		for queue, ch := range b.inChans {
			err := ch.Close()
			if err != nil {
				Slog.Warn("couldn't close channel", "err", err)
			}
			delete(b.inChans, queue)
		}
		err = b.outChan.Close()
		if err != nil {
			Slog.Warn("couldn't close channel", "err", err)
		}
	}

	Slog.Warn("Run() finished")
	b.CancelFunc()
}

func (b *Bus) Connect() error {
	if b.CallbackQueue == "" {
		return fmt.Errorf("callback queue not defined")
	}

	var err error

	// incoming
	{
		b.InConn, err = amqp091.Dial(b.DSN)
		if err != nil {
			return fmt.Errorf("failed to connect to rmq: %w", err)
		}

		ch := make(chan *amqp091.Error)
		b.InConn.NotifyClose(ch)
		go func() {
			for msg := range ch {
				Slog.Debug("NotifyClose InConn", "error", msg)
				if b.connCancel != nil {
					b.connCancel()
				}
			}
		}()
	}

	// outgoing
	{
		b.outConn, err = amqp091.Dial(b.DSN)
		if err != nil {
			return fmt.Errorf("failed to connect to rmq: %w", err)
		}

		ch := make(chan *amqp091.Error)
		b.outConn.NotifyClose(ch)
		go func() {
			for msg := range ch {
				Slog.Debug("NotifyClose outConn", "error", msg)
				if b.connCancel != nil {
					b.connCancel()
				}
			}
		}()
	}

	return nil
}

func (b *Bus) OpenInChannel(queue string, prefetchCount int) error {
	Slog.Debug("opening in channel", "queue", queue, "prefetch", prefetchCount)

	chTmp, err := b.InConn.Channel()
	if err != nil {
		return fmt.Errorf("failed to open in channel: %w", err)
	}
	b.inChans[queue] = &channel{chTmp}

	// set prefetch
	err = b.inChans[queue].qos(qosOpts{
		PrefetchCount: prefetchCount,

		// defaults
		PrefetchSize: 0,
		Global:       false,
	})
	if err != nil {
		return fmt.Errorf("failed to set in qos: %w", err)
	}

	// pre-declare queue
	_, err = b.inChans[queue].queueDeclare(queueDeclareOpts{
		Queue:   queue,
		Durable: true, // keep queue on rmq reboot (+ msgs with Persistent=true will stay on reboot)

		// defaults
		Exclusive:  false, // queue only on this conn, other conn with err
		AutoDelete: false, // delete queue if no consumers
		NoWait:     false, // assume queue is declared, err if re-declared differrently
		Arguments:  nil,
	})
	if err != nil {
		return fmt.Errorf("failed to declare queue: %w", err)
	}

	b.bindChannelEvents(b.inChans[queue], "b.chanIn")

	return nil
}

func (b *Bus) openOutChannel() error {
	chTmp, err := b.outConn.Channel()
	if err != nil {
		return fmt.Errorf("failed to open out channel: %w", err)
	}
	b.outChan = &channel{chTmp}

	b.bindChannelEvents(b.outChan, "b.chanOut")
	return nil
}
