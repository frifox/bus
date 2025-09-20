package bus

import (
	"context"
	"os"
	"reflect"
	"sync"
	"time"

	"github.com/rabbitmq/amqp091-go"
)

func (b *Bus) Consume(consumers *sync.WaitGroup, queue string) {
	defer consumers.Done()

	ch, ok := b.inChans[queue]
	if !ok {
		Slog.Error("queue not in inChans")
		return
	}

	delivereis, err := ch.consume(consumeOpts{
		Queue: queue, // rmq queue name

		// defaults
		ConsumerTag: "",    // consumer name, auto-generated
		NoLocal:     false, // not supported...
		NoAck:       false, // ie: autoAck (false=consumer has to Ack)
		Exclusive:   false, // force only 1 consumer for this queue
		NoWait:      false, // begin deliveries immediately, don't wait for server to confirm request
		Arguments:   nil,
	})
	if err != nil {
		Slog.Error("failed to consume queue", "err", err)
		os.Exit(1)
	}

	wg := sync.WaitGroup{}
	for {
		select {
		case delivery, open := <-delivereis:
			if !open {
				break
			}

			Slog.Debug("new delivery", "queue", queue, "funcName", delivery.CorrelationId, "msgID", delivery.MessageId)

			wg.Add(1)
			go b.consume(&delivery, &wg)
		case <-b.AppCtx.Done():
			Slog.Warn("app ctx done. stopping consuming")
			break
		case <-b.connCtx.Done():
			Slog.Warn("conn ctx done. stopping consuming")
			break
		}

		// need to stop consuming?
		if b.AppCtx.Err() != nil {
			break
		}
		if b.connCtx.Err() != nil {
			break
		}
	}

	Slog.Warn("waiting for consuming to finish")
	wg.Wait()
}

func (b *Bus) consume(delivery *amqp091.Delivery, wg *sync.WaitGroup) {
	defer wg.Done()

	msg := Message{}
	msg.FromDelivery(delivery)

	ret := b.consumeMsg(&msg)
	err := delivery.Ack(false)
	if err != nil {
		Slog.Error("delivery.Ack()", "err", err)
	}

	if msg.ReplyTo != "" {
		b.sendHandlerReturnToBus(&msg, ret)
	}
}

func (b *Bus) consumeMsg(msg *Message) *reflect.Value {
	switch msg.BusMsgType {
	case MsgTypeRequest:
		// TODO if async, do not make ctx?

		// make msg ctx, for future cancels
		var timeoutAt time.Time
		switch {
		case !msg.MsgDeadline.IsZero():
			// api will return after timeout, safe to assume request is cancelled too
			timeoutAt = msg.MsgDeadline
		default:
			// default = expire immediately
			timeoutAt = time.Now()
		}

		// create CTX even if expires immediately, so that func context.Context isn't nil
		msg.ctx, msg.cancel = context.WithDeadline(context.Background(), timeoutAt)

		// store cancel(), ran if user cancels request / route timeout
		b.inContextCancels.Store(msg.MsgID, msg.cancel)
		go func() {
			// garbage-collect b.inContextCancels
			<-msg.ctx.Done()
			cancel, found := b.inContextCancels.LoadAndDelete(msg.MsgID)
			if !found {
				return // already gc'd
			}
			cancel()
		}()

		// msg for Handler?
		handler, ok := b.handlers[msg.ToQueue]
		if ok && handler != nil && msg.ToFunc != "" {
			Slog.Info("consuming via handler", "func", msg.ToFunc, "body", msg.Body)
			return b.consumeMsgViaHandler(handler, msg)
		}

		// msg for HandleFunc?
		handleFunc, ok := b.handleFuncs[msg.ToQueue]
		if ok && handleFunc != nil {
			Slog.Info("consuming via registered handleFunc", "body", msg.Body)
			go handleFunc(msg) // process in bg, to avoid unexpected blocks
			return nil
		}

		Slog.Error("no handler found for msg", "ToQueue", msg.ToQueue, "ToFunc", msg.ToFunc, "message", msg)
	case MsgTypeResponse:
		// need MsgID so we can find closure
		if msg.MsgID == "" {
			Slog.Warn("can't find busClosure: no msgID", "message", msg)
			return nil
		}

		busClosure, ok := b.outClosures.LoadAndDelete(msg.MsgID)
		if !ok {
			Slog.Warn("no busClosure for reply. Client gave up?")
			return nil
		}

		// fwd reply to user closure as-is
		busClosure(msg)
	case MsgTypeCtxCancel:
		cancel, found := b.inContextCancels.LoadAndDelete(msg.MsgID)
		if !found {
			Slog.Info("cancel msg: ctx not found. Msg timeout or consumer replied?", "ToQueue", msg.ToQueue, "ToFunc", msg.ToFunc, "MsgID", msg.MsgID)
			return nil
		}

		Slog.Info("got cancel ctx msg. Cancelling", "ToQueue", msg.ToQueue, "ToFunc", msg.ToFunc, "MsgID", msg.MsgID)
		cancel()
	default:
		Slog.Error("unknown msg type", "BusMsgType", msg.BusMsgType, "message", msg)
	}
	return nil
}
