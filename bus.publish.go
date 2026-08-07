package bus

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

var ErrPublishing = errors.New("publish failed")
var ErrReplyTimeout = errors.New("bus reply timeout")
var ErrRequestCancelled = errors.New("request cancelled")

//type PublishOpts struct {
//	ToQueue    string
//	ToFunc     string
//	BusTimeout time.Duration
//	BusTTL     time.Duration
//}

func (b *Bus) publishBacklog() {
	for {
		select {
		case job, open := <-b.outBacklog.Jobs:
			if !open {
				break
			}

			err := b.Publish(job.Request)
			if err != nil {
				Slog.Error("failed publishing backlog", "err", err)
				return
			}
		case <-b.connCtx.Done():
			return
		}
	}
}

const MsgTypeRequest = "Request"
const MsgTypeResponse = "Response"
const MsgTypeCtxCancel = "CtxCancel"

// Publish pushes msg to bus. Useful if RPCs are one-way, or you want to handle replies yourself
func (b *Bus) Publish(msg *Message) error {
	// just in case
	if msg.BusMsgType == "" {
		msg.BusMsgType = MsgTypeRequest
	}

	// if conn is down, publish after conn is restored
	if b.connCtx == nil || b.connCtx.Err() != nil {
		b.outBacklog.Add(msg.MsgID, msg)
		return nil
	}

	// ack will be nil (channel default is confirming=false)
	_, err := b.outChan.publishWithDeferredConfirm(publishOpts{
		Exchange:   "",          // single pub to sincle queue = can use default exchange
		RoutingKey: msg.ToQueue, // queue name
		Mandatory:  false,       // don't check if queue exists
		Immediate:  false,       // don't need consumer to be running
		Publishing: msg.ToPublishing(),
	})
	if err != nil {
		return fmt.Errorf("%w: %w", ErrPublishing, err)
	}

	Slog.Info("published to bus", "MsgID", msg.MsgID, "queue", msg.ToQueue, "func", msg.ToFunc, "body", msg.Body)

	return nil
}

func (b *Bus) CancelMsg(msg Message, callbackQueue string) error {
	Slog.Debug("CancelMsg()", "msgID", msg.MsgID)

	return b.Publish(&Message{
		BusMsgType: MsgTypeCtxCancel,
		MsgID:      msg.MsgID,
		ToQueue:    callbackQueue,
		MsgTTL:     b.MsgTTL,
	})
}

// PublishAndClose extends Publish by catching replies + handing bus timeouts / request cancels
func (b *Bus) PublishAndClose(msg *Message, userClosure func(any), requestCtxs ...context.Context) {
	if msg.MsgID == "" {
		msg.MsgID = uuid.New().String()
	}
	if msg.ReplyTo == "" {
		msg.ReplyTo = b.CallbackQueue
	}
	if msg.BusMsgType == "" {
		msg.BusMsgType = MsgTypeRequest
	}

	// defaults
	if msg.MsgDeadline.IsZero() {
		Slog.Warn("msg with MsgDeadline=0. Falling back to now+10s")
		msg.MsgDeadline = time.Now().Add(time.Second * 10)
	}
	if msg.MsgTTL == 0 {
		Slog.Warn("msg with MsgTTL=0. Falling back to 10m")
		msg.MsgTTL = time.Minute * 10
	}

	// user doesn't want any return...?
	if userClosure == nil {
		userClosure = func(busReturn any) {
			// do nothing
		}
	}

	// request ctx catches client giving up
	var requestCtx context.Context
	if len(requestCtxs) > 0 {
		requestCtx = requestCtxs[0]
	} else {
		requestCtx = context.Background()
	}

	// rpc reply timeout
	timeoutCtx, timeoutDone := context.WithDeadline(context.Background(), msg.MsgDeadline)
	defer timeoutDone()

	// reply closure = fwd msg to user
	replyCtx, replyReceived := context.WithCancel(context.Background())
	b.outClosures.Store(msg.MsgID, func(ret any) {
		replyReceived()
		userClosure(ret)
	})

	// send to bus
	err := b.Publish(msg)
	if err != nil {
		replyClosure, ok := b.outClosures.LoadAndDelete(msg.MsgID)
		if !ok {
			<-replyCtx.Done()
			return
		}
		replyClosure(err)
		return
	}

	// wait for reply
	select {
	case <-requestCtx.Done(): // client gave up
		replyClosure, ok := b.outClosures.LoadAndDelete(msg.MsgID)
		if !ok {
			return
		}
		replyClosure(ErrRequestCancelled)
	case <-timeoutCtx.Done(): // reply too slow
		closure, ok := b.outClosures.LoadAndDelete(msg.MsgID)
		if !ok {
			return
		}
		closure(ErrReplyTimeout)
	case <-replyCtx.Done():
		// success
	}
}
