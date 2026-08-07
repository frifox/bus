package bus

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"
)

func (b *Bus) CallAndWait(queue string, funcName string, req any, timeout time.Duration, ttl time.Duration, reqCancels ...context.Context) (*Message, error) {
	// prepare closure
	wg := sync.WaitGroup{}
	wg.Add(1)
	var ret any
	closure := func(pubReturn any) {
		ret = pubReturn
		wg.Done()
	}

	// call rpc
	err := b.CallAndClose(queue, funcName, req, closure, timeout, ttl, reqCancels...)
	if err != nil {
		wg.Done() // if err, closure will not execute
	}

	// wait for response
	wg.Wait()

	// parse response
	switch ret := ret.(type) {
	case error: // publishing err
		return nil, ret
	case *Message: // worker reply
		return ret, nil
	default:
		return nil, errors.New("unexpected CallAndClose return")
	}
}

func (b *Bus) CallAndClose(queue string, funcName string, req any, closure func(any), timeout time.Duration, ttl time.Duration, reqCancels ...context.Context) error {
	if queue == "" {
		return errors.New("toQueue is empty")
	}
	if funcName == "" {
		return errors.New("toFunc is empty")
	}
	if timeout == 0 {
		Slog.Warn("msg with MsgTimeout=0. Falling back to 10s")
		timeout = time.Second * 10
	}
	if ttl == 0 {
		Slog.Warn("msg with MsgTTL=0. Falling back to 10m")
		ttl = time.Minute * 10
	}

	// prepare bus msg
	busMsg := Message{
		BusMsgType:  MsgTypeRequest,
		ToQueue:     queue,
		ToFunc:      funcName,
		MsgDeadline: time.Now().Add(timeout),
		MsgTTL:      ttl,
	}
	switch request := req.(type) {
	case nil:
		// empty body
	case []byte:
		busMsg.Body = request
	case string:
		busMsg.Body = []byte(request)
	default:
		data, err := json.Marshal(request)
		if err != nil {
			return fmt.Errorf("marshal(request): %w", err)
		}
		busMsg.Body = data
	}

	b.PublishAndClose(&busMsg, closure, reqCancels...)

	return nil
}

func (b *Bus) CallAsync(queue string, funcName string, req any, ttl time.Duration) error {
	if queue == "" {
		return errors.New("toQueue is empty")
	}
	if funcName == "" {
		return errors.New("toFunc is empty")
	}
	if ttl == 0 {
		Slog.Warn("msg with MsgTTL=0. Falling back to 10m")
		ttl = time.Minute * 10
	}

	busMsg := Message{
		BusMsgType: MsgTypeRequest,
		ToQueue:    queue,
		ToFunc:     funcName,
		MsgTTL:     ttl,
	}

	switch request := req.(type) {
	case nil:
		// no body
	case []byte:
		busMsg.Body = request
	case string:
		busMsg.Body = []byte(request)
	default:
		data, err := json.Marshal(request)
		if err != nil {
			return fmt.Errorf("marshal(request): %w", err)
		}
		busMsg.Body = data
	}

	return b.Publish(&busMsg)
}
