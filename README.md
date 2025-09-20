# Go Message Bus using RMQ3

# RPC Producer (api)
```go
// init and consume "api:callback" for RPC replies
rmq := bus.NewBus(bus.Opts{
    Context:       appCtx,
    DSN:           "amqp://guest:guest@localhost:5672",
    CallbackQueue: "api:callback",
    Prefetch:      200,
})
go rmq.Run()

// Option 1: make RPC, you implement timeout/replies/etc yourself
msg := bus.Message{
    ToQueue:     "worker_queue",
    ToFunc:      "FoobarFunc",
    MsgTTL:      time.Hour,
    Body:        []byte(`{"Foo":"bar"}`),
}
err := rmq.Publish(&msg)

// Option 2: make RPC, wait for response
responseMsg, err := rmq.PublishAndReturn(MyRequest{Foo:"bar"}, "worker_queue", "FoobarFunc", time.Second, time.Minute, r.Context())

// Option 3: make RPC, execute closure once response comes
wg := sync.WaitGroup{}
wg.Add(1)
closure := func(resp any) {
    defer wg.Done()
    
    switch resp := resp.(type) {
    case *bus.Message:
        // response from worker
    case error:
        switch {
        case errors.Is(resp, bus.ErrRequestCancelled)
            // r,Context() cancelled
            rmq.CancelMsg(msg, "worker:callback") // have worker cancel RPC ctx
        case errors.Is(resp, bus.ErrReplyTimeout)
            // MsgDeadline reached
        case errors.Is(resp, bus.ErrPublishing):
            // rmq err, couldn't publish msg
        }
    }
}
msg := bus.Message{
    ToQueue:     "worker_queue",
    ToFunc:      "FoobarFunc",
    MsgDeadline: time.Now().Add(time.Minute),
    MsgTTL:      time.Hour,
    Body:        []byte(`{"Foo":"bar"}`),
}
rmq.PublishAndClose(&msg, closure, r.Context())
wg.Wait()


```

## RPC Consumer (worker)

```go
// init and consume "worker:callback" for RPC replies/ctx cancels (from publisher)
bus := bus.NewBus(bus.Opts{
    Context:       appCtx,
    DSN:           "amqp://guest:guest@localhost:5672",
    CallbackQueue: "worker:callback",
    Prefetch:      200,
})

// bind &foobar to handle RPCs from "worker_queue" and a special "worker_queue:noprefetch" queues 
err := rmq.RegisterHandler(bus.HandlerOpts{
    Queue:    "worker.FoobarService",
    Prefetch: 10,
    Handler:  &foobar,
})

go rmq.Run()
```

# Consumer Funcs
```go
// RPC funcs must be Exported and have bus.Response return 
type FoobarService struct {}

func (f *FoobarService) ReturnBlank() bus.Response {
    return nil
}
func (f *FoobarService) ReturnBytes() bus.Response {
    return []byte("raw data")
}
func (f *FoobarService) ReturnString() bus.Response {
    return "ok"
}
func (f *FoobarService) ReturnMap() bus.Response {
    return map[string]any{
        "str": "string",
        "int": 123,
        "bool": true,
        "struct": MyStruct{},
    }
}
func (f *FoobarService) ReturnError() bus.Response {
    return errors.New("my error")
}
func (f *FoobarService) ReturnJson() bus.Response {
    // can be struct or *struct
    return MyResponse{
        Foo: "bar",
    }
}
func (f *FoobarService) ReturnCustomResponse() bus.Response {
    return bus.CustomResponse{
        StatusCode: 201,
        Header: http.Header{"Content-Type": {"application/xml"}},
        Body: `<?xml version = "1.0" encoding = "UTF-8"?>`,
    }        
}

func (f *FoobarService) GetString(body string) bus.Response {
    //
}
func (f *FoobarService) GetBytes(body []byte) bus.Response {
    //
}
func (f *FoobarService) GetUnmarshal(request MyRequest) bus.Response {
    // can be struct or *struct
}
func (f *FoobarService) GetHttpHeader(header http.Header) bus.Rseponse {
    // passed header via bus.Message{Header: r.Header}
}
func (f *FoobarService) GetQueryVars(query url.Values) bus.Response {
    // pass r.URL.RawQuery via "RawQuery" header, ie:
    // r.Header.Set("RawQuery", r.URL.RawQuery) and bus.Message{Header: r.Header}
}
func (f *FoobarService) GetRequestCtx(ctx context.Context) bus.Response {
    // currently ONLY for ctx.Cancels (no values, deadlines, etc)
    <- ctx.Done()
}
```

# Message Type

```go
// bus.Message
type Message struct {
    ToQueue string // remote.worker.queue
    ToFunc  string // RemoteFunc
    ReplyTo string // api:callback
    
    MsgID       string        // msg uuid
    MsgDeadline time.Time     // used to build remote ctx, ie RemoteFunc(ctx context.Context)
    MsgTTL      time.Duration // time msg lives in RMQ until expiring
    
    // HTTP related, usually in bus.Response
    StatusCode int         // HTTP StatusCode: 200, etc
    Header     http.Header // HTTP Headers: [MyHeader: foobar, ...]
    
    Type       string      // auto pretty-printed ResponseStructName or manually via CustomResponse{Type:___}
    Body       []byte      // raw bytes, usually marshalled struct
    
    // constructed in consumeMsg() off MsgDeadline, then passed to RemoteFunc(ctx)
    // and early cancelled if got CancelMsgSpecialFuncName
    ctx    context.Context
    cancel context.CancelFunc
}
```

# Bus Slog

```go

```