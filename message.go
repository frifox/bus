package bus

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/rabbitmq/amqp091-go"
)

var MessageTypeError = "error"

// RMQPublishing for reference only
type RMQPublishing struct {
	// rmq vars
	DeliveryMode uint8 // Transient (0 or 1) or Persistent (2)
	Expiration   string

	// bus vars
	Type            string // [Request, Response, CtxCancel]
	MessageId       string // MsgID
	CorrelationId   string // ToFuncName
	ReplyTo         string // reply queue
	ContentEncoding string // RFC3339Nano MsgTimeout for deadlines

	// request & replies
	Headers     amqp091.Table // map[string]any{nil,bool,byte,int,float,[]byte,Decimal,time.Time}; t.SetClientConnectionName() => t["connection_name"]=connName
	Body        []byte        // raw bytes
	ContentType string        // StructName (for replies)
	AppId       string        // http status code (for replies)

	// not used
	Priority  uint8     // 0 to 9
	Timestamp time.Time // truncated to 1s
	UserId    string    // creating user id - ex: "guest"
}

type Message struct {
	BusMsgType string // [Request, Response, CtxCancel]
	ToQueue    string // remote.worker.queue
	ToFunc     string // RemoteFunc
	ReplyTo    string // api:callback

	MsgID       string        // msg uuid
	MsgDeadline time.Time     // used to build/cancel req ctx, ie RemoteFunc(ctx context.Context)
	MsgTTL      time.Duration // time msg lives in RMQ before dying

	StatusCode int         // HTTP StatusCode: 200, etc
	Header     http.Header // HTTP Headers: [MyHeader: foobar, ...]
	Type       string      // pretty print StructName or CustomResponse{Type:___} string
	Body       []byte      // raw bytes, or marshalled struct

	// constructed in consumeMsg() off MsgDeadline, passed to RemoteFunc(ctx)
	// early cancelled if got BusMsgType=CtxCancel
	ctx    context.Context
	cancel context.CancelFunc
}

func (m *Message) ToPublishing() *amqp091.Publishing {
	pub := amqp091.Publishing{
		DeliveryMode: amqp091.Transient,
		Type:         m.BusMsgType, // Request/Response/CtxCancel

		MessageId:     m.MsgID,   // msg uuid
		CorrelationId: m.ToFunc,  // ToFuncName
		ReplyTo:       m.ReplyTo, // service:reply
		ContentType:   m.Type,    // StructName
	}

	// rmq MsgTTL
	pub.Expiration = strconv.FormatInt(m.MsgTTL.Milliseconds(), 10)

	// abuse ContentEncoding field for MsgTimeout,
	// consumer will use to auto-cancel ctx and not reply
	pub.ContentEncoding = m.MsgDeadline.Format(time.RFC3339Nano)

	// http headers
	if m.Header == nil {
		m.Header = http.Header{}
	}
	pub.Headers = amqp091.Table{}
	for key, vals := range m.Header {
		if len(vals) == 0 {
			continue
		}

		// strip delimiter from val
		for i, val := range vals {
			if strings.Contains(val, "\x00") {
				vals[i] = strings.ReplaceAll(val, "\x00", "")
			}
		}

		pub.Headers[key] = strings.Join(vals, "\x00")
	}

	if m.StatusCode != 0 {
		pub.AppId = strconv.Itoa(m.StatusCode)
	}

	pub.Body = m.Body

	return &pub
}

func (m *Message) FromDelivery(d *amqp091.Delivery) {
	m.BusMsgType = d.Type // Request/Response/CtxCancel
	m.MsgID = d.MessageId
	m.ToQueue = d.RoutingKey // usually unnecessary
	m.ToFunc = d.CorrelationId
	m.ReplyTo = d.ReplyTo
	m.Type = d.ContentType
	m.Header = http.Header{
		//"XID": []string{d.MessageId},
	}

	if d.ContentEncoding != "" {
		var err error
		m.MsgDeadline, err = time.Parse(time.RFC3339Nano, d.ContentEncoding)
		if err != nil {
			Slog.Error("time.Parse(ContentEncoding) as MsgDeadline", "err", err, "ContentEncoding", d.ContentEncoding)
		}
	}

	for k, v := range d.Headers {
		switch v := v.(type) {
		case string:
			vals := strings.Split(v, "\x00")
			if len(vals) == 0 {
				continue
			}
			for _, val := range vals {
				m.Header.Add(k, val)
			}
		default:
			Slog.Error("non-string header", "type", fmt.Sprintf("%T", v), "key", k, "value", v)
		}
	}

	httpStatusCode, _ := strconv.Atoi(d.AppId)
	if httpStatusCode != 0 {
		m.StatusCode = httpStatusCode
	}

	m.Body = d.Body
}
