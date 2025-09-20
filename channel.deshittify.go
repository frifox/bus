package bus

import (
	"errors"

	"github.com/rabbitmq/amqp091-go"
)

// Hot take: config via struct > config via args

type channel struct {
	*amqp091.Channel
}

// qosOpts = amqp091.basicQos{}
type qosOpts struct {
	PrefetchSize  int // (max bytes) will be conv to uint32() in amqp/Channel.Qos(...)
	PrefetchCount int // (max msg) will be conv to uint16() in amqp/Channel.Qos(...)
	Global        bool
}

func (c *channel) qos(o qosOpts) error {
	return c.Channel.Qos(o.PrefetchCount, o.PrefetchSize, o.Global)
}

// queueDeclareOpts = amqp091.queueDeclare{}
type queueDeclareOpts struct {
	Queue      string // rmq queue name
	Durable    bool   // keep queue on rmq restart
	AutoDelete bool   // delete queue if no consumers
	Exclusive  bool   // queue only on this conn, other conn with err
	NoWait     bool   // assume queue is declared, err if re-declared differrently
	Arguments  amqp091.Table
	//Passive    bool // Use ch.QueueDeclarePassive(...); true = will err if queue doesn't exist
}

func (c *channel) queueDeclare(o queueDeclareOpts) (amqp091.Queue, error) {
	return c.Channel.QueueDeclare(o.Queue, o.Durable, o.AutoDelete, o.Exclusive, o.NoWait, o.Arguments)
}

// consumeOpts = amqp091.basicConsume{}
type consumeOpts struct {
	Queue       string // rmq queue name
	ConsumerTag string // consumer name
	NoAck       bool   // ie: autoAck (false=consumer has to Ack)
	Exclusive   bool   // force only 1 consumer for this queue
	NoLocal     bool   // not supported...
	NoWait      bool   // begin deliveries immediately, don't wait for server to confirm request
	Arguments   amqp091.Table
}

func (c *channel) consume(o consumeOpts) (<-chan amqp091.Delivery, error) {
	return c.Channel.Consume(o.Queue, o.ConsumerTag, o.NoAck, o.Exclusive, o.NoLocal, o.NoWait, o.Arguments)
}

// publishOpts = amqp091.basicPublish{}
type publishOpts struct {
	Exchange   string
	RoutingKey string
	Mandatory  bool
	Immediate  bool
	Publishing *amqp091.Publishing
}

func (c *channel) publishWithDeferredConfirm(o publishOpts) (*amqp091.DeferredConfirmation, error) {
	if o.Publishing == nil {
		return nil, errors.New("nil publishing")
	}
	return c.Channel.PublishWithDeferredConfirm(o.Exchange, o.RoutingKey, o.Mandatory, o.Immediate, *o.Publishing)
}

// amqp091.Publishing is mostly basicPublish.properties:
//   basicPublish {
//     ...
//	   properties{
//		 ContentType:     msg.ContentType, 		// (string) MIME content type
//		 ContentEncoding: msg.ContentEncoding,	// (string) MIME content encoding
//		 Header:         msg.Header,			// (amqp091.Table) Application or header exchange table
//		 DeliveryMode:    msg.DeliveryMode,		// (uint8) queue implementation use - Transient (1) or Persistent (2)
//		 Priority:        msg.Priority,			// (uint8) queue implementation use - 0 to 9
//		 Expiration:      msg.Expiration,		// (string) implementation use - message expiration spec
//
//		 CorrelationId:   msg.CorrelationId,		// (string) application use - correlation identifier
//		 MessageId:       msg.MessageId,			// (string) application use - message identifier
//		 ReplyTo:         msg.ReplyTo,			// (string) application use - address to reply to (ex: RPC)
//		 Timestamp:       msg.Timestamp,			// (time.Time) application use - message timestamp
//		 Type:            msg.Type,				// (string) application use - message type name
//		 UserId:          msg.UserId,			// (string) application use - creating user MsgID
//		 AppId:           msg.AppId,				// (string) application use - creating application
//	   }
//   }
