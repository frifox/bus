package bus

import (
	"github.com/rabbitmq/amqp091-go"
)

func (b *Bus) bindChannelEvents(c *channel, name string) {
	{
		ch := make(chan *amqp091.Error)
		c.NotifyClose(ch)
		go func() {
			for msg := range ch {
				Slog.Debug("NotifyClose", "chan", name, "error", msg)
			}
		}()
	}

	// on false = pause all publishers
	// on true = resume
	{
		ch := make(chan bool)
		c.NotifyFlow(ch)
		go func() {
			for msg := range ch {
				Slog.Debug("NotifyFlow", "chan", name, "flow", msg)
			}
		}()
	}

	// undeliverable due to mandatory/immediate flag (contains Message+error)
	{
		ch := make(chan amqp091.Return)
		c.NotifyReturn(ch)
		go func() {
			for msg := range ch {
				Slog.Debug("NotifyReturn", "chan", name, "return", msg)
			}
		}()
	}

	// when queue is deleted, or where master of mirrored queue failed/moved to another node
	{
		ch := make(chan string)
		c.NotifyCancel(ch)
		go func() {
			for msg := range ch {
				Slog.Debug("NotifyCancel", "chan", name, "message", msg)
			}
		}()
	}

	// async notify Publish
	{
		chAck := make(chan uint64)
		chNack := make(chan uint64)
		c.NotifyConfirm(chAck, chNack)
		go func() {
			for msg := range chAck {
				Slog.Debug("NotifyConfirm", "chan", name, "ack", msg)
			}
		}()
		go func() {
			for msg := range chNack {
				Slog.Debug("NotifyConfirm", "chan", name, "nack", msg)
			}
		}()
	}

	// chan confirmations for every publishing. Good to wait for all confirms before closing chan/conn
	{
		ch := make(chan amqp091.Confirmation)
		c.NotifyPublish(ch)
		go func() {
			for msg := range ch {
				Slog.Debug("NotifyPublish", "chan", name, "confirmation", msg)
			}
		}()
	}
}
