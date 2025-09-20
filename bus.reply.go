package bus

import (
	"encoding/json"
	"net/http"
	"reflect"
)

func (b *Bus) sendHandlerReturnToBus(msg *Message, ret *reflect.Value) {
	if msg.ctx.Err() != nil {
		Slog.Warn("not replying, msg ctx is cancelled", "ToQueue", msg.ToQueue, "ToFunc", msg.ToFunc, "ReplyTo", msg.ReplyTo)
		return
	}

	// user triggered cancel?
	cancel, found := b.inContextCancels.LoadAndDelete(msg.MsgID)
	if !found {
		Slog.Warn("not replying, user triggered ctx cancel", "ToQueue", msg.ToQueue, "ToFunc", msg.ToFunc, "ReplyTo", msg.ReplyTo)
		return
	}
	cancel() // gc cancel() func, no turning back now, WILL publish reply

	// prepare reply
	reply := Message{
		BusMsgType: MsgTypeResponse,
		ToQueue:    msg.ReplyTo,
		MsgID:      msg.MsgID,
		MsgTTL:     b.MsgTTL,
		Header:     http.Header{},
	}

	// format reply
	if !ret.IsNil() {
		body := ret.Elem()

		// CustomResponse: custom StatusCode, Header, Type
		customResponseType := reflect.TypeFor[CustomResponse]()
		customResponsePtrType := reflect.TypeFor[*CustomResponse]()
		switch body.Type() {
		case customResponseType, customResponsePtrType:
			var customResponse *CustomResponse

			switch body.Type() {
			case customResponseType:
				notPtr := body.Convert(customResponseType).Interface().(CustomResponse)
				customResponse = &notPtr
			case customResponsePtrType:
				customResponse = body.Convert(customResponsePtrType).Interface().(*CustomResponse)
			}

			if customResponse == nil {
				break // shouldn't happen
			}

			if customResponse.Header != nil {
				reply.Header = customResponse.Header
			}
			if customResponse.StatusCode != 0 {
				reply.StatusCode = customResponse.StatusCode
			}
			if customResponse.Type != "" {
				reply.Type = customResponse.Type
			}

			body = reflect.ValueOf(customResponse.Body)
		}

		if body.IsValid() {
			// body is error?
			if body.CanInterface() {
				if err, ok := body.Interface().(error); ok {
					// by default, errors are HTTP 400
					if reply.StatusCode == 0 {
						reply.StatusCode = http.StatusBadRequest // 400
					}
					reply.Type = MessageTypeError
					body = reflect.ValueOf(err.Error())
				}
			}

			// detect body type
			bodyType := body.Type()
			if bodyType.Kind() == reflect.Ptr {
				bodyType = bodyType.Elem()
			}
			switch bodyType.Kind() {
			case reflect.String:
				reply.Body = []byte(body.Interface().(string))
			case reflect.Slice:
				sliceType := bodyType.Elem()
				switch sliceType.Kind() {
				case reflect.Uint8:
					reply.Type = "[]byte"
					reply.Body = body.Interface().([]byte)
				default: // hope it marshals
					reply.Header.Set("Content-Type", "application/json")

					if body.Len() == 0 {
						reply.Body = []byte("[]") // avoid "null" as Body
					} else {
						bodyBytes, err := json.Marshal(body.Interface())
						if err != nil {
							Slog.Error("failed to marshal map to json", "err", err)
							break
						}
						reply.Body = bodyBytes
					}
				}
			case reflect.Map:
				reply.Header.Set("Content-Type", "application/json")

				bodyBytes, err := json.Marshal(body.Interface())
				if err != nil {
					Slog.Error("failed to marshal map to json", "err", err)
					break
				}
				reply.Body = bodyBytes
			case reflect.Struct:
				reply.Header.Set("Content-Type", "application/json")

				// pretty-print type name, if not already done
				if reply.Type == "" {
					if body.Kind() == reflect.Ptr && !body.IsNil() {
						reply.Type = body.Elem().Type().Name()
					} else {
						reply.Type = body.Type().Name()
					}
				}
				bodyBytes, err := json.Marshal(body.Interface())
				if err != nil {
					Slog.Error("failed to marshal struct to json", "err", err)
					break
				}
				reply.Body = bodyBytes
			default:
				Slog.Error("unexpected ret type", "type", ret.Kind().String())
			}
		}
	}

	if reply.StatusCode == 0 {
		reply.StatusCode = http.StatusOK // 200
	}

	err := b.Publish(&reply)
	if err != nil {
		Slog.Error("failed to publish reply", "err", err)
		return
	}

	Slog.Info("published reply to bus", "replyTo", msg.ReplyTo, "MsgID", reply.MsgID, "reply", string(reply.Body))
}
