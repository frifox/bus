package bus

import (
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"net/url"
	"reflect"
	"slices"
)

func (b *Bus) HandlerFuncs() map[string][]string {
	funcs := map[string][]string{}

	for queue, handlerFuncs := range b.handlers {
		funcs[queue] = append(funcs[queue], slices.Collect(maps.Keys(handlerFuncs))...)
	}

	return funcs
}

func (b *Bus) consumeMsgViaHandler(handler Handler, msg *Message) *reflect.Value {
	fn, ok := handler[msg.ToFunc]
	if !ok {
		ret := reflect.ValueOf(&CustomResponse{
			StatusCode: http.StatusNotImplemented, // 501
			Type:       "error",
			Body:       errors.New("handler func not found"),
		})
		return &ret
	}

	// make func args
	args, err := b.buildFuncArgs(msg, fn)
	if err != nil {
		// bad json (or failed to build args)
		ret := reflect.ValueOf(&CustomResponse{
			StatusCode: http.StatusBadRequest, // 400
			Type:       "error",
			Body:       err,
		})
		return &ret
	}

	// call func
	Slog.Debug("calling handler method", "remoteFunc", msg.ToFunc, "len(args)", len(args))
	rets := fn.Func.Call(args)
	if len(rets) != 1 {
		Slog.Error("unexpected amount of returns", "func", msg.ToFunc, "len", len(rets))
	}
	ret := rets[0]

	return &ret
}

func (b *Bus) buildFuncArgs(msg *Message, fn HandlerFunc) ([]reflect.Value, error) {
	// build args for HandlerFunc
	args := []reflect.Value{}
	for _, arg := range fn.Args {

		// fn() args checked at boot via handlerFuncIsSupported()
		switch arg.Kind() {
		case reflect.Map:
			switch arg.String() {
			case "http.Header":
				args = append(args, reflect.ValueOf(msg.Header))
			case "url.Values":
				u := url.URL{RawQuery: msg.Header.Get("RawQuery")}
				args = append(args, reflect.ValueOf(u.Query()))
			default:
				argTyped := reflect.New(arg)
				err := json.Unmarshal(msg.Body, argTyped.Interface())
				if err != nil {
					return nil, fmt.Errorf("json.Unmarshal(body, map): %w", err)
				}
				args = append(args, argTyped.Elem())
			}
		case reflect.Interface:
			switch arg.String() {
			case "context.Context":
				args = append(args, reflect.ValueOf(msg.ctx))
			default:
				Slog.Error("unsupported interface arg", "type", arg.String())
				return nil, fmt.Errorf("unsupported interface arg type: %s", arg.String())
			}
		case reflect.String:
			args = append(args, reflect.ValueOf(string(msg.Body)))
		case reflect.Slice:
			el := arg.Elem()

			switch el.Kind() {
			case reflect.Uint8: // []byte
				args = append(args, reflect.ValueOf(msg.Body))
			case reflect.String: // []string
				argTyped := reflect.New(arg)
				err := json.Unmarshal(msg.Body, argTyped.Interface())
				if err != nil {
					return nil, fmt.Errorf("json.Unmarshal(body, []string): %w", err)
				}
				args = append(args, argTyped.Elem())
			case reflect.Struct: // []struct{}
				argTyped := reflect.New(arg)
				err := json.Unmarshal(msg.Body, argTyped.Interface())
				if err != nil {
					return nil, fmt.Errorf("json.Unmarshal(body, []struct): %w", err)
				}
				args = append(args, argTyped.Elem())
			default:
				Slog.Error("unsupported slice arg type", "type", arg.String())
				return nil, fmt.Errorf("unsupported slice arg type: %s", arg.String())
			}
		case reflect.Struct:
			argTyped := reflect.New(arg)
			err := json.Unmarshal(msg.Body, argTyped.Interface())
			if err != nil {
				return nil, fmt.Errorf("json.Unmarshal(body, struct): %w", err)
			}
			args = append(args, argTyped.Elem())
		case reflect.Ptr:
			// pointer? allow only *struct{}
			switch arg.Elem().Kind() {
			case reflect.Struct:
				argTyped := reflect.New(arg)
				err := json.Unmarshal(msg.Body, argTyped.Interface())
				if err != nil {
					return nil, fmt.Errorf("json.Unmarshal(body, *struct): %w", err)
				}
				args = append(args, argTyped.Elem())
			default:
				Slog.Error("unsupported ptr arg type", "type", arg.String())
				return nil, fmt.Errorf("unsupported ptr arg type: %s", arg.String())
			}
		default:
			Slog.Error("unsupported arg type", "type", arg.String())
			return nil, fmt.Errorf("unsupported arg type: %s", arg.String())
		}
	}

	if len(args) != len(fn.Args) {
		return nil, errors.New("failed to build func args")
	}

	return args, nil
}
