package bus

import (
	"fmt"
	"maps"
	"reflect"
	"slices"
)

type HandlerOpts struct {
	Handler  any
	Queue    string
	Prefetch int
}

const NoPrefetchSuffix = ":noprefetch"

// RegisterHandler registers 1 handler (with many funcs) to handle msgs from 1 queue
func (b *Bus) RegisterHandler(opts HandlerOpts) error {
	Slog.Info("registerHandler", "opts", opts)

	hValue := reflect.ValueOf(opts.Handler)
	if hValue.Kind() != reflect.Ptr {
		return fmt.Errorf("handler is not a pointer, is %v", hValue.Kind())
	}
	if hValue.IsNil() {
		return fmt.Errorf("handler ptr is nil")
	}
	hType := reflect.TypeOf(opts.Handler)

	// register supported handler
	handlerFuncs := make(map[string]HandlerFunc)
	for i := 0; i < hValue.NumMethod(); i++ {
		fnType := hType.Method(i)
		fnValue := hValue.Method(i)
		fnValueType := fnValue.Type()

		if !handlerFuncIsSupported(fnValueType) {
			continue
		}

		// register func
		fn := HandlerFunc{
			Func: fnValue,
		}
		for i := 0; i < fnValueType.NumIn(); i++ {
			fn.Args = append(fn.Args, fnValueType.In(i))
		}
		for i := 0; i < fnValueType.NumOut(); i++ {
			fn.Returns = append(fn.Returns, fnValueType.Out(i))
		}

		handlerFuncs[fnType.Name] = fn
	}

	if len(handlerFuncs) == 0 {
		return fmt.Errorf("handler has no usable funcs")
	}

	Slog.Info("handler accepted", "funcs", slices.Collect(maps.Keys(handlerFuncs)))

	// bind to queue
	b.handlers[opts.Queue] = handlerFuncs
	b.inQueues[opts.Queue] = struct{ Prefetch int }{
		Prefetch: opts.Prefetch,
	}

	// bind to special queue for bypassing prefetch, ie replies/interrupts
	b.handlers[opts.Queue+NoPrefetchSuffix] = handlerFuncs
	b.inQueues[opts.Queue+NoPrefetchSuffix] = struct{ Prefetch int }{
		Prefetch: 0,
	}

	return nil
}

// handlerFuncIsSupported checks if handler Func format is supported (ie: has bus.Response return)
func handlerFuncIsSupported(mValType reflect.Type) bool {
	// func return must be bus.Response
	busResponseType := reflect.TypeFor[Response]()
	returns := mValType.NumOut()
	if returns != 1 {
		return false
	}
	ret := mValType.Out(0)
	if ret.String() != busResponseType.String() {
		return false
	}

	// check all args are supported by Bus.buildFuncArgs()
	for i := 0; i < mValType.NumIn(); i++ {
		arg := mValType.In(i)

		// MAKE SURE Bus.buildFuncArgs() actually handles theses!
		switch arg.Kind() {
		case reflect.Map:
			switch arg.String() {
			case "http.Header":
				// http header
			case "url.Values":
				// query vars
			default:
				// ie: body map[string]any,
				// will use json.Unmarshal(msg.Body, &arg)
			}
		case reflect.Interface:
			switch arg.String() {
			case "context.Context":
				// request ctx
			default:
				Slog.Error("unsupported interface arg", "arg", arg.String())
				return false
			}
		case reflect.String:
			// body string
		case reflect.Slice:
			el := arg.Elem()

			switch el.Kind() {
			case reflect.Uint8:
				// body []byte
			case reflect.String:
				// body []string
			case reflect.Struct:
				// body []Item
			default:
				Slog.Error("unsupported slice arg", "elem kind", el.Kind().String())
				return false
			}
		case reflect.Struct:
			// body MyStruct
		case reflect.Ptr:
			// body *something
			el := arg.Elem()
			switch el.Kind() {
			case reflect.Struct:
				// body *MyStruct
			default:
				// TODO support *string / *others?
				Slog.Error("unsupported ptr arg", "elem kind", el.Kind().String(), "arg", arg.String())
				return false
			}
		default:
			Slog.Error("unsupported arg", " kind", arg.Kind().String(), "arg", arg.String())
			return false
		}

		// arg is supported, check next
		continue

		// only 1 http.Header
		//if !headerFound {
		//	switch arg.Kind() {
		//	case reflect.Map:
		//		if arg.String() == "http.Header" {
		//			headerFound = true
		//			continue
		//		}
		//	}
		//}

		// only 1 url.Values
		//if !queryFound {
		//	switch arg.Kind() {
		//	case reflect.Map:
		//		if arg.String() == "url.Values" {
		//			queryFound = true
		//			continue
		//		}
		//	}
		//}

		// only 1 context.Context
		//if !ctxFound {
		//	switch arg.Kind() {
		//	case reflect.Interface:
		//		if arg.String() == "context.Context" {
		//			ctxFound = true
		//			continue
		//		}
		//	}
		//}

		// only 1 body
		//if !bodyFound {
		//	switch arg.Kind() {
		//case reflect.String:
		//	bodyFound = true
		//	continue
		//case reflect.Map:
		//	if arg.String() == "http.Header" {
		//		// don't allow duplicate headers
		//		return false
		//	}
		//	if arg.String() == "url.Values" {
		//		// don't allow duplicate query
		//		return false
		//	}
		//	bodyFound = true
		//	continue
		//case reflect.Slice:
		//	el := arg.Elem()
		//	//if el.Kind() == reflect.Ptr {
		//	//	el = el.Elem()
		//	//}
		//	switch el.Kind() {
		//	case reflect.Uint8:
		//		bodyFound = true // []byte
		//		continue
		//	case reflect.String:
		//		bodyFound = true // []string
		//		continue
		//	case reflect.Struct:
		//		bodyFound = true // []struct{}
		//		continue
		//	default:
		//
		//		return false
		//	}
		//case reflect.Struct:
		//	bodyFound = true
		//	continue
		//default:
		//	Slog.Error("unsupported arg kind", "kind", arg.Kind().String(), "fn", arg.String())
		//	return false
		//}
		//}

		// unsupported arg
		//return false
	}

	// all args supported, accept func
	return true
}
