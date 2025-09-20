package bus

import (
	"context"
	"log/slog"
)

// Default behavior is to ignore all slogs.
// User can override it, ie for debugging:
//   bus.Slog = slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
//       Level: slog.LevelDebug,
//	 }))

var Slog *slog.Logger = slog.New(&NilHandler{})

type NilHandler struct {
	slog.Handler
}

func (*NilHandler) Enabled(_ context.Context, _ slog.Level) bool {
	return false
}
