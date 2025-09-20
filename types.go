package bus

import (
	"net/http"
)

// Response type will be auto-detected in bus.sendHandlerReturnToBus()
type Response any

// CustomResponse allows use to customize bus.Response reply
type CustomResponse struct {
	StatusCode int
	Header     http.Header
	Type       string
	Body       Response
}
