package transport

import "net/http"

type Transport interface {
	RegisterHandlers(mux *http.ServeMux)
}
