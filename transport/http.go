package transport

import (
	"fmt"
	"net/http"

	"github.com/alarmfox/distributed-kv/controller"
)

func MakeHTTPHandler(c *controller.Controller) *http.ServeMux {
	r := http.NewServeMux()
	r.HandleFunc("/get", getHandler(c))
	r.HandleFunc("/set", setHandler(c))
	return r
}

func getHandler(c *controller.Controller) http.HandlerFunc {
	return func(rw http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			http.Error(rw, fmt.Sprintf("invalid query params: %v", err), http.StatusBadRequest)
			return
		}
		key := r.Form.Get("key")

		if key == "" {
			http.Error(rw, "invalid query params", http.StatusBadRequest)
			return
		}

		res, err := c.Get(key)

		if err != nil {
			http.Error(rw, "unknown error", http.StatusInternalServerError)
			return
		}

		rw.Write(res)
	}
}

func setHandler(c *controller.Controller) http.HandlerFunc {
	return func(rw http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			http.Error(rw, fmt.Sprintf("invalid query params: %v", err), http.StatusBadRequest)
			return
		}
		key := r.Form.Get("key")
		val := r.Form.Get("value")

		if key == "" || val == "" {
			http.Error(rw, "invalid query params", http.StatusBadRequest)
			return
		}

		if err := c.Set(key, []byte(val)); err != nil {
			http.Error(rw, fmt.Sprintf("cannot store: %v", err), http.StatusBadRequest)
			return
		}

		rw.WriteHeader(http.StatusNoContent)
	}
}
