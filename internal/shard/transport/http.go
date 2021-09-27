package transport

import (
	"errors"
	"fmt"
	"net/http"

	"gitlab.com/alarmfox/distributed-kv/internal/shard/storage"
)

type Storage interface {
	Get(key string) ([]byte, error)
	Set(key string, value []byte) error
}

func MakeHTTPHandler(storage Storage) *http.ServeMux {
	r := http.NewServeMux()
	r.HandleFunc("/get", get(storage))
	r.HandleFunc("/set", set(storage))
	return r
}

func get(st Storage) http.HandlerFunc {
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

		res, err := st.Get(key)

		if errors.Is(err, storage.ErrNotFound) {
			http.Error(rw, fmt.Sprintf("key %q not found", key), http.StatusNotFound)
			return
		}

		if err != nil {
			http.Error(rw, "unknown error", http.StatusInternalServerError)
			return
		}

		fmt.Fprintf(rw, fmt.Sprintf("key: %s; val: %s", key, string(res)))
	}
}

func set(storage Storage) http.HandlerFunc {
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

		if err := storage.Set(key, []byte(val)); err != nil {
			http.Error(rw, fmt.Sprintf("cannot store: %v", err), http.StatusBadRequest)
			return
		}

		fmt.Fprintf(rw, fmt.Sprintf("key: %s; val: %s", key, val))
	}
}