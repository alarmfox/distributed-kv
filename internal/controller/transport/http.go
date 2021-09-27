package transport

import (
	"fmt"
	"io"
	"log"
	"net/http"
)

type Service interface {
	GetShardAddress(string) (string, error)
}

func MakeHTTPHandler(service Service) *http.ServeMux {
	r := http.NewServeMux()
	r.HandleFunc("/get", handle(service))
	r.HandleFunc("/set", handle(service))
	return r
}

func handle(s Service) http.HandlerFunc {
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

		shardAddress, err := s.GetShardAddress(key)
		if err != nil {
			log.Printf("error: %v\n", err)
			http.Error(rw, "internal error", http.StatusInternalServerError)
			return
		}
		res, err := http.Get(fmt.Sprintf("http://%s/%s", shardAddress, r.RequestURI))

		if err != nil {
			log.Printf("http.Get: %v", err)
			http.Error(rw, "internal error", http.StatusInternalServerError)
			return
		}
		defer r.Body.Close()
		if body, err := io.ReadAll(res.Body); err == nil {
			rw.Write(body)
			return
		}
		log.Printf("io.ReadAll: %v", err)
		http.Error(rw, "internal error", http.StatusInternalServerError)

	}
}
