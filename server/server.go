package server

import (
	"fmt"
	"log"
	"net/http"

	"fts.io/engine"
)

func Start(e *engine.FtsEngine, port string) {

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {

		searchTerm := r.URL.Query().Get("q")
		exact := r.URL.Query().Get("exact")

		if searchTerm == "" {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte("Search term cannot be blank"))

			return
		}

		exactResults := exact == "true"

		w.Header().Set("Content-Type", "application/json")

		w.Write(e.Search(searchTerm, exactResults))
	})

	log.Printf("Server is up and running 🚀🚀🚀 on %s \n", port)

	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%s", port), nil))
}
