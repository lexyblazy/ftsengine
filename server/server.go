package server

import (
	"fmt"
	"log"
	"net/http"
	"strconv"

	"fts.io/engine"
)

func Start(e *engine.FtsEngine, port string) {

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {

		searchTerm := r.URL.Query().Get("q")
		exact := r.URL.Query().Get("exact")
		page, pageErr := strconv.Atoi(r.URL.Query().Get("page"))
		limit, limitErr := strconv.Atoi(r.URL.Query().Get("limit"))

		if pageErr != nil {
			page = 1
		}

		if limitErr != nil {
			limit = 100
		}

		if searchTerm == "" {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte("Search term cannot be blank"))

			return
		}

		w.Header().Set("Content-Type", "application/json")

		params := &engine.SearchParams{
			Page:  page,
			Limit: limit,
			Query: searchTerm,
			Exact: exact == "true",
		}

		w.Write(e.Search(params))
	})

	log.Printf("Server is up and running 🚀🚀🚀 on %s \n", port)

	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%s", port), nil))
}
