package engine

import (
	"compress/gzip"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"log"
	"os"
	"sort"
	"strconv"
	"time"

	"fts.io/analyzer"
	"fts.io/index"
	"fts.io/utils"
)

type FtsEngine struct {
	index *index.Index
}

type SearchResults struct {
	Meta struct {
		Count         int    `json:"totalResultsCount"`
		PageCount     int    `json:"currentPageCount"`
		TimeTakenSecs string `json:"timeTaken"`
		Query         string `json:"searchQuery"`
		Page          int    `json:"page"`
		Limit         int    `json:"limit"`
	} `json:"meta"`
	Data []index.Document `json:"data"`
}

type EngineStats struct {
	Docs   index.DbStateMeta `json:"docs"`
	Tokens index.DbStateMeta `json:"tokens"`
}

type SearchParams struct {
	Page  int
	Limit int
	Query string
	Exact bool
}

func (f *FtsEngine) buildIndex() {

	metaFieldName := "indexMeta"

	lastToken := f.index.GetMostRecentIndexedToken()

	if lastToken != nil {
		meta := f.index.GetMeta(metaFieldName)

		log.Printf("Index was  built at  %v, skipping ⏭️⏭️ \n", meta.LastUpdatedAt)
		log.Printf("Index contains %v tokens \n", meta.Count)

		return
	}

	ch := make(chan bool)

	go utils.Spinner(ch, "Building Index...")

	start := time.Now()

	recordsCount := f.index.BuildIndex()
	f.index.SaveMeta(metaFieldName, recordsCount)

	ch <- true
	log.Printf("\n Index built in %s ✅✅. No of tokens = %v  \n", time.Since(start), recordsCount)

}

func (f *FtsEngine) getDocs(text string, exactResults bool) []index.Document {

	var r []int

	searchTokens := analyzer.Analyze(text)

	for _, token := range searchTokens {
		val := f.index.GetFromInvertedIndex(token)

		if val == "" {
			continue

		}

		ids := utils.StringToIntArray(val)

		if r == nil {
			r = ids
		} else {
			if exactResults {
				r = utils.Intersection(r, ids) // exact matches
			} else {
				r = append(r, ids...) // partial to exact matches
			}
		}

	}

	docs := []index.Document{}

	for _, id := range r {
		doc, err := f.index.GetDocument(strconv.Itoa(id))

		if err != nil {
			log.Printf("Failed to retrieve docId: %v , err: %s \n", id, err)
			continue
		}

		// assign default rank
		doc.Rank = 1

		docs = append(docs, doc)

	}

	// no need to rank if results are exact matches
	if exactResults {
		return docs
	}

	return f.rankResults(docs, searchTokens)

}

func (f *FtsEngine) rankResults(docs []index.Document, searchTokens []string) []index.Document {

	// for each document, calculate the relevance by counting the amount of
	// searchTokens that can be found in the document.Text
	for i := range docs {
		score := 0

		resultTokens := analyzer.Analyze(docs[i].Text)

		resultTokensMap := make(map[string]bool)

		for _, resultToken := range resultTokens {
			resultTokensMap[resultToken] = true
		}

		for _, searchToken := range searchTokens {
			if _, exists := resultTokensMap[searchToken]; exists {
				score++
			}

		}

		docs[i].Rank = (float64(score) / float64(len(searchTokens)))
	}

	// order by relevance...
	sort.Slice(docs, func(i, j int) bool {
		return docs[i].Rank > docs[j].Rank
	})

	return docs

}

func (f *FtsEngine) Search(params *SearchParams) []byte {

	startTime := time.Now()

	docs := f.index.GetCachedSearchResults(params.Query, params.Exact)

	if len(docs) == 0 {
		docs = f.getDocs(params.Query, params.Exact)
		// save search results for caching and pagination purposes
		f.index.CacheSearchResults(params.Query, docs, params.Exact)
	}

	results := SearchResults{}

	start := (params.Page - 1) * params.Limit
	end := start + params.Limit

	// handle out of bounds
	if end > len(docs) {
		end = len(docs)

		if start > end {
			start = end
		}
	}

	results.Data = docs[start:end]
	results.Meta.Count = len(docs)
	results.Meta.Query = params.Query
	results.Meta.Limit = params.Limit
	results.Meta.Page = params.Page
	results.Meta.PageCount = len(results.Data)
	results.Meta.TimeTakenSecs = fmt.Sprintf("%.9f seconds", time.Since(startTime).Seconds())

	res, err := json.Marshal(results)

	if err != nil {
		log.Println("Failed to json.Marshal results", err)

		return make([]byte, 0)
	}

	return res

}

func (f *FtsEngine) loadDocuments(path string) error {

	log.Println("Starting...")

	metaFieldName := "docsMeta"
	lastDocument := f.index.GetMostRecentDocument()

	if lastDocument != nil {

		docsMeta := f.index.GetMeta(metaFieldName)

		log.Printf("Documents already loaded in database at %v. Skipping ⏭️⏭️ \n", docsMeta.LastUpdatedAt)
		log.Printf("Documents count = %v \n", docsMeta.Count)

		return nil
	}

	ch := make(chan bool)

	go utils.Spinner(ch, "Loading Documents...")

	start := time.Now()
	file, err := os.Open(path)

	if err != nil {
		return err
	}

	defer file.Close()

	gz, err := gzip.NewReader(file)

	if err != nil {
		return err
	}

	defer gz.Close()

	decoder := xml.NewDecoder(gz)

	dump := struct {
		Documents []index.Document `xml:"doc"`
	}{}

	if err := decoder.Decode(&dump); err != nil {
		return err
	}

	wb := f.index.NewWriteBatch()
	defer wb.Destroy()

	for i := range dump.Documents {
		dump.Documents[i].ID = i

		docJson, err := json.Marshal(dump.Documents[i])

		if err != nil {
			log.Printf("Failed to marshal document at index %v \n", i)
			continue
		}

		key := strconv.Itoa(i)
		f.index.WriteDocumentsBatch(wb, key, docJson)
	}

	recordsCount := f.index.BulkSave(wb)
	f.index.SaveMeta(metaFieldName, recordsCount)

	log.Printf("\n %v Documents loaded in %s ✅✅ \n", recordsCount, time.Since(start))

	ch <- true

	return nil
}

func (f *FtsEngine) GetStats() []byte {

	docsMeta := f.index.GetMeta("docsMeta")
	indexMeta := f.index.GetMeta("indexMeta")

	stats := EngineStats{
		Docs:   docsMeta,
		Tokens: indexMeta,
	}
	val, err := json.Marshal(stats)

	if err != nil {
		return nil
	}

	return val

}

func New(path string, dataDir string) (*FtsEngine, error) {

	index, err := index.New(dataDir)

	if err != nil {
		return nil, err
	}

	engine := &FtsEngine{
		index: index,
	}

	err = engine.loadDocuments(path)

	if err != nil {
		return nil, err
	}

	engine.buildIndex()

	return engine, nil

}
