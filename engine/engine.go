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
		Count         int    `json:"count"`
		TimeTakenSecs string `json:"timeTaken"`
		Query         string `json:"searchQuery"`
	} `json:"meta"`
	Data []index.Document `json:"data"`
}

func (f *FtsEngine) buildIndex() {

	metaFieldName := "indexMeta"

	lastToken := f.index.GetMostRecentIndexedToken()

	if lastToken != nil {
		meta := f.index.GetMeta(metaFieldName)
		timestamp, _ := strconv.Atoi(meta.LastUpdatedAt)
		since := time.Since(time.UnixMilli(int64(timestamp))).Round(time.Second)

		log.Printf("Index already built  %v ago, skipping ⏭️⏭️ \n", since)
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

func (f *FtsEngine) Search(text string, exactResults bool) []byte {

	start := time.Now()
	docs := f.getDocs(text, exactResults)

	results := SearchResults{}

	results.Data = docs
	results.Meta.Count = len(docs)
	results.Meta.TimeTakenSecs = fmt.Sprintf("%.9f seconds", time.Since(start).Seconds())
	results.Meta.Query = text

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
		timestamp, _ := strconv.Atoi(docsMeta.LastUpdatedAt)
		since := time.Since(time.UnixMilli(int64(timestamp))).Round(time.Second)
		log.Printf("Documents already loaded in database, %v ago. Skipping ⏭️⏭️ \n", since)
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
