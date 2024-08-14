package engine

import (
	"compress/gzip"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"fts.io/analyzer"
	"fts.io/index"
	"fts.io/utils"
)

type FtsEngine struct {
	index *index.Index
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
	fmt.Printf("\n Index built in %s ✅✅. No of tokens = %v  \n", time.Since(start), recordsCount)

}

func (f *FtsEngine) Search(text string) []byte {

	var r []int

	for _, token := range analyzer.Analyze(text) {
		val := f.index.GetFromInvertedIndex(token)

		if val == "" {
			return nil
		}

		ids := utils.StringToIntArray(val)

		if r == nil {
			r = ids
		} else {
			r = utils.Intersection(r, ids)
		}

	}

	var results []index.Document

	for _, id := range r {
		doc := f.index.GetDocument(strconv.Itoa(id))

		if doc == nil {
			fmt.Printf("Failed to find doc.id=%v in database \n", id)
			continue
		}

		var document index.Document

		json.Unmarshal(doc, &document)

		results = append(results, document)

	}

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

	fmt.Printf("\n %v Documents loaded in %s ✅✅ \n", recordsCount, time.Since(start))

	ch <- true

	return nil
}

func New(path string, dataDir string) (*FtsEngine, error) {

	index, err := index.New(dataDir)

	if err != nil {
		return nil, err
	}

	engine := FtsEngine{
		index: index,
	}

	err = engine.loadDocuments(path)

	if err != nil {
		return nil, err
	}

	engine.buildIndex()

	return &engine, nil

}
