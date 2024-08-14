package index

import (
	"encoding/json"
	"log"
	"slices"
	"strconv"
	"time"

	"fts.io/analyzer"
	"fts.io/utils"
	"github.com/linxGnu/grocksdb"
)

const (
	cfhDbMeta = iota
	cfhInvertedIdx
	cfhDocuments
)

var cfNames = []string{"default", "meta", "index", "docs"}

type Index struct {
	ro  *grocksdb.ReadOptions
	wo  *grocksdb.WriteOptions
	cfh grocksdb.ColumnFamilyHandles
	db  *grocksdb.DB
}

type Document struct {
	Title string `xml:"title" json:"title"`
	URL   string `xml:"url" json:"url"`
	Text  string `xml:"abstract" json:"abstract"`
	ID    int    `json:"id"`
}

type DbStateMeta struct {
	LastUpdatedAt string `json:"lastUpdatedAt"`
	Count         int    `json:"count"`
}

func getDbOptions() *grocksdb.Options {
	bbto := grocksdb.NewDefaultBlockBasedTableOptions()
	bbto.SetBlockCache(grocksdb.NewLRUCache(3 << 30))
	bbto.SetBlockSize(32 << 10) // 32kB
	bbto.SetFilterPolicy(grocksdb.NewBloomFilter(float64(10)))

	opts := grocksdb.NewDefaultOptions()
	opts.SetBlockBasedTableFactory(bbto)
	opts.SetCreateIfMissing(true)
	opts.SetCreateIfMissingColumnFamilies(true)

	return opts
}

func New(dataDir string) (*Index, error) {

	opts := getDbOptions()

	cfOptions := []*grocksdb.Options{opts, opts, opts, opts}

	db, cfh, err := grocksdb.OpenDbColumnFamilies(opts, dataDir, cfNames, cfOptions)

	if err != nil {
		return nil, err
	}

	return &Index{
		ro:  grocksdb.NewDefaultReadOptions(),
		wo:  grocksdb.NewDefaultWriteOptions(),
		cfh: cfh,
		db:  db,
	}, nil

}

func (d *Index) NewWriteBatch() *grocksdb.WriteBatch {
	return grocksdb.NewWriteBatch()
}

func (d *Index) saveToInvertedIndex(key string, value string) {

	d.db.PutCF(d.wo, d.cfh[cfhInvertedIdx], []byte(key), []byte(value))

}

func (d *Index) BulkSave(wb *grocksdb.WriteBatch) int {
	d.db.Write(d.wo, wb)

	return wb.Count()
}

func (d *Index) WriteDocumentsBatch(wb *grocksdb.WriteBatch, key string, value []byte) {
	wb.PutCF(d.cfh[cfhDocuments], []byte(key), value)
}

func (d *Index) GetMostRecentDocument() []byte {
	it := d.db.NewIteratorCF(d.ro, d.cfh[cfhDocuments])

	defer it.Close()

	for it.SeekToLast(); it.Valid(); {
		value := it.Value().Data()
		return value
	}

	return nil

}

func (d *Index) GetMostRecentIndexedToken() []byte {
	it := d.db.NewIteratorCF(d.ro, d.cfh[cfhInvertedIdx])

	defer it.Close()

	for it.SeekToLast(); it.Valid(); {
		value := it.Value().Data()
		return value
	}

	return nil

}

func (d *Index) GetDocument(docId string) []byte {
	val, _ := d.db.GetCF(d.ro, d.cfh[cfhDocuments], []byte(docId))
	// defer val.Free()

	return val.Data()
}

func (d *Index) GetFromInvertedIndex(key string) string {

	val, _ := d.db.GetCF(d.ro, d.cfh[cfhInvertedIdx], []byte(key))
	defer val.Free()

	if val.Exists() {
		return string(val.Data())
	}

	return ""
}

func (d *Index) SaveMeta(fieldName string, count int) {
	now := time.Now().UnixMilli()
	nowStr := strconv.Itoa(int(now))

	meta := DbStateMeta{
		LastUpdatedAt: nowStr,
		Count:         count,
	}

	val, err := json.Marshal(meta)

	if err != nil {
		log.Println("Failed to marshal dbState meta", err)

		return
	}

	err = d.db.PutCF(d.wo, d.cfh[cfhDbMeta], []byte(fieldName), val)

	if err != nil {
		log.Printf("Failed to update meta for %s \n", fieldName)
	}

}

func (d *Index) GetMeta(fieldName string) DbStateMeta {

	val, _ := d.db.GetCF(d.ro, d.cfh[cfhDbMeta], []byte(fieldName))

	defer val.Free()

	data := val.Data()

	if data == nil {
		log.Fatalf("Failed to get meta for %s \n", fieldName)
	}

	var dbMeta DbStateMeta

	json.Unmarshal(data, &dbMeta)

	return dbMeta

}

func (d *Index) IndexDocument(doc Document) {

	for _, token := range analyzer.Analyze(doc.Text) {

		value := d.GetFromInvertedIndex(token)
		indexValues := ""

		if value == "" {

			indexValues = strconv.Itoa(doc.ID)

		} else {
			// index values exist
			ids := utils.StringToIntArray(value)

			// we've saved this id before this token
			if slices.Contains(ids, doc.ID) {

				continue
			}

			indexValues = utils.IntArrayToString(append(ids, doc.ID))

		}

		d.saveToInvertedIndex(token, indexValues)

	}
}

func (d *Index) DropIndex() {
	err := d.db.DropColumnFamily(d.cfh[cfhInvertedIdx])

	if err != nil {
		log.Println("Failed to drop index", err)
	}

}

func (d *Index) DropDocuments() {
	err := d.db.DropColumnFamily(d.cfh[cfhDocuments])

	if err != nil {
		log.Println("Failed to drop index", err)
	}

}

func (d *Index) BuildIndex2() {
	it := d.db.NewIteratorCF(d.ro, d.cfh[cfhDocuments])

	defer it.Close()

	// iterate all the keys in the documents columnFamilyHandle
	for it.SeekToFirst(); it.Valid(); it.Next() {

		key := string(it.Key().Data())
		value := it.Value().Data()

		var doc Document

		err := json.Unmarshal(value, &doc)

		if err != nil {
			log.Println("Failed to Unmarshal document with id", key)
			continue
		}

		d.IndexDocument(doc)

	}

}

func (d *Index) BuildIndex() int {

	it := d.db.NewIteratorCF(d.ro, d.cfh[cfhDocuments])

	defer it.Close()

	inMemoryIndex := make(map[string][]int)

	for it.SeekToFirst(); it.Valid(); it.Next() {

		key := string(it.Key().Data())
		value := it.Value().Data()

		var doc Document

		err := json.Unmarshal(value, &doc)

		if err != nil {
			log.Println("Failed to Unmarshal document with id", key)
			continue
		}

		for _, token := range analyzer.Analyze(doc.Text) {

			ids, ok := inMemoryIndex[token]

			if ok && slices.Contains(ids, doc.ID) {
				continue
			}

			inMemoryIndex[token] = append(ids, doc.ID)

		}

	}

	wb := d.NewWriteBatch()
	defer wb.Destroy()

	for token, ids := range inMemoryIndex {

		key := []byte(token)
		values := []byte(utils.IntArrayToString(ids))

		wb.PutCF(d.cfh[cfhInvertedIdx], key, values)
	}

	count := d.BulkSave(wb)

	return count

}