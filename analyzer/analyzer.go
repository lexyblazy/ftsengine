package analyzer

import (
	"github.com/kljensen/snowball/english"

	"strings"
	"unicode"
)

func getStopWords() map[string]bool {

	stopwords := [...]string{"a", "an", "and", "are", "as", "at", "be", "but", "by", "for", "if", "in", "into", "is", "it", "no", "not", "of", "on", "or", "such", "that", "the", "their", "then", "there", "these", "they", "this", "to", "was", "will", "with"}

	var sets = make(map[string]bool)

	for _, word := range stopwords {
		sets[word] = true

	}

	return sets
}

func tokenize(text string) []string {
	return strings.FieldsFunc(text, func(r rune) bool {

		return !unicode.IsLetter(r) && !unicode.IsNumber(r)

	})
}

func lowerCaseFilter(tokens []string) []string {
	result := make([]string, len(tokens))

	for i, token := range tokens {
		result[i] = strings.ToLower(token)
	}

	return result
}

func stopWordsFilters(tokens []string) []string {
	stopWords := getStopWords()

	result := make([]string, 0, len(tokens))

	for _, token := range tokens {

		if _, ok := stopWords[token]; !ok {
			result = append(result, token)
		}
	}

	return result
}

func stemmerFilter(tokens []string) []string {
	result := make([]string, len(tokens))

	for i, token := range tokens {

		result[i] = english.Stem(token, false)
	}

	return result
}

func Analyze(text string) []string {
	tokens := tokenize(text)
	tokens = lowerCaseFilter(tokens)
	tokens = stopWordsFilters(tokens)
	tokens = stemmerFilter(tokens)

	return tokens
}
