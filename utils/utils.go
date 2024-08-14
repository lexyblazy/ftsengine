package utils

import (
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"
)

var separator string = ","

func Spinner(ch chan bool, message string) {
	symbols := []string{"🌑 ", "🌒 ", "🌓 ", "🌔 ", "🌕 ", "🌖 ", "🌗 ", "🌘 "}

	defer func() {
		close(ch)
	}()

	for {
		select {
		case <-ch:
			return
		default:
			for _, s := range symbols {
				fmt.Printf("\r %s %s", s, message)
				time.Sleep(100 * time.Millisecond)
			}

		}

	}

}
func IntArrayToString(numbers []int) string {

	str := make([]string, len(numbers))
	for i, number := range numbers {

		str[i] = strconv.Itoa(number)

	}

	return strings.Join(str, separator)

}

func StringToIntArray(str string) []int {
	strArray := strings.Split(str, separator)

	numbers := make([]int, len(strArray))

	for i, s := range strArray {
		val, err := strconv.Atoi(s)

		if err != nil {
			log.Fatal(err)
		}

		numbers[i] = val
	}

	return numbers
}

func Intersection(a []int, b []int) []int {

	// slices.Sort(a)
	// slices.Sort(b)
	// fmt.Println("a is sorted array", sort.IntsAreSorted(a))
	// fmt.Println("b is sorted array", sort.IntsAreSorted(b))

	maxLen := len(a)

	if len(b) > maxLen {
		maxLen = len(b)
	}

	r := make([]int, 0, maxLen)

	var i, j int
	for i < len(a) && j < len(b) {
		if a[i] < b[j] {
			i++
		} else if a[i] > b[j] {
			j++
		} else {
			r = append(r, a[i])
			i++
			j++
		}
	}
	return r
}
