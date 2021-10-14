package stringz

import (
	"math/rand"
	"regexp"
	"strings"
	"time"
)

const (
	letterBytes = "abcdefghijklmnopqrstuvwxyz1234567890"
)

var (
	relc = regexp.MustCompile("[^a-z0-9-]+")
	remh = regexp.MustCompile("[-]{2,}")
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

// RandomStringWithCharset returns a random string with given length and letterBytes
func RandomStringWithCharset(length int, letterBytes string) string {
	b := make([]byte, length)
	for i := range b {
		b[i] = letterBytes[rand.Int63()%int64(len(letterBytes))]
	}

	return string(b)
}

// RandomString returns a random string with given length
func RandomString(length int) string {
	return RandomStringWithCharset(length, letterBytes)
}

// Sluggify will change string to slug
func Sluggify(s string) string {
	slug := strings.ToLower(s)

	// symbols to replace
	symbols := []string{"'", "\"", ".", "`", "’"}
	for _, symbol := range symbols {
		slug = strings.Replace(slug, symbol, "", -1)
	}

	slug = relc.ReplaceAllString(slug, "-")
	slug = strings.Trim(slug, "-")

	// remove multiple hyphen
	slug = remh.ReplaceAllString(slug, "-")

	return slug
}
