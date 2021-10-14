package stringz

import (
	"testing"
)

func TestRandomStringWithCharset(t *testing.T) {
	zeLen := 5
	randStr := RandomStringWithCharset(zeLen, "a")
	if len(randStr) != zeLen {
		t.Errorf("Length was incorrect, got: %d, want: %d.", len(randStr), zeLen)
	}

	expectedStr := "aaaaa"
	if randStr != expectedStr {
		t.Errorf("Random string incorrect, got: %s, want: %s", string(randStr), expectedStr)
	}
}

func TestRandomString(t *testing.T) {
	zeLen := 8
	randStr := RandomString(zeLen)
	if len(randStr) != zeLen {
		t.Errorf("Length was incorrect, got: %d, want: %d.", len(randStr), zeLen)
	}
}

func TestSluggify(t *testing.T) {
	str := "--Thi's i\"s a d.o.t w’ay--to  go!@#~with#$%^&*()_=+slug 01 --"
	expected := "this-is-a-dot-way-to-go-with-slug-01"

	slug := Sluggify(str)

	if slug != expected {
		t.Errorf("Incorrect slug, got: %s, expected: %s", slug, expected)
	}
}
