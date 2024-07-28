package randomid

import "crypto/rand"

var chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

// GenerateID generate n length cryptographically secured random ID
// Notes: it's best to have length of 16 or more to avoid collisions
// see: https://zelark.github.io/nano-id-cc/
func Generate(length int) string {
	lengthOfChars := len(chars)
	b := make([]byte, length)

	rand.Read(b)

	for i := 0; i < length; i++ {
		b[i] = chars[int(b[i])%lengthOfChars]
	}

	return string(b)
}

// GenerateDefault will generate random id with 16 in length
func GenerateDefault() string {
	defaultLength := 16
	return Generate(defaultLength)
}
