package sessions

import (
	"crypto/rand"
	"encoding/base64"
	"errors"

	"golang.org/x/crypto/nacl/secretbox"
)

var errInvalidToken = errors.New("session: invalid token")

func encrypt(in []byte, key [32]byte) (string, error) {
	var nonce [24]byte
	_, err := rand.Read(nonce[:])
	if err != nil {
		return "", err
	}

	box := secretbox.Seal(nonce[:], in, &nonce, &key)

	return base64.RawURLEncoding.EncodeToString(box), nil
}

func decrypt(token string, keys [][32]byte) ([]byte, error) {
	box, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return nil, errInvalidToken
	}

	if len(box) < 24 {
		return nil, errInvalidToken
	}
	var nonce [24]byte
	copy(nonce[:], box[:24])

	for _, key := range keys {
		out, ok := secretbox.Open(nil, box[24:], &nonce, &key)
		if ok {
			return out, nil
		}
	}

	return nil, errInvalidToken
}
