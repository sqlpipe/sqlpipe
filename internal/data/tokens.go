package data

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base32"
	"fmt"

	"github.com/coreos/etcd/clientv3"
	"github.com/sqlpipe/sqlpipe/internal/globals"
	"github.com/sqlpipe/sqlpipe/internal/validator"
)

const (
	ScopeActivation     = "activation"
	ScopeAuthentication = "authentication"
	ScopePasswordReset  = "password-reset"
)

type Token struct {
	Plaintext      string `json:"token"`
	ExpiryUnixTime string `json:"expiryUnixTime"`
	Hash           []byte `json:"-"`
}

func generateToken(ttl string) (Token, error) {
	token := Token{
		ExpiryUnixTime: ttl,
	}

	randomBytes := make([]byte, 16)

	_, err := rand.Read(randomBytes)
	if err != nil {
		return token, err
	}

	token.Plaintext = base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(randomBytes)

	hash := sha256.Sum256([]byte(token.Plaintext))
	token.Hash = hash[:]

	return token, nil
}

func ValidateTokenPlaintext(v *validator.Validator, tokenPlaintext string) {
	v.Check(tokenPlaintext != "", "token", "must be provided")
	v.Check(len(tokenPlaintext) == 26, "token", "must be 26 bytes long")
}

type TokenModel struct {
	Etcd *clientv3.Client
}

func (m TokenModel) New(user User, ttl string) (Token, error) {
	token, err := generateToken(ttl)
	if err != nil {
		return token, err
	}

	err = m.Insert(token, user)
	return token, err
}

func (m TokenModel) Insert(token Token, user User) (err error) {
	ctx, cancel := context.WithTimeout(context.Background(), globals.EtcdTimeout)
	defer cancel()

	newTokenPath := globals.GetUserHashedTokenPath(user.Username, fmt.Sprintf("%X", string(token.Hash)))
	_, err = m.Etcd.Put(ctx, newTokenPath, token.ExpiryUnixTime)
	return err
}

func (m TokenModel) DeleteAllForUserWithContext(username string, ctx context.Context) (err error) {
	usersTokenPrefix := fmt.Sprintf("%v/tokens", globals.GetUserPath(username))
	_, err = m.Etcd.Delete(ctx, usersTokenPrefix, clientv3.WithPrefix())
	return err
}
