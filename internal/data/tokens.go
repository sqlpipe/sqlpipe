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
	Plaintext string `json:"token"`
	Expiry    string `json:"expiry"`
	Hash      []byte `json:"-"`
}

func generateToken(username string, ttl string) (Token, error) {
	token := Token{
		Expiry: ttl,
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

func (m TokenModel) New(username string, ttl string) (Token, error) {
	token, err := generateToken(username, ttl)
	if err != nil {
		return token, err
	}

	err = m.Insert(token, username)
	return token, err
}

func (m TokenModel) Insert(token Token, username string) (err error) {
	tokenPath := fmt.Sprintf("%v/tokens/%X", globals.GetUserDataPath(username), token.Hash)

	ctx, cancel := context.WithTimeout(context.Background(), globals.EtcdTimeout)

	_, err = m.Etcd.Put(ctx, tokenPath, token.Expiry)
	cancel()

	return err
}

func (m TokenModel) DeleteAllForUserWithContext(username string, ctx context.Context) (err error) {
	usersTokenPrefix := fmt.Sprintf("%v/tokens", globals.GetUserDataPath(username))
	_, err = m.Etcd.Delete(ctx, usersTokenPrefix, clientv3.WithPrefix())
	return err
}
