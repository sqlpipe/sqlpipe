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
	newTokenPath := fmt.Sprintf("%v/tokens/%X", globals.GetUserPath(user.Username), token.Hash)
	callingUserPath := globals.GetUserPath(user.Username)
	callingUserPasswordPath := globals.GetUserHashedPasswordPath(user.Username)
	fastHashedPassword := fmt.Sprintf("%X", sha256.Sum256([]byte(user.BcryptedPassword)))
	// hashedPassword :=  fmt.Sprintf("%X", sha256.Sum256([]byte(user.BcryptedPassword)))

	ctx, cancel := context.WithTimeout(context.Background(), globals.EtcdTimeout)
	defer cancel()

	resp, err := m.Etcd.Txn(ctx).If(
		clientv3.Compare(clientv3.CreateRevision(callingUserPath), ">", 0),
		clientv3.Compare(clientv3.Value(callingUserPasswordPath), "=", fastHashedPassword),
	).Then(
		clientv3.OpPut(newTokenPath, token.ExpiryUnixTime),
	).Else(
		clientv3.OpGet(callingUserPath),
		clientv3.OpGet(callingUserPasswordPath),
	).Commit()

	if err != nil {
		return err
	}

	if !resp.Succeeded {
		if resp.Responses[0].GetResponseRange().Count == 0 {
			return ErrRecordNotFound
		}

		storedFastHashPassword := string(resp.Responses[1].GetResponseRange().Kvs[0].Value)
		if storedFastHashPassword != fastHashedPassword {
			fmt.Println(storedFastHashPassword)
			fmt.Println(fastHashedPassword)
			return ErrInvalidCredentials
		}

		panic("inserting token failed with an unknown error")
	}

	return err
}

func (m TokenModel) DeleteAllForUserWithContext(username string, ctx context.Context) (err error) {
	usersTokenPrefix := fmt.Sprintf("%v/tokens", globals.GetUserPath(username))
	_, err = m.Etcd.Delete(ctx, usersTokenPrefix, clientv3.WithPrefix())
	return err
}
