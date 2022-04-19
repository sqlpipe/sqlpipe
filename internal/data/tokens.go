package data

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base32"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/sqlpipe/sqlpipe/internal/validator"
)

const (
	ScopeActivation     = "activation"
	ScopeAuthentication = "authentication"
	ScopePasswordReset  = "password-reset"
)

type Token struct {
	Plaintext string    `json:"token"`
	Hash      []byte    `json:"-"`
	UserID    int64     `json:"-"`
	Expiry    time.Time `json:"expiry"`
	Scope     string    `json:"-"`
}

func generateToken(userID int64, ttl time.Duration, scope string) (*Token, error) {
	token := &Token{
		UserID: userID,
		Expiry: time.Now().Add(ttl),
		Scope:  scope,
	}

	randomBytes := make([]byte, 16)

	_, err := rand.Read(randomBytes)
	if err != nil {
		return nil, err
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

func (m TokenModel) New(userID int64, ttl time.Duration, scope string) (token *Token, err error) {
	// token, err := generateToken(userID, ttl, scope)
	// if err != nil {
	// 	return nil, err
	// }

	// err = m.Insert(token)
	return token, err
}

func (m TokenModel) Insert(token *Token) (err error) {
	// query := `
	//     INSERT INTO tokens (hash, user_id, expiry, scope)
	//     VALUES ($1, $2, $3, $4)`

	// args := []interface{}{token.Hash, token.UserID, token.Expiry, token.Scope}

	// ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	// defer cancel()

	// _, err := m.DB.ExecContext(ctx, query, args...)
	return err
}

func (m TokenModel) DeleteAllForUser(scope string, userID int64) (err error) {
	// query := `
	//     DELETE FROM tokens
	//     WHERE scope = $1 AND user_id = $2`

	// ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	// defer cancel()

	// _, err := m.DB.ExecContext(ctx, query, scope, userID)
	return err
}
