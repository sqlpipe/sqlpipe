package data

import (
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
	UserId    int64     `json:"-"`
	Expiry    time.Time `json:"expiry"`
	Scope     string    `json:"-"`
}

func ValidateTokenPlaintext(v *validator.Validator, tokenPlaintext string) {
	v.Check(tokenPlaintext != "", "token", "must be provided")
	v.Check(len(tokenPlaintext) == 26, "token", "must be 26 bytes long")
}

type TokenModel struct {
	Etcd *clientv3.Client
}

func (m TokenModel) New(username string, ttl time.Duration, scope string) (token *Token, err error) {
	// token, err := generateToken(userId, ttl, scope)
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

	// args := []interface{}{token.Hash, token.UserId, token.Expiry, token.Scope}

	// ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	// defer cancel()

	// _, err := m.DB.ExecContext(ctx, query, args...)
	return err
}

func (m TokenModel) DeleteAllForUser(scope string, userId string) (err error) {
	// query := `
	//     DELETE FROM tokens
	//     WHERE scope = $1 AND user_id = $2`

	// ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	// defer cancel()

	// _, err := m.DB.ExecContext(ctx, query, scope, userId)
	return err
}
