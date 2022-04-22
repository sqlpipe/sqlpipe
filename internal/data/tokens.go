package data

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base32"
	"encoding/json"
	"fmt"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/coreos/etcd/clientv3"
	"github.com/sqlpipe/sqlpipe/internal/globals"
	"github.com/sqlpipe/sqlpipe/internal/validator"
)

const (
	ScopeActivation     = "activation"
	ScopeAuthentication = "authentication"
	ScopePasswordReset  = "password-reset"
	TokenPrefix         = "sqlpipe/tokens/"
)

type Token struct {
	Plaintext string    `json:"token"`
	Hash      []byte    `json:"-"`
	Username  string    `json:"-"`
	Expiry    time.Time `json:"expiry"`
	Scope     string    `json:"-"`
}

func generateToken(username string, ttl time.Duration, scope string) (*Token, error) {
	token := &Token{
		Username: username,
		Expiry:   time.Now().Add(ttl),
		Scope:    scope,
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

func (m TokenModel) New(username string, ttl time.Duration, scope string, ctx context.Context) (*Token, error) {
	token, err := generateToken(username, ttl, scope)
	if err != nil {
		return nil, err
	}

	err = m.Insert(token, ctx)
	return token, err
}

func (m TokenModel) Insert(token *Token, ctx context.Context) (err error) {
	bytes, err := json.Marshal(token)
	if err != nil {
		return err
	}

	_, err = m.Etcd.Put(
		ctx,
		fmt.Sprintf("%v%v", TokenPrefix, token.Hash),
		string(bytes),
	)
	return err
}

func (m TokenModel) DeleteAllForUserWithContext(username string, ctx context.Context) (err error) {
	resp, err := m.Etcd.Get(ctx, fmt.Sprintf("%v", TokenPrefix), clientv3.WithPrefix())
	if resp.Count == 0 {
		return ErrRecordNotFound
	}
	if err != nil {
		return err
	}

	tokenHashes := []string{}

	for i := range resp.Kvs {
		token := Token{}

		err = json.Unmarshal(resp.Kvs[i].Value, &token)
		if err != nil {
			return err
		}

		if token.Username == username {
			tokenHashes = append(tokenHashes, string(token.Hash))
		}
	}

	ch := make(chan *stringWithContext)
	g := errgroup.Group{}

	for t := 0; t < globals.EtcdMaxConcurrentRequests; t++ {
		go m.asyncDeleteWorker(ch, &g)
	}

	for _, tokenHash := range tokenHashes {
		ch <- &stringWithContext{tokenHash, &ctx}
	}

	return g.Wait()
}

type stringWithContext struct {
	str string
	ctx *context.Context
}

func (m TokenModel) asyncDeleteWorker(ch chan *stringWithContext, g *errgroup.Group) {
	for job := range ch {
		job := job
		g.Go(func() error {
			_, err := m.Etcd.Delete(*job.ctx, fmt.Sprintf("%v%v", TokenPrefix, job.str))
			return err
		})
	}
}
