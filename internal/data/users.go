package data

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/concurrency"
	"github.com/sqlpipe/sqlpipe/internal/globals"
	"github.com/sqlpipe/sqlpipe/internal/validator"

	"golang.org/x/crypto/bcrypt"
)

var (
	ErrDuplicateUsername = errors.New("duplicate username")
)

var AnonymousUser = &User{}

type User struct {
	Username     string    `json:"username"`
	CreatedAt    time.Time `json:"createdAt"`
	LastModified time.Time `json:"lastModified"`
	Password     password  `json:"-"`
	Admin        bool      `json:"admin"`
	Version      int64     `json:"version"`
}

type internalUser struct {
	Username     string    `json:"username"`
	CreatedAt    time.Time `json:"createdAt"`
	LastModified time.Time `json:"lastModified"`
	PasswordHash string    `json:"passwordHash"`
	Admin        bool      `json:"admin"`
	Version      int64     `json:"version"`
}

func (u *User) IsAnonymous() bool {
	return u == AnonymousUser
}

type password struct {
	plaintext *string
	hash      []byte
}

func (p *password) Set(plaintextPassword string) error {
	hash, err := bcrypt.GenerateFromPassword([]byte(plaintextPassword), 12)
	if err != nil {
		return err
	}

	p.plaintext = &plaintextPassword
	p.hash = hash

	return nil
}

func (p *password) Matches(plaintextPassword string) (bool, error) {
	err := bcrypt.CompareHashAndPassword(p.hash, []byte(plaintextPassword))
	if err != nil {
		switch {
		case errors.Is(err, bcrypt.ErrMismatchedHashAndPassword):
			return false, nil
		default:
			return false, err
		}
	}

	return true, nil
}

func ValidatePasswordPlaintext(v *validator.Validator, password string) {
	v.Check(password != "", "password", "must be provided")
	v.Check(len(password) >= 8, "password", "must be at least 8 bytes long")
	v.Check(len(password) <= 72, "password", "must not be more than 72 bytes long")
}

func ValidateUser(v *validator.Validator, user *User) {
	v.Check(user.Username != "", "username", "must be provided")
	v.Check(!strings.Contains("/", user.Username), "username", "cannot contain a '/' character")
	v.Check(len(user.Username) <= 500, "username", "must not be more than 500 bytes long")

	if user.Password.plaintext != nil {
		ValidatePasswordPlaintext(v, *user.Password.plaintext)
	}

	if user.Password.hash == nil {
		panic("missing password hash for user")
	}
}

type UserModel struct {
	Etcd *clientv3.Client
}

func (m UserModel) Insert(user *User) (err error) {
	session, err := concurrency.NewSession(m.Etcd)
	if err != nil {
		return err
	}
	defer session.Close()

	userKey := fmt.Sprintf("sqlpipe/users/%v", user.Username)
	mutex := concurrency.NewMutex(session, userKey)

	ctx, cancel := context.WithTimeout(context.Background(), globals.EtcdTimeout)
	defer cancel()

	err = mutex.Lock(ctx)
	if err != nil {
		return err
	}
	defer mutex.Unlock(ctx)

	resp, err := m.Etcd.Get(ctx, userKey)
	if err != nil {
		return err
	}
	if resp.Count != 0 {
		return ErrDuplicateUsername
	}

	userWithPassword := internalUser{
		Username:     user.Username,
		PasswordHash: string(user.Password.hash),
		Admin:        user.Admin,
		CreatedAt:    user.CreatedAt,
		LastModified: user.LastModified,
		Version:      user.Version,
	}

	creationTime := time.Now()
	userWithPassword.CreatedAt = creationTime
	userWithPassword.LastModified = creationTime

	bytes, err := json.Marshal(userWithPassword)
	if err != nil {
		return err
	}

	_, err = m.Etcd.Put(
		ctx,
		userKey,
		string(bytes),
	)
	if err != nil {
		return err
	}
	return nil
}

func (m UserModel) Get(username string) (*User, error) {
	ctx, cancel := context.WithTimeout(context.Background(), globals.EtcdTimeout)
	defer cancel()
	resp, err := m.Etcd.Get(ctx, fmt.Sprintf("sqlpipe/users/%v", username))
	if err != nil {
		return nil, err
	}
	if resp.Count == 0 {
		return nil, ErrRecordNotFound
	}

	var user User
	err = json.Unmarshal(resp.Kvs[0].Value, &user)
	if err != nil {
		return nil, err
	}
	user.Version = resp.Kvs[0].Version

	return &user, nil
}

func (m UserModel) Update(user *User, ctx context.Context) (err error) {
	userWithPassword := internalUser{
		Username:     user.Username,
		PasswordHash: string(user.Password.hash),
		Admin:        user.Admin,
		CreatedAt:    user.CreatedAt,
		LastModified: user.LastModified,
		Version:      user.Version,
	}

	userWithPassword.LastModified = time.Now()

	bytes, err := json.Marshal(userWithPassword)
	if err != nil {
		return err
	}

	_, err = m.Etcd.Put(
		ctx,
		fmt.Sprintf("sqlpipe/users/%v", userWithPassword.Username),
		string(bytes),
	)
	if err != nil {
		return err
	}
	return nil
}

func (m UserModel) Delete(username string) error {
	if username == "" {
		return ErrRecordNotFound
	}

	session, err := concurrency.NewSession(m.Etcd)
	if err != nil {
		return err
	}
	defer session.Close()

	userKey := fmt.Sprintf("sqlpipe/users/%v", username)
	mutex := concurrency.NewMutex(session, userKey)

	ctx, cancel := context.WithTimeout(context.Background(), globals.EtcdTimeout)
	defer cancel()

	err = mutex.Lock(ctx)
	if err != nil {
		return err
	}
	defer mutex.Unlock(ctx)

	// count keys about to be deleted
	resp, err := m.Etcd.Get(ctx, userKey, clientv3.WithPrefix())
	if err != nil {
		return err
	}

	if resp.Count == 1 {
		return ErrRecordNotFound
	}

	_, err = m.Etcd.Delete(ctx, userKey, clientv3.WithPrefix())
	if err != nil {
		return err
	}

	return nil
}

func (m UserModel) GetAll() ([]*User, error) {
	ctx, cancel := context.WithTimeout(context.Background(), globals.EtcdTimeout)
	defer cancel()
	resp, err := m.Etcd.Get(ctx, "sqlpipe/users/", clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	users := []*User{}
	for i := range resp.Kvs {
		user := User{}
		prefixStripped := strings.TrimPrefix(string(resp.Kvs[i].Key), "sqlpipe/users/")
		if strings.Contains(prefixStripped, "/") {
			// it is a child node, not a user node. do not unmarshal it
			continue
		}
		err = json.Unmarshal(resp.Kvs[i].Value, &user)
		user.Version = resp.Kvs[0].Version
		users = append(users, &user)
	}
	if err != nil {
		return nil, err
	}

	return users, nil
}

func (m UserModel) GetForToken(tokenScope, tokenPlaintext string) (*User, error) {
	// tokenHash := sha256.Sum256([]byte(tokenPlaintext))

	// query := `
	//     SELECT users.id, users.created_at, users.name, users.email, users.password_hash, users.activated, users.version
	//     FROM users
	//     INNER JOIN tokens
	//     ON users.id = tokens.user_id
	//     WHERE tokens.hash = $1
	//     AND tokens.scope = $2
	//     AND tokens.expiry > $3`

	// args := []interface{}{tokenHash[:], tokenScope, time.Now()}

	// var user User

	// ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	// defer cancel()

	// err := m.DB.QueryRowContext(ctx, query, args...).Scan(
	// 	&user.ID,
	// 	&user.CreatedAt,
	// 	&user.Name,
	// 	&user.Email,
	// 	&user.Password.hash,
	// 	&user.Activated,
	// 	&user.Version,
	// )
	// if err != nil {
	// 	switch {
	// 	case errors.Is(err, sql.ErrNoRows):
	// 		return nil, ErrRecordNotFound
	// 	default:
	// 		return nil, err
	// 	}
	// }
	var user User
	return &user, nil
}
