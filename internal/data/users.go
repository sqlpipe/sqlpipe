package data

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/concurrency"
	"github.com/sqlpipe/sqlpipe/internal/globals"
	"github.com/sqlpipe/sqlpipe/internal/validator"
	"github.com/sqlpipe/sqlpipe/pkg"

	"golang.org/x/crypto/bcrypt"
)

var (
	ErrDuplicateUsername = errors.New("duplicate username")
)

const UserPrefix = "sqlpipe/users/"

func getUserKey(username string) string {
	return fmt.Sprintf("%v%v", UserPrefix, username)
}

var AnonymousUser = &ScrubbedUser{}

type User struct {
	Username          string    `json:"username"`
	CreatedAt         time.Time `json:"createdAt"`
	LastModified      time.Time `json:"lastModified"`
	PlaintextPassword string    `json:"-"`
	HashedPassword    []byte    `json:"hashedPassword"`
	Admin             bool      `json:"admin"`
	Version           int64     `json:"version"`
}

type ScrubbedUser struct {
	Username     string    `json:"username"`
	CreatedAt    time.Time `json:"createdAt"`
	LastModified time.Time `json:"lastModified"`
	Admin        bool      `json:"admin"`
	Version      int64     `json:"version"`
}

func (user User) Scrub() ScrubbedUser {
	return ScrubbedUser{
		Username:     user.Username,
		CreatedAt:    user.CreatedAt,
		LastModified: user.LastModified,
		Admin:        user.Admin,
		Version:      user.Version,
	}
}

func (u *ScrubbedUser) IsAnonymous() bool {
	return u == AnonymousUser
}

func (u *User) CheckPassword(plaintextPassword string) (bool, error) {
	err := bcrypt.CompareHashAndPassword(u.HashedPassword, []byte(plaintextPassword))
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

func ValidatePassword(v *validator.Validator, password string) {
	v.Check(password != "", "password", "must be provided")
	v.Check(len([]rune(password)) >= 12, "password", "must be at least 12 characters long")
	v.Check(len([]rune(password)) <= 32, "password", "must not be more than 32 characters long")
}

func ValidateUsername(v *validator.Validator, username string) {
	if username != "" {
		v.Check(validator.Matches(username, validator.UsernameRX), "username", "Username must be 5-30 characters, contain alphanumeric characters or underscores, and first letter must be a letter")
	}
}

func ValidateUser(v *validator.Validator, user *User) {
	ValidateUsername(v, user.Username)
	ValidatePassword(v, user.PlaintextPassword)

	if user.HashedPassword == nil {
		panic("missing password hash for user")
	}
}

func (u *User) SetPassword(plaintextPassword string) error {
	hash, err := bcrypt.GenerateFromPassword([]byte(plaintextPassword), 12)
	if err != nil {
		return err
	}

	u.PlaintextPassword = plaintextPassword
	u.HashedPassword = hash
	return nil
}

type UserModel struct {
	Etcd *clientv3.Client
}

func (m UserModel) Insert(user *User) (err error) {

	creationTime := time.Now()
	user.CreatedAt = creationTime
	user.LastModified = creationTime

	bytes, err := json.Marshal(user)
	if err != nil {
		return err
	}

	userKey := fmt.Sprintf("%v%v", UserPrefix, user.Username)

	ctx, cancel := context.WithTimeout(context.Background(), globals.EtcdTimeout)

	resp, err := m.Etcd.Txn(ctx).If(
		clientv3.Compare(clientv3.CreateRevision(userKey), "=", 0),
	).Then(
		clientv3.OpPut(userKey, string(bytes)),
	).Commit()
	cancel()
	if err != nil {
		return err
	}

	if !resp.Succeeded {
		return ErrDuplicateUsername
	}

	return nil
}

func (m UserModel) Get(username string) (*ScrubbedUser, error) {
	ctx, cancel := context.WithTimeout(context.Background(), globals.EtcdTimeout)
	resp, err := m.Etcd.Get(ctx, getUserKey(username))
	cancel()
	if err != nil {
		return nil, err
	}
	if resp.Count == 0 {
		return nil, ErrRecordNotFound
	}

	var user User
	if err = json.Unmarshal(resp.Kvs[0].Value, &user); err != nil {
		return nil, err
	}
	user.Version = resp.Kvs[0].Version
	scrubbedUser := user.Scrub()

	return &scrubbedUser, nil
}

func (m UserModel) GetUserWithPassword(username string) (*User, error) {
	ctx, cancel := context.WithTimeout(context.Background(), globals.EtcdTimeout)
	defer cancel()
	resp, err := m.Etcd.Get(ctx, getUserKey(username))
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

func (m UserModel) GetUserWithPasswordWithContext(username string, ctx context.Context) (*User, error) {
	resp, err := m.Etcd.Get(ctx, getUserKey(username))
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
	user.LastModified = time.Now()
	bytes, err := json.Marshal(user)
	if err != nil {
		return err
	}

	_, err = m.Etcd.Put(
		ctx,
		fmt.Sprintf("%v%v", UserPrefix, user.Username),
		string(bytes),
	)

	return err
}

func (m UserModel) Delete(username string) error {
	session, err := concurrency.NewSession(m.Etcd)
	if err != nil {
		return err
	}
	defer session.Close()

	userKey := getUserKey(username)
	mutex := concurrency.NewMutex(session, userKey)

	ctx, cancel := context.WithTimeout(context.Background(), globals.EtcdTimeout)
	defer cancel()

	if err = mutex.Lock(ctx); err != nil {
		return err
	}

	// count keys about to be deleted
	resp, err := m.Etcd.Get(ctx, userKey, clientv3.WithPrefix())
	if err != nil {
		return err
	}

	if resp.Count == 0 {
		return ErrRecordNotFound
	}

	_, err = m.Etcd.Delete(ctx, userKey, clientv3.WithPrefix())
	return err
}

func (m UserModel) GetAll(username string, admin *bool, filters Filters) ([]*ScrubbedUser, Metadata, error) {
	metadata := Metadata{}
	ctx, cancel := context.WithTimeout(context.Background(), globals.EtcdTimeout)
	defer cancel()
	resp, err := m.Etcd.Get(ctx, UserPrefix, clientv3.WithPrefix())
	if err != nil {
		return nil, metadata, err
	}

	users := []*ScrubbedUser{}
	totalRecords := 0

	for i := range resp.Kvs {
		user := User{}
		prefixStripped := strings.TrimPrefix(string(resp.Kvs[i].Key), UserPrefix)
		levels := strings.Split(prefixStripped, "/")
		topLevel := levels[0]

		if len(levels) > 1 {
			// it is a child node, not a user node. do not unmarshal it
			continue
		}

		if username != "" && !strings.Contains(topLevel, username) {
			// doesn't match filter criteria
			continue
		}

		err = json.Unmarshal(resp.Kvs[i].Value, &user)
		if err != nil {
			return nil, metadata, err
		}

		if admin != nil && user.Admin != *admin {
			// doesn't match filter criteria
			continue
		}

		user.Version = resp.Kvs[0].Version
		scrubbedUser := user.Scrub()
		users = append(users, &scrubbedUser)
		totalRecords++
	}

	switch filters.sortColumn() {
	case "-username":
		sort.Slice(users, func(i, j int) bool { return users[i].Username > users[j].Username })
	case "created_at":
		sort.Slice(users, func(i, j int) bool { return users[i].CreatedAt.Before(users[j].CreatedAt) })
	case "-created_at":
		sort.Slice(users, func(i, j int) bool { return users[j].CreatedAt.Before(users[i].CreatedAt) })
	case "last_modified":
		sort.Slice(users, func(i, j int) bool { return users[i].LastModified.Before(users[j].LastModified) })
	case "-last_modified":
		sort.Slice(users, func(i, j int) bool { return users[j].LastModified.Before(users[i].LastModified) })
	default:
		sort.Slice(users, func(i, j int) bool { return users[i].Username < users[j].Username })
	}

	if filters.offset() > totalRecords {
		metadata = calculateMetadata(totalRecords, filters.Page, filters.PageSize)
		return nil, metadata, nil
	}

	maxItem := pkg.Min(filters.offset()+filters.limit(), totalRecords)
	paginatedUsers := []*ScrubbedUser{}
	for i := filters.offset(); i < maxItem; i++ {
		paginatedUsers = append(paginatedUsers, users[i])
	}

	metadata = calculateMetadata(totalRecords, filters.Page, filters.PageSize)

	return paginatedUsers, metadata, nil
}

// func (m UserModel) GetForToken(tokenScope, tokenPlaintext string) (*User, error) {
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
// var user User
// return &user, nil
// }
