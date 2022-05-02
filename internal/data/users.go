package data

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/sqlpipe/sqlpipe/internal/globals"
	"github.com/sqlpipe/sqlpipe/internal/validator"
	"github.com/sqlpipe/sqlpipe/pkg"

	"golang.org/x/crypto/bcrypt"
)

var (
	ErrDuplicateUsername  = errors.New("duplicate username")
	ErrInvalidCredentials = errors.New("invalid authentication credentials")
	ErrExpiredAuthToken   = errors.New("expired authentication token")
	ErrCorruptUser        = errors.New("user data corrupted")
	ErrNotPermitted       = errors.New("not permitted")
)

var AnonymousUser = &User{}

type User struct {
	Username          string `json:"username"`
	PlaintextPassword string `json:"-"`
	BcryptedPassword  []byte `json:"-"`
	AuthToken         string `json:"-"`
	Admin             bool   `json:"admin"`
}

type ScrubbedUser struct {
	Username string `json:"username"`
	Admin    bool   `json:"admin"`
}

func (user User) Scrub() ScrubbedUser {
	return ScrubbedUser{
		Username: user.Username,
		Admin:    user.Admin,
	}
}

func (u *User) IsAnonymous() bool {
	return u == AnonymousUser
}

// func (u *User) CheckPassword(plaintextPassword string) (bool, error) {
// 	err := bcrypt.CompareHashAndPassword(u.HashedPassword, []byte(plaintextPassword))
// 	if err != nil {
// 		switch {
// 		case errors.Is(err, bcrypt.ErrMismatchedHashAndPassword):
// 			return false, nil
// 		default:
// 			return false, err
// 		}
// 	}

// 	return true, nil
// }

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

func ValidateUser(v *validator.Validator, user User) {
	ValidateUsername(v, user.Username)
	ValidatePassword(v, user.PlaintextPassword)

	if user.BcryptedPassword == nil {
		panic("missing password hash for user")
	}
}

func (u *User) SetPassword(plaintextPassword string) error {
	hash, err := bcrypt.GenerateFromPassword([]byte(plaintextPassword), 12)
	if err != nil {
		return err
	}

	u.PlaintextPassword = plaintextPassword
	u.BcryptedPassword = hash
	return nil
}

type UserModel struct {
	Etcd *clientv3.Client
}

func (m UserModel) Insert(newUser User, callingUser User) (user User, err error) {
	callingUserPath := globals.GetUserPath(callingUser.Username)
	callingUserPasswordPath := globals.GetUserHashedPasswordPath(callingUser.Username)
	callingUserAdminPath := globals.GetUserAdminPath(callingUser.Username)
	callingUserTokenPath := globals.GetUserHashedTokenPath(callingUser.Username, globals.GetSha256Hash(callingUser.AuthToken))

	targetUserPath := globals.GetUserPath(newUser.Username)
	newUserAdminPath := globals.GetUserAdminPath(newUser.Username)
	newUserHashedPasswordPath := globals.GetUserHashedPasswordPath(newUser.Username)
	fastHashedPassword := fmt.Sprintf("%X", sha256.Sum256([]byte(newUser.BcryptedPassword)))

	ctx, cancel := context.WithTimeout(context.Background(), globals.EtcdTimeout)
	defer cancel()

	resp, err := m.Etcd.Txn(ctx).If(
		// authentication checks
		clientv3.Compare(clientv3.CreateRevision(callingUserTokenPath), ">", 0),
		clientv3.Compare(clientv3.Value(callingUserTokenPath), ">", globals.UnixTimeStringWithLeadingZeros(time.Now().Unix())),
		// corruption checks
		clientv3.Compare(clientv3.CreateRevision(callingUserPath), ">", 0),
		clientv3.Compare(clientv3.CreateRevision(callingUserPasswordPath), ">", 0),
		clientv3.Compare(clientv3.Value(callingUserAdminPath), "=", "true"),
		// business logic checks
		clientv3.Compare(clientv3.CreateRevision(targetUserPath), "=", 0),
	).Then(
		// create target user
		clientv3.OpPut(targetUserPath, ""),
		clientv3.OpPut(newUserHashedPasswordPath, fastHashedPassword),
		clientv3.OpPut(newUserAdminPath, fmt.Sprint(newUser.Admin)),
	).Else(
		// authentication checks
		clientv3.OpGet(callingUserTokenPath),
		clientv3.OpGet(callingUserPath),
		// corruption checks
		clientv3.OpGet(callingUserPasswordPath),
		clientv3.OpGet(callingUserAdminPath),
		// business logic checks
		clientv3.OpGet(targetUserPath),
	).Commit()

	if err != nil {
		return user, err
	}

	if !resp.Succeeded {

		for _, rr := range resp.Responses {
			fmt.Println(fmt.Sprint(rr.GetResponseRange().Kvs[0].Version), string(rr.GetResponseRange().Kvs[0].Value))
		}
		// check if calling user token exists
		if resp.Responses[0].GetResponseRange().Kvs[0].Version == 0 {
			return user, ErrInvalidCredentials
		}

		// parse token expiry
		expiry, err := strconv.ParseInt(string(resp.Responses[0].GetResponseRange().Kvs[0].Value), 10, 64)
		if err != nil {
			return user, err
		}
		// check if token expired
		if expiry < time.Now().Unix() {
			return user, ErrExpiredAuthToken
		}

		// check if calling user main node exists
		if resp.Responses[1].GetResponseRange().Kvs[0].Version == 0 {
			return user, ErrInvalidCredentials
		}
		// check if calling user password node exists
		if resp.Responses[2].GetResponseRange().Kvs[0].Version == 0 {
			return user, ErrCorruptUser
		}
		// check if calling user admin node exists
		if resp.Responses[3].GetResponseRange().Kvs[0].Version == 0 {
			return user, ErrCorruptUser
		}
		// check if calling user is an admin
		isAdmin, err := strconv.ParseBool(string(resp.Responses[3].GetResponseRange().Kvs[0].Value))
		if err != nil {
			return user, err
		}
		if !isAdmin {
			return user, ErrNotPermitted
		}
		// check if target user node exists
		if resp.Responses[4].GetResponseRange().Kvs[0].Version == 0 {
			return user, ErrDuplicateUsername
		}

		panic("an unknown error occured while creating a user")
	}

	return newUser, nil
}

func (m UserModel) InsertInitialUser(newUser User) (err error) {
	newUserPath := globals.GetUserPath(newUser.Username)
	newUserAdminPath := globals.GetUserAdminPath(newUser.Username)
	newUserHashedPasswordPath := globals.GetUserHashedPasswordPath(newUser.Username)

	fastHashedPassword := fmt.Sprintf("%X", sha256.Sum256([]byte(newUser.BcryptedPassword)))

	ctx, cancel := context.WithTimeout(context.Background(), globals.EtcdTimeout)
	defer cancel()

	resp, err := m.Etcd.Txn(ctx).If(
		clientv3.Compare(clientv3.CreateRevision(newUserPath), "=", 0),
	).Then(
		clientv3.OpPut(newUserPath, ""),
		clientv3.OpPut(newUserHashedPasswordPath, fastHashedPassword),
		clientv3.OpPut(newUserAdminPath, fmt.Sprint(newUser.Admin)),
	).Else(
		clientv3.OpGet(newUserPath),
	).Commit()

	if err != nil {
		return err
	}

	if !resp.Succeeded {
		if resp.Responses[0].GetResponseRange().Count > 0 {
			return ErrDuplicateUsername
		}
		panic("an unkown error occured while creating the initial user")
	}

	return nil
}

func (m UserModel) Get(
	callingUser User,
	username string,
) (
	user User,
	err error,
) {
	callingUserPath := globals.GetUserPath(callingUser.Username)
	callingUserPasswordPath := globals.GetUserHashedPasswordPath(callingUser.Username)
	callingUserAdminPath := globals.GetUserAdminPath(callingUser.Username)
	callingUserTokenPath := globals.GetUserHashedTokenPath(callingUser.Username, callingUser.AuthToken)

	targetUserPath := globals.GetUserPath(username)

	ctx, cancel := context.WithTimeout(context.Background(), globals.EtcdTimeout)
	defer cancel()

	resp, err := m.Etcd.Txn(ctx).If(
		// authentication checks
		clientv3.Compare(clientv3.CreateRevision(callingUserTokenPath), ">", 0),
		clientv3.Compare(clientv3.Value(callingUserTokenPath), ">", fmt.Sprint(time.Now().Unix())),
		// corruption checks
		clientv3.Compare(clientv3.CreateRevision(callingUserPath), ">", 0),
		clientv3.Compare(clientv3.CreateRevision(callingUserPasswordPath), ">", 0),
		clientv3.Compare(clientv3.Value(callingUserAdminPath), "=", "true"),
		// business logic checks
		clientv3.Compare(clientv3.CreateRevision(targetUserPath), "=", 0),
	).Then(
		// create target user
		clientv3.OpGet(targetUserPath),
	).Else(
		// authentication checks
		clientv3.OpGet(callingUserTokenPath),
		clientv3.OpGet(callingUserPath),
		// corruption checks
		clientv3.OpGet(callingUserPasswordPath),
		clientv3.OpGet(callingUserAdminPath),
		// business logic checks
		clientv3.OpGet(targetUserPath),
	).Commit()

	if err != nil {
		return user, err
	}

	if !resp.Succeeded {

		// check if calling user token exists
		if resp.Responses[0].GetResponseRange().Count == 0 {
			return user, ErrInvalidCredentials
		}

		// parse token expiry
		expiry, err := strconv.ParseInt(string(resp.Responses[0].GetResponseRange().Kvs[0].Value), 10, 64)
		if err != nil {
			return user, err
		}
		// check if token expired
		if expiry < time.Now().Unix() {
			return user, ErrExpiredAuthToken
		}
		// check if calling user main node exists
		if resp.Responses[1].GetResponseRange().Count == 0 {
			return user, ErrInvalidCredentials
		}
		// check if calling user password node exists
		if resp.Responses[2].GetResponseRange().Count == 0 {
			return user, ErrCorruptUser
		}
		// check if calling user admin node exists
		if resp.Responses[3].GetResponseRange().Count == 0 {
			return user, ErrCorruptUser
		}
		// check if calling user is an admin
		isAdmin, err := strconv.ParseBool(string(resp.Responses[3].GetResponseRange().Kvs[0].Value))
		if err != nil {
			return user, err
		}
		if !isAdmin {
			return user, ErrNotPermitted
		}
		// check if target user node exists
		if resp.Responses[4].GetResponseRange().Count == 0 {
			return user, ErrRecordNotFound
		}

		panic("an unknown error occured while getting a user")
	}

	return user, nil
}

func (m UserModel) GetWithoutToken(
	callingUser User,
) (
	user User,
	err error,
) {
	callingUserPath := globals.GetUserPath(callingUser.Username)
	callingUserAdminPath := globals.GetUserAdminPath(callingUser.Username)
	callingUserPasswordPath := globals.GetUserHashedPasswordPath(callingUser.Username)

	ctx, cancel := context.WithTimeout(context.Background(), globals.EtcdTimeout)
	defer cancel()

	resp, err := m.Etcd.Txn(ctx).If(
		clientv3.Compare(clientv3.CreateRevision(callingUserPath), ">", 0),
		clientv3.Compare(clientv3.CreateRevision(callingUserPasswordPath), ">", 0),
		clientv3.Compare(clientv3.CreateRevision(callingUserAdminPath), ">", 0),
	).Then(
		clientv3.OpGet(callingUserPasswordPath),
	).Else(
		clientv3.OpGet(callingUserPath),
		clientv3.OpGet(callingUserPasswordPath),
		clientv3.OpGet(callingUserAdminPath),
	).Commit()

	if err != nil {
		return user, err
	}

	if !resp.Succeeded {

		// check if calling user main node exists
		if resp.Responses[0].GetResponseRange().Count == 0 {
			return user, ErrInvalidCredentials
		}
		// check if calling user password node exists
		if resp.Responses[1].GetResponseRange().Count == 0 {
			return user, ErrCorruptUser
		}
		// check if calling user admin node exists
		if resp.Responses[2].GetResponseRange().Count == 0 {
			return user, ErrCorruptUser
		}

		panic("an unknown error occured while creating a user")
	}

	user = callingUser
	user.BcryptedPassword = resp.Responses[0].GetResponseRange().Kvs[0].Value

	return user, nil
}

func (m UserModel) GetUserCheckToken(
	username string,
	inputToken string,
) (
	scrubbedUser ScrubbedUser,
	err error,
) {
	userDataPath := globals.GetUserPath(username)
	userTokenPath := fmt.Sprintf("%v/tokens", userDataPath)

	ctx, cancel := context.WithTimeout(context.Background(), globals.EtcdTimeout)
	defer cancel()

	resp, err := m.Etcd.Txn(ctx).If(
		clientv3.Compare(clientv3.CreateRevision(userDataPath), ">", 0),
		clientv3.Compare(clientv3.CreateRevision(userTokenPath), ">", 0),
	).Then(
		clientv3.OpGet(userTokenPath),
		clientv3.OpGet(userDataPath),
	).Commit()

	if err != nil {
		return scrubbedUser, err
	}

	if !resp.Succeeded {
		return scrubbedUser, ErrRecordNotFound
	}

	var token Token
	if err = json.Unmarshal(resp.Responses[0].GetResponseRange().Kvs[0].Value, &token); err != nil {
		return scrubbedUser, err
	}

	expiry, err := strconv.ParseInt(token.ExpiryUnixTime, 10, 64)
	if err != nil {
		return scrubbedUser, err
	}

	if expiry < time.Now().Unix() {
		return scrubbedUser, ErrRecordNotFound
	}

	if err = json.Unmarshal(resp.Responses[1].GetResponseRange().Kvs[0].Value, &scrubbedUser); err != nil {
		return scrubbedUser, err
	}

	return scrubbedUser, nil
}

func (m UserModel) GetUserWithPasswordWithContext(
	username string,
	ctx *context.Context,
) (
	user User,
	err error,
) {
	resp, err := m.Etcd.Get(*ctx, globals.GetUserPath(username))
	if err != nil {
		return user, err
	}
	if resp.Count == 0 {
		return user, ErrRecordNotFound
	}

	err = json.Unmarshal(resp.Kvs[0].Value, &user)
	if err != nil {
		return user, err
	}

	return user, nil
}

func (m UserModel) UpdateNoLock(user User, ctx context.Context) (err error) {
	bytes, err := json.Marshal(user)
	if err != nil {
		return err
	}

	_, err = m.Etcd.Put(
		ctx,
		globals.GetUserPath(user.Username),
		string(bytes),
	)

	return err
}

func (m UserModel) Delete(username string) error {

	userDataPath := globals.GetUserPath(username)

	ctx, cancel := context.WithTimeout(context.Background(), globals.EtcdTimeout)
	defer cancel()

	resp, err := m.Etcd.Get(ctx, userDataPath, clientv3.WithPrefix())
	if err != nil {
		return err
	}

	if resp.Count == 0 {
		return ErrRecordNotFound
	}

	_, err = m.Etcd.Delete(ctx, userDataPath, clientv3.WithPrefix())
	return err
}

func (m UserModel) GetAll(username string, admin *bool, filters Filters) ([]ScrubbedUser, Metadata, error) {
	metadata := Metadata{}
	ctx, cancel := context.WithTimeout(context.Background(), globals.EtcdTimeout)
	defer cancel()
	resp, err := m.Etcd.Get(ctx, "sqlpipe/data/users", clientv3.WithPrefix())
	if err != nil {
		return nil, metadata, err
	}

	users := []ScrubbedUser{}
	totalRecords := 0

	for i := range resp.Kvs {
		user := User{}
		prefixStripped := strings.TrimPrefix(string(resp.Kvs[i].Key), "sqlpipe/data/users")
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

		scrubbedUser := user.Scrub()
		users = append(users, scrubbedUser)
		totalRecords++
	}

	switch filters.sortColumn() {
	case "-username":
		sort.Slice(users, func(i, j int) bool { return users[i].Username > users[j].Username })
	default:
		sort.Slice(users, func(i, j int) bool { return users[i].Username < users[j].Username })
	}

	if filters.offset() > totalRecords {
		metadata = calculateMetadata(totalRecords, filters.Page, filters.PageSize)
		return nil, metadata, nil
	}

	maxItem := pkg.Min(filters.offset()+filters.limit(), totalRecords)
	paginatedUsers := []ScrubbedUser{}
	for i := filters.offset(); i < maxItem; i++ {
		paginatedUsers = append(paginatedUsers, users[i])
	}

	metadata = calculateMetadata(totalRecords, filters.Page, filters.PageSize)

	return paginatedUsers, metadata, nil
}
