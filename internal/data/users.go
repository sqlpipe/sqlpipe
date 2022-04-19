package data

import (
	"errors"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/sqlpipe/sqlpipe/internal/validator"

	"golang.org/x/crypto/bcrypt"
)

var (
	ErrDuplicateUsername = errors.New("duplicate username")
)

var AnonymousUser = &User{}

type User struct {
	ID        int64     `json:"id"`
	CreatedAt time.Time `json:"created_at"`
	Username  string    `json:"username"`
	Password  password  `json:"-"`
	Activated bool      `json:"activated"`
	Version   int       `json:"-"`
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
	v.Check(user.Username != "", "name", "must be provided")
	v.Check(len(user.Username) <= 500, "name", "must not be more than 500 bytes long")

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
	// query := `
	//     INSERT INTO users (name, email, password_hash, activated)
	//     VALUES ($1, $2, $3, $4)
	//     RETURNING id, created_at, version`

	// args := []interface{}{user.Name, user.Email, user.Password.hash, user.Activated}

	// ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	// defer cancel()

	// err := m.DB.QueryRowContext(ctx, query, args...).Scan(&user.ID, &user.CreatedAt, &user.Version)
	// if err != nil {
	// 	switch {
	// 	case err.Error() == `pq: duplicate key value violates unique constraint "users_email_key"`:
	// 		return ErrDuplicateEmail
	// 	default:
	// 		return err
	// 	}
	// }

	return err
}

func (m UserModel) GetByUsername(username string) (user *User, err error) {
	// query := `
	//     SELECT id, created_at, name, email, password_hash, activated, version
	//     FROM users
	//     WHERE email = $1`

	// var user User

	// ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	// defer cancel()

	// err := m.DB.QueryRowContext(ctx, query, email).Scan(
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

	return user, nil
}

func (m UserModel) Update(user *User) (err error) {
	// query := `
	//     UPDATE users
	//     SET name = $1, email = $2, password_hash = $3, activated = $4, version = version + 1
	//     WHERE id = $5 AND version = $6
	//     RETURNING version`

	// args := []interface{}{
	// 	user.Name,
	// 	user.Email,
	// 	user.Password.hash,
	// 	user.Activated,
	// 	user.ID,
	// 	user.Version,
	// }

	// ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	// defer cancel()

	// err := m.DB.QueryRowContext(ctx, query, args...).Scan(&user.Version)
	// if err != nil {
	// 	switch {
	// 	case err.Error() == `pq: duplicate key value violates unique constraint "users_email_key"`:
	// 		return ErrDuplicateEmail
	// 	case errors.Is(err, sql.ErrNoRows):
	// 		return ErrEditConflict
	// 	default:
	// 		return err
	// 	}
	// }

	return err
}

func (m UserModel) GetForToken(tokenScope, tokenPlaintext string) (user *User, err error) {
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

	return user, nil
}
