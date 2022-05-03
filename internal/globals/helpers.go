package globals

import (
	"crypto/sha256"
	"fmt"
)

func GetUserPath(username string) string {
	return fmt.Sprintf("%v/users/%v", SqlpipePath, username)
}

func GetUserAdminPath(username string) string {
	return fmt.Sprintf("%v/users/%v/admin", SqlpipePath, username)
}

func GetUserLockPath(username string) string {
	return fmt.Sprintf("%v/users/%v/lock", SqlpipePath, username)
}

func GetUserHashedPasswordPath(username string) string {
	return fmt.Sprintf("%v/users/%v/hashed_password", SqlpipePath, username)
}

func GetUserHashedTokenPath(username string, hashedToken string) string {
	return fmt.Sprintf("%v/users/%v/tokens/%v", SqlpipePath, username, hashedToken)
}

func GetSha256Hash(str string) string {
	return fmt.Sprintf("%X", sha256.Sum256([]byte(str)))
}

func UnixTimeStringWithLeadingZeros(unixTime int64) string {
	expiryString := fmt.Sprint(unixTime)
	expiryStringLen := len(expiryString)
	if expiryStringLen < 19 {
		zerosToAdd := 19 - expiryStringLen
		for i := 0; i < zerosToAdd; i++ {
			expiryString = "0" + expiryString
		}
	}

	return expiryString
}
