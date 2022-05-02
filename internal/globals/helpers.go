package globals

import "fmt"

func GetUserPath(username string) string {
	return fmt.Sprintf("%v/users/%v", SqlpipePath, username)
}

func GetUserAdminPath(username string) string {
	return fmt.Sprintf("%v/users/%v/admin", SqlpipePath, username)
}

func GetUserHashedPasswordPath(username string) string {
	return fmt.Sprintf("%v/users/%v/hashed_password", SqlpipePath, username)
}

func GetUserTokenPath(username string, authToken string) string {
	return fmt.Sprintf("%v/users/%v/tokens/%v", SqlpipePath, username, authToken)
}
