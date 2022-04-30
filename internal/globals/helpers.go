package globals

import "fmt"

func GetUserDataPath(username string) string {
	return fmt.Sprintf("%v/users/%v", SqlpipePath, username)
}

func GetUserTokenPath(username string, authToken string) string {
	return fmt.Sprintf("%v/users/%v/tokens/%v", SqlpipePath, username, authToken)
}
