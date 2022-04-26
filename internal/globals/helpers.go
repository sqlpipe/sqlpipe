package globals

import "fmt"

func GetUserDataPath(username string) string {
	return fmt.Sprintf("%v/users/%v", SqlpipeDataPath, username)
}

func GetUserLockPath(username string) string {
	return fmt.Sprintf("%v/users/%v", SqlpipeLocksPath, username)
}
