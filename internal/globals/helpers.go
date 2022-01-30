package globals

import "time"

func HumanDate(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format("2 Jan 2006 15:04:05 UTC")
}

func PgDate(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format("2 Jan 2006 15:04:05 UTC")
}
