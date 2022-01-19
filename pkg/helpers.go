package pkg

func Min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func Min64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func Max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func Max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func Background(fn func()) {
	go func() {
		fn()
	}()
}
