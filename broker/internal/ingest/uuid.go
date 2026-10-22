package ingest

import "errors"

var errUUID = errors.New("invalid GPU UUID")

// parseUUID decodes "GPU-xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx" (the GPU-
// prefix is optional) into its 16 raw bytes without allocating.
func parseUUID(s string) ([16]byte, error) {
	var u [16]byte
	if len(s) == 40 && s[:4] == "GPU-" {
		s = s[4:]
	}
	if len(s) != 36 || s[8] != '-' || s[13] != '-' || s[18] != '-' || s[23] != '-' {
		return u, errUUID
	}
	j := 0
	for i := 0; i < 36; {
		if i == 8 || i == 13 || i == 18 || i == 23 {
			i++
			continue
		}
		hi, lo := unhex(s[i]), unhex(s[i+1])
		if hi > 15 || lo > 15 {
			return u, errUUID
		}
		u[j] = hi<<4 | lo
		j++
		i += 2
	}
	return u, nil
}

func unhex(c byte) byte {
	switch {
	case '0' <= c && c <= '9':
		return c - '0'
	case 'a' <= c && c <= 'f':
		return c - 'a' + 10
	case 'A' <= c && c <= 'F':
		return c - 'A' + 10
	}
	return 255
}
