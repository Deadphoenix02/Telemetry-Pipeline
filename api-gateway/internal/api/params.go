package api

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

// ParseGPUID accepts the DCGM form "GPU-xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
// or the bare UUID, as the broker does when it ingests them.
func ParseGPUID(s string) (uuid.UUID, error) {
	if len(s) == 40 && strings.EqualFold(s[:4], "GPU-") {
		s = s[4:]
	}
	if len(s) != 36 { // uuid.Parse also takes urn:, braces and no dashes; the pipeline never emits those
		return uuid.UUID{}, errors.New("invalid GPU id: want GPU-xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx")
	}
	id, err := uuid.Parse(s)
	if err != nil {
		return uuid.UUID{}, errors.New("invalid GPU id: want GPU-xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx")
	}
	return id, nil
}

// FormatGPUID is the id the API returns: the form the GPUs report themselves.
func FormatGPUID(id uuid.UUID) string { return "GPU-" + id.String() }

// maxLimitParam only keeps limit+1 from overflowing; it is no practical cap.
const maxLimitParam = 1_000_000_000

// parseLimit reads the optional limit; 0 means no limit, return everything.
func parseLimit(s string) (int, error) {
	if s == "" {
		return 0, nil
	}
	n, err := strconv.Atoi(s)
	if err != nil || n < 1 || n > maxLimitParam {
		return 0, fmt.Errorf("limit must be an integer from 1 to %d; leave it out to get everything", maxLimitParam)
	}
	return n, nil
}

// parseTime reads an RFC 3339 timestamp, with optional fractional seconds.
func parseTime(name, s string) (*time.Time, error) {
	if s == "" {
		return nil, nil
	}
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil && strings.Contains(s, " ") {
		// An unescaped "+05:30" offset arrives as " 05:30": a query string decodes '+' to a space.
		t, err = time.Parse(time.RFC3339Nano, strings.ReplaceAll(s, " ", "+"))
	}
	if err != nil {
		return nil, fmt.Errorf("%s must be an RFC 3339 timestamp such as 2025-07-18T20:42:34Z", name)
	}
	return &t, nil
}

// checkParams rejects parameters the route does not know, so a typo such as
// startTime fails loudly instead of silently returning the whole history.
func checkParams(q map[string][]string, allowed ...string) error {
	for k := range q {
		ok := false
		for _, a := range allowed {
			ok = ok || k == a
		}
		if !ok {
			return fmt.Errorf("unknown query parameter %q (allowed: %s)", k, strings.Join(allowed, ", "))
		}
	}
	return nil
}
