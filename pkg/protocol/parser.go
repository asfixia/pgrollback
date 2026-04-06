package protocol

import (
	"regexp"
)

// ParseApplicationIdentity reads startup parameters and returns:
//   - testID: session key for routing.
//   - displayName: for logs — "(sem application_name)" when unset; for pgrollback_* the same id as testID;
//     otherwise the raw application_name.
func ParseApplicationIdentity(params map[string]string) (testID string, displayName string) {
	// pgrollbackApplicationNameRegexp matches application_name values that identify a test session:
	// "pgrollback_<test_id>" or "pgrollback-<test_id>". The first submatch is test_id.
	var pgrollbackApplicationNameRegexp = regexp.MustCompile(`^pgrollback[_-](.+)$`)
	raw := ""
	if params != nil {
		raw = params["application_name"]
	}
	if raw == "" {
		return "default", "(sem application_name)"
	}
	if raw == "default" {
		return "default", "default"
	}
	if m := pgrollbackApplicationNameRegexp.FindStringSubmatch(raw); m != nil {
		return m[1], m[1]
	}
	return raw, raw
}

// ExtractAppname returns displayName only (see ParseApplicationIdentity).
func ExtractAppname(params map[string]string) string {
	_, display := ParseApplicationIdentity(params)
	return display
}

// ExtractTestID returns testID only (see ParseApplicationIdentity). Error is always nil today.
func ExtractTestID(params map[string]string) (string, error) {
	id, _ := ParseApplicationIdentity(params)
	return id, nil
}

func BuildStartupMessageForPostgres(params map[string]string) map[string]string {
	newParams := make(map[string]string)

	for k, v := range params {
		if k == "application_name" {
			newParams[k] = "pgrollback-proxy"
		} else {
			newParams[k] = v
		}
	}

	if newParams["application_name"] == "" {
		newParams["application_name"] = "pgrollback-proxy"
	}

	return newParams
}
