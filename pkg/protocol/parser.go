package protocol

import (
	"fmt"
	"regexp"
)

func ExtractTestID(params map[string]string) (string, error) {
	appName := params["application_name"]
	if appName == "" {
		return "", fmt.Errorf("application_name não fornecido")
	}

	match := regexp.MustCompile(`^pgtest_(.+)$`).FindStringSubmatch(appName)
	if match == nil {
		return "", fmt.Errorf("application_name deve estar no formato 'pgtest_<test_id>'")
	}

	return match[1], nil
}

func BuildStartupMessageForPostgres(params map[string]string) map[string]string {
	newParams := make(map[string]string)
	
	for k, v := range params {
		if k == "application_name" {
			newParams[k] = "pgtest-proxy"
		} else {
			newParams[k] = v
		}
	}

	if newParams["application_name"] == "" {
		newParams["application_name"] = "pgtest-proxy"
	}

	return newParams
}
