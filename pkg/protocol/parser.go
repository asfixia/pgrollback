package protocol

import (
	"regexp"
)

func ExtractTestID(params map[string]string) (string, error) {
	appName := params["application_name"]

	// Se não há application_name ou é 'default', usa conexão compartilhada
	if appName == "" || appName == "default" {
		return "default", nil
	}

	// Verifica se está no formato pgtest_<test_id>
	match := regexp.MustCompile(`^pgtest_(.+)$`).FindStringSubmatch(appName)
	if match != nil {
		return match[1], nil
	}

	// Qualquer outro application_name (como "pgAdmin", "psql", etc.) usa conexão compartilhada
	// O application_name será definido como "pgtest_default" ao conectar ao PostgreSQL real
	return "default", nil
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
