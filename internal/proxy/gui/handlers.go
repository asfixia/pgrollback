package gui

import (
	"encoding/json"
	"net/http"
	"strings"

	"pgtest-transient/internal/config"
)

// ConfigResponse is the config returned by GET /api/config (includes masked password and config_path).
type ConfigResponse struct {
	ConfigPath string         `json:"config_path"`
	Config     *config.Config `json:"config"`
}

func handleAPISessions(provider SessionProvider) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		list := provider.GetSessions()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(list)
	}
}

func handleAPISessionsClose(provider SessionProvider) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		var testID string
		if ct := r.Header.Get("Content-Type"); strings.Contains(ct, "application/json") {
			var body struct {
				TestID string `json:"test_id"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				http.Error(w, "invalid JSON", http.StatusBadRequest)
				return
			}
			testID = body.TestID
		} else {
			testID = r.URL.Query().Get("test_id")
			if testID == "" {
				testID = r.FormValue("test_id")
			}
		}
		if testID == "" {
			http.Error(w, "test_id required", http.StatusBadRequest)
			return
		}
		if err := provider.DestroySession(testID); err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK"))
	}
}

func handleAPISessionsClearHistory(provider SessionProvider) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		var testID string
		if ct := r.Header.Get("Content-Type"); strings.Contains(ct, "application/json") {
			var body struct {
				TestID string `json:"test_id"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				http.Error(w, "invalid JSON", http.StatusBadRequest)
				return
			}
			testID = body.TestID
		} else {
			testID = r.URL.Query().Get("test_id")
			if testID == "" {
				testID = r.FormValue("test_id")
			}
		}
		if testID == "" {
			http.Error(w, "test_id required", http.StatusBadRequest)
			return
		}
		if err := provider.ClearHistory(testID); err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK"))
	}
}

func handleAPIConfigGet(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	cfg, ok := config.GetCfgIfSet()
	if !ok {
		w.WriteHeader(http.StatusServiceUnavailable)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "config not initialized"})
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(ConfigResponse{
		ConfigPath: config.GetConfigPath(),
		Config:     config.ConfigForAPI(cfg),
	})
}

func handleAPIConfigSave(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost && r.Method != http.MethodPut {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	var payload struct {
		Config *config.Config `json:"config"`
	}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, "invalid JSON: "+err.Error(), http.StatusBadRequest)
		return
	}
	if payload.Config == nil {
		http.Error(w, "config required", http.StatusBadRequest)
		return
	}
	if err := config.UpdateAndSave(payload.Config); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("OK"))
}
