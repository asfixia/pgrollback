package protocol_test

import (
	"testing"

	"pgrollback/pkg/protocol"
)

func TestParseApplicationIdentity(t *testing.T) {
	tests := []struct {
		name        string
		params      map[string]string
		wantTestID  string
		wantDisplay string
	}{
		{
			name:        "nil params",
			params:      nil,
			wantTestID:  "default",
			wantDisplay: "(sem application_name)",
		},
		{
			name: "pgrollback underscore",
			params: map[string]string{
				"application_name": "pgrollback_my_test",
			},
			wantTestID:  "my_test",
			wantDisplay: "my_test",
		},
		{
			name: "pgrollback dash",
			params: map[string]string{
				"application_name": "pgrollback-abc",
			},
			wantTestID:  "abc",
			wantDisplay: "abc",
		},
		{
			name: "other client",
			params: map[string]string{
				"application_name": "pgAdmin 4",
			},
			wantTestID:  "pgAdmin 4",
			wantDisplay: "pgAdmin 4",
		},
		{
			name: "literal default",
			params: map[string]string{
				"application_name": "default",
			},
			wantTestID:  "default",
			wantDisplay: "default",
		},
		{
			name: "invalid_format style",
			params: map[string]string{
				"application_name": "invalid_format",
			},
			wantTestID:  "invalid_format",
			wantDisplay: "invalid_format",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotID, gotDisp := protocol.ParseApplicationIdentity(tt.params)
			if gotID != tt.wantTestID {
				t.Errorf("testID = %q, want %q", gotID, tt.wantTestID)
			}
			if gotDisp != tt.wantDisplay {
				t.Errorf("displayName = %q, want %q", gotDisp, tt.wantDisplay)
			}
			// Wrappers stay aligned
			if testID, _ := protocol.ParseApplicationIdentity(tt.params); testID != tt.wantTestID {
				t.Errorf("ExtractTestID = %q, want %q", testID, tt.wantTestID)
			}
			if _, displayName := protocol.ParseApplicationIdentity(tt.params); displayName != tt.wantDisplay {
				t.Errorf("ExtractAppname = %q, want %q", displayName, tt.wantDisplay)
			}
		})
	}
}

func TestExtractAppname(t *testing.T) {
	tests := []struct {
		name   string
		params map[string]string
		want   string
	}{
		{
			name: "pgrollback prefix returns test id only",
			params: map[string]string{
				"application_name": "pgrollback_my_test",
			},
			want: "my_test",
		},
		{
			name: "pgrollback-dash prefix",
			params: map[string]string{
				"application_name": "pgrollback-abc",
			},
			want: "abc",
		},
		{
			name: "no prefix returns full application_name",
			params: map[string]string{
				"application_name": "pgAdmin 4",
			},
			want: "pgAdmin 4",
		},
		{
			name: "missing application_name",
			params: map[string]string{
				"database": "x",
			},
			want: "(sem application_name)",
		},
		{
			name: "empty application_name",
			params: map[string]string{
				"application_name": "",
			},
			want: "(sem application_name)",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, got := protocol.ParseApplicationIdentity(tt.params)
			if got != tt.want {
				t.Errorf("ExtractAppname() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestExtractTestID(t *testing.T) {
	tests := []struct {
		name    string
		params  map[string]string
		want    string
		wantErr bool
	}{
		{
			name: "valid test id",
			params: map[string]string{
				"application_name": "pgrollback_abc123",
			},
			want:    "abc123",
			wantErr: false,
		},
		{
			name: "valid test id with underscore",
			params: map[string]string{
				"application_name": "pgrollback_test_123",
			},
			want:    "test_123",
			wantErr: false,
		},
		{
			name: "missing application_name",
			params: map[string]string{
				"database": "mydb",
			},
			want:    "default",
			wantErr: false,
		},
		{
			name: "invalid format",
			params: map[string]string{
				"application_name": "invalid_format",
			},
			want:    "invalid_format",
			wantErr: false,
		},
		{
			name: "empty application_name",
			params: map[string]string{
				"application_name": "",
			},
			want:    "default",
			wantErr: false,
		},
		{
			name: "default application_name",
			params: map[string]string{
				"application_name": "default",
			},
			want:    "default",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, _ := protocol.ParseApplicationIdentity(tt.params)
			if got != tt.want {
				t.Errorf("ExtractTestID() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBuildStartupMessageForPostgres(t *testing.T) {
	tests := []struct {
		name   string
		params map[string]string
		want   string
	}{
		{
			name: "replace application_name",
			params: map[string]string{
				"application_name": "pgrollback_abc123",
				"database":         "mydb",
			},
			want: "pgrollback-proxy",
		},
		{
			name: "add application_name if missing",
			params: map[string]string{
				"database": "mydb",
			},
			want: "pgrollback-proxy",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := protocol.BuildStartupMessageForPostgres(tt.params)
			if got["application_name"] != tt.want {
				t.Errorf("BuildStartupMessageForPostgres() application_name = %v, want %v", got["application_name"], tt.want)
			}
		})
	}
}
