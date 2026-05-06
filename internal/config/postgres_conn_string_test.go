package config

import "testing"

func TestPostgresConnStringMasked(t *testing.T) {
	p := &PostgresConfig{
		Host:     "db.example.com",
		Port:     5432,
		Database: "appdb",
		User:     "appuser",
		Password: "real-secret",
	}
	got := PostgresConnStringMasked(p)
	want := "host=\"db.example.com\" port=\"5432\" dbname=\"appdb\" user=\"appuser\" password=\"" + PasswordMask + "\""
	if got != want {
		t.Fatalf("PostgresConnStringMasked() = %q, want %q", got, want)
	}
}

func TestPostgresConnStringMasked_IPv6Host(t *testing.T) {
	p := &PostgresConfig{
		Host:     "::1",
		Port:     5432,
		Database: "postgres",
		User:     "u",
		Password: "x",
	}
	got := PostgresConnStringMasked(p)
	want := "host=\"::1\" port=\"5432\" dbname=\"postgres\" user=\"u\" password=\"" + PasswordMask + "\""
	if got != want {
		t.Fatalf("PostgresConnStringMasked() = %q, want %q", got, want)
	}
}

func TestPostgresConnStringMasked_nil(t *testing.T) {
	if s := PostgresConnStringMasked(nil); s != "" {
		t.Fatalf("want empty, got %q", s)
	}
}
