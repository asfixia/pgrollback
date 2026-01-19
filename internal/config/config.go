package config

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Postgres PostgresConfig `yaml:"postgres"`
	Proxy    ProxyConfig    `yaml:"proxy"`
	Logging  LoggingConfig  `yaml:"logging"`
}

type PostgresConfig struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	Database string `yaml:"database"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
}

type ProxyConfig struct {
	ListenPort int           `yaml:"listen_port"`
	Timeout    time.Duration `yaml:"timeout"`
}

type LoggingConfig struct {
	Level string `yaml:"level"`
	File  string `yaml:"file"`
}

func LoadConfig(configPath string) (*Config, error) {
	config := &Config{
		Postgres: PostgresConfig{
			Host:     "localhost",
			Port:     5432,
			Database: "postgres",
			User:     "postgres",
			Password: "",
		},
		Proxy: ProxyConfig{
			ListenPort: 5432,
			Timeout:    3600 * time.Second,
		},
		Logging: LoggingConfig{
			Level: "info",
			File:  "",
		},
	}

	if configPath != "" {
		data, err := os.ReadFile(configPath)
		if err == nil {
			if err := yaml.Unmarshal(data, config); err != nil {
				return nil, fmt.Errorf("failed to parse config file: %w", err)
			}
		}
	}

	loadFromEnv(config)

	if err := validateConfig(config); err != nil {
		return nil, err
	}

	return config, nil
}

func loadFromEnv(config *Config) {
	if host := os.Getenv("POSTGRES_HOST"); host != "" {
		config.Postgres.Host = host
	}
	if port := os.Getenv("POSTGRES_PORT"); port != "" {
		if p, err := strconv.Atoi(port); err == nil {
			config.Postgres.Port = p
		}
	}
	if db := os.Getenv("POSTGRES_DB"); db != "" {
		config.Postgres.Database = db
	}
	if user := os.Getenv("POSTGRES_USER"); user != "" {
		config.Postgres.User = user
	}
	if pass := os.Getenv("POSTGRES_PASSWORD"); pass != "" {
		config.Postgres.Password = pass
	}

	if port := os.Getenv("PGTEST_LISTEN_PORT"); port != "" {
		if p, err := strconv.Atoi(port); err == nil {
			config.Proxy.ListenPort = p
		}
	}
	if timeout := os.Getenv("PGTEST_TIMEOUT"); timeout != "" {
		if d, err := time.ParseDuration(timeout); err == nil {
			config.Proxy.Timeout = d
		}
	}

	if level := os.Getenv("PGTEST_LOG_LEVEL"); level != "" {
		config.Logging.Level = level
	}
	if file := os.Getenv("PGTEST_LOG_FILE"); file != "" {
		config.Logging.File = file
	}
}

func validateConfig(config *Config) error {
	if config.Postgres.Host == "" {
		return fmt.Errorf("POSTGRES_HOST is required")
	}
	if config.Postgres.Port == 0 {
		return fmt.Errorf("POSTGRES_PORT is required")
	}
	if config.Postgres.Database == "" {
		return fmt.Errorf("POSTGRES_DB is required")
	}
	if config.Postgres.User == "" {
		return fmt.Errorf("POSTGRES_USER is required")
	}
	return nil
}
