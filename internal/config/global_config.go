package config

import (
	"sync"
)

type GlobalConfig struct {
	instance   *Config
	configPath string
}

var (
	global *GlobalConfig
	once   sync.Once
)

func Init() {
	once.Do(func() {
		global = &GlobalConfig{
			instance:   nil,
			configPath: "",
		}
	})
}

func SetOnce(config *Config, cfgPath string) {
	if global.instance != nil {
		panic("AppConfig already initialized")
	}
	global.instance = config
	global.configPath = cfgPath
}

func GetCfg() *Config {
	if global.instance == nil {
		panic("AppConfig not initialized")
	}
	cloned := *global.instance
	return &cloned
}
