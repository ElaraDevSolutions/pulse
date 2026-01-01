package pulse

import (
	"os"
	"path/filepath"

	"gopkg.in/yaml.v3"
)

type BrokerConfig struct {
	Host      string `yaml:"host"`
	HTTPPort  int    `yaml:"http_port"`
	GRPCPort  int    `yaml:"grpc_port"`
	TimeoutMS int    `yaml:"timeout_ms"`
}

type ClientConfig struct {
	ID         string `yaml:"id"`
	AutoCommit bool   `yaml:"auto_commit"`
	MaxRetries int    `yaml:"max_retries"`
}

type TopicConfig struct {
	Name   string `yaml:"name"`
	Create bool   `yaml:"create_if_missing"`
	Config struct {
		FIFO           bool  `yaml:"fifo"`
		RetentionBytes int64 `yaml:"retention_bytes"`
	} `yaml:"config"`
}

type Config struct {
	Broker BrokerConfig  `yaml:"broker"`
	Client ClientConfig  `yaml:"client"`
	Topics []TopicConfig `yaml:"topics"`
}

var defaultConfig = Config{
	Broker: BrokerConfig{
		Host:      "localhost",
		HTTPPort:  5555,
		GRPCPort:  5556,
		TimeoutMS: 5000,
	},
	Client: ClientConfig{
		ID:         "go-client",
		AutoCommit: true,
		MaxRetries: 3,
	},
}

var loadedConfig *Config

func GetConfig() *Config {
	if loadedConfig == nil {
		loadedConfig = loadConfig()
	}
	return loadedConfig
}

func loadConfig() *Config {
	cfg := defaultConfig

	candidates := []string{
		"pulse.yaml",
		"pulse.yml",
	}

	home, err := os.UserHomeDir()
	if err == nil {
		candidates = append(candidates, filepath.Join(home, ".pulse", "pulse.yml"))
	}

	for _, path := range candidates {
		if _, err := os.Stat(path); err == nil {
			f, err := os.Open(path)
			if err == nil {
				defer f.Close()
				decoder := yaml.NewDecoder(f)
				if err := decoder.Decode(&cfg); err == nil {
					break
				}
			}
		}
	}
	return &cfg
}
