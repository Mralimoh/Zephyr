package config

import (
	"encoding/json"
	"fmt"
	"os"

	"Zephyr/internal/httpclient"
)

type AppConfig struct {
	ListenAddr string `json:"listen_addr,omitempty"`
	ClientID string `json:"client_id,omitempty"`
	GoogleFolderID string `json:"google_folder_id,omitempty"`
	Transport httpclient.TransportConfig `json:"transport,omitempty"`
}

func (c *AppConfig) Save(path string) error {
	b, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, b, 0600)
}

func Load(path string) (*AppConfig, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file %s: %w", path, err)
	}

	var cfg AppConfig
	if err := json.Unmarshal(b, &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse config JSON: %w", err)
	}

	return &cfg, nil
}
