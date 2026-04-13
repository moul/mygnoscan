package main

import (
	"encoding/json"
	"fmt"
	"os"
)

type NetworkConfig struct {
	ID         string `json:"id"`
	IndexerURL string `json:"indexer"`
	RPCURL     string `json:"rpc,omitempty"`
}

type AppConfig struct {
	Networks []NetworkConfig `json:"networks"`
}

var defaultConfig = &AppConfig{
	Networks: []NetworkConfig{
		{ID: "gnoland1", IndexerURL: "https://indexer.gno.land/graphql/query", RPCURL: "https://rpc.gno.land"},
		{ID: "test12", IndexerURL: "https://indexer.test12.moul.p2p.team/graphql"},
	},
}

func LoadConfig(path string) (*AppConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config %s: %w", path, err)
	}
	var cfg AppConfig
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	return &cfg, nil
}
