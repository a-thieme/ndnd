package repo

import (
	"fmt"
	"os"
	"path/filepath"

	enc "github.com/named-data/ndnd/std/encoding"
)

type RepoGroupConfig struct {
	// RepoName is the name of the repo service.
	RepoName string `json:"repo_name"`
	// Number of replicas
	NumReplicas int `json:"num_replicas"`
	// Heartbeat interval
	HeartbeatInterval float64 `json:"heartbeat_interval"`
	// Heartbeat expiry
	HeartbeatExpiry float64 `json:"heartbeat_expiry"`

	// RepoNameN is the parsed name of the repo service.
	RepoNameN enc.Name
}

func (c *RepoGroupConfig) ParseGroupConfig() (err error) {
	c.RepoNameN, err = enc.NameFromStr(c.RepoName)
	if err != nil || len(c.RepoNameN) == 0 {
		return fmt.Errorf("failed to parse or invalid repo name (%s): %w", c.RepoName, err)
	}
	if c.NumReplicas <= 0 {
		return fmt.Errorf("num_replicas must be positive")
	}
	if c.HeartbeatInterval <= 0 {
		return fmt.Errorf("heartbeat_interval must be positive")
	}
	if c.HeartbeatExpiry <= 0 {
		return fmt.Errorf("heartbeat_expiry must be positive")
	}

	return nil
}

func DefaultGroupConfig() *RepoGroupConfig {
	return &RepoGroupConfig{
		RepoName:          "", // invalid
		NumReplicas:       3,
		HeartbeatInterval: 5.0,
		HeartbeatExpiry:   20.0,

		RepoNameN: nil,
	}
}

type RepoNodeConfig struct {
	// NodeName is the name of the node
	NodeName string `json:"node_name"`
	// StorageDir is the directory to store data.
	StorageDir string `json:"storage_dir"`
	// URI specifying KeyChain location.
	KeyChainUri string `json:"keychain"`
	// List of trust anchor full names.
	TrustAnchors []string `json:"trust_anchors"`

	// NodeNameN is the parsed name of the node
	NodeNameN enc.Name
}

func (c *RepoNodeConfig) ParseNodeConfig() (err error) {
	c.NodeNameN, err = enc.NameFromStr(c.NodeName)
	if err != nil || len(c.NodeNameN) == 0 {
		return fmt.Errorf("failed to parse or invalid node name (%s): %w", c.NodeName, err)
	}

	if c.StorageDir == "" {
		return fmt.Errorf("storage-dir must be set")
	} else {
		path, err := filepath.Abs(c.StorageDir)
		if err != nil {
			return fmt.Errorf("failed to get absolute path: %w", err)
		}
		if err := os.MkdirAll(path, 0755); err != nil {
			return fmt.Errorf("failed to create storage directory: %w", err)
		}
		c.StorageDir = path
	}
	return nil
}

func (c *RepoNodeConfig) TrustAnchorNames() []enc.Name {
	res := make([]enc.Name, len(c.TrustAnchors))
	for i, ta := range c.TrustAnchors {
		var err error
		res[i], err = enc.NameFromStr(ta)
		if err != nil {
			panic(fmt.Sprintf("failed to parse trust anchor name (%s): %v", ta, err))
		}
	}
	return res
}

func DefaultNodeConfig() *RepoNodeConfig {
	return &RepoNodeConfig{
		NodeName:   "", // invalid
		StorageDir: "", // invalid

		NodeNameN: nil,
	}
}
