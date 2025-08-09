package repo

import (
	"fmt"
	"os"
	"path/filepath"

	enc "github.com/named-data/ndnd/std/encoding"
)

type RepoConfig struct {
	// Name is the name of the repo service.
	RepoName string `json:"repo_name"`
	// NodeName is the name of the node
	NodeName string `json:"node_name"`
	// StorageDir is the directory to store data.
	StorageDir string `json:"storage_dir"`
	// URI specifying KeyChain location.
	KeyChainUri string `json:"keychain"`
	// List of trust anchor full names.
	TrustAnchors []string `json:"trust_anchors"`

	// RepoNameN is the parsed name of the repo service.
	RepoNameN enc.Name
	// NodeNameN is the parsed name of the node
	NodeNameN enc.Name
}

func (c *RepoConfig) Parse() (err error) {
	c.RepoNameN, err = enc.NameFromStr(c.RepoName)
	if err != nil || len(c.RepoNameN) == 0 {
		return fmt.Errorf("failed to parse or invalid repo name (%s): %w", c.RepoName, err)
	}
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

func (c *RepoConfig) TrustAnchorNames() []enc.Name {
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

func DefaultConfig() *RepoConfig {
	return &RepoConfig{
		RepoName:   "", // invalid
		NodeName:   "", // invalid
		StorageDir: "", // invalid

		RepoNameN: nil,
		NodeNameN: nil,
	}
}
