package repo

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/named-data/ndnd/std/log"
	"github.com/named-data/ndnd/std/utils"
	"github.com/named-data/ndnd/std/utils/toolutils"
	"github.com/spf13/cobra"
)

var CmdRepo = &cobra.Command{
	Use:     "repo CONFIG-FILE",
	Short:   "Named Data Networking Data Repository",
	GroupID: "run",
	Version: utils.NDNdVersion,
	Args:    cobra.ExactArgs(2),
	Run:     run,
}

func run(cmd *cobra.Command, args []string) {
	log.Default().SetLevel(log.LevelTrace)
	log.Trace(nil, "trace log")
	log.Debug(nil, "debug log")
	log.Info(nil, "Info log")
	config := struct {
		Group *RepoGroupConfig `json:"repo_group"`
		Repo  *RepoNodeConfig  `json:"repo"`
	}{
		Group: DefaultGroupConfig(),
		Repo:  DefaultNodeConfig(),
	}
	toolutils.ReadYaml(&config, args[0])
	toolutils.ReadYaml(&config, args[1])

	if err := config.Group.ParseGroupConfig(); err != nil {
		log.Fatal(nil, "Configuration error", "err", err)
	}
	if err := config.Repo.ParseNodeConfig(); err != nil {
		log.Fatal(nil, "Configuration error", "err", err)
	}

	repo := NewRepo(config.Group, config.Repo)
	err := repo.Start()
	if err != nil {
		log.Fatal(nil, "Failed to start repo", "err", err)
	}
	defer repo.Stop()

	sigChannel := make(chan os.Signal, 1)
	signal.Notify(sigChannel, os.Interrupt, syscall.SIGTERM)
	<-sigChannel
}
