package main

import (
	"fmt"

	"github.com/named-data/ndnd/repo/tlv"
	enc "github.com/named-data/ndnd/std/encoding"
	"github.com/named-data/ndnd/std/engine"
	"github.com/named-data/ndnd/std/log"
	"github.com/named-data/ndnd/std/object"
	"github.com/named-data/ndnd/std/utils"
	"github.com/spf13/cobra"
)

var cmdTestStatusChecker = &cobra.Command{
	Use:     "repo-status-check [REPO NAME] [RESOURCE NAME]",
	Short:   "A test tool to check the status of a resource in a repo",
	GroupID: "test",
	Version: utils.NDNdVersion,
	Args:    cobra.ExactArgs(2),
	Run:     run,
}

func run(cmd *cobra.Command, args []string) {
	repoNameN, _ := enc.NameFromStr(args[0])
	resourceNameN, _ := enc.NameFromStr(args[1])
	myName, _ := enc.NameFromStr("tester")

	// Send status request interest to repo
	app := engine.NewBasicEngine(engine.NewDefaultFace())
	err := app.Start()
	if err != nil {
		log.Fatal(nil, "Unable to start engine", "err", err)
		return
	}
	defer app.Stop()

	client := object.NewClient(app, nil, nil)
	sr := tlv.RepoStatusRequest{Target: resourceNameN}
	client.ExpressCommand(
		repoNameN.Append(enc.NewGenericComponent("status")),
		myName,
		sr.Encode(),
		func(w enc.Wire, e error) {
			if e != nil {
				return
			}
			tlv.ParseRepoStatusResponse(enc.NewWireView(w), false)
			sr, err := tlv.ParseRepoStatusResponse(enc.NewWireView(w), false)
			if err != nil {
				fmt.Println("sr error:", err.Error())
			}

			fmt.Println("got status response:", sr)
		})
}

// func main() {
// 	cmdTestStatusChecker.Execute()
// }
