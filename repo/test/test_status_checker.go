package main

import (
	"strconv"
	"time"

	"github.com/named-data/ndnd/repo/tlv"
	enc "github.com/named-data/ndnd/std/encoding"
	"github.com/named-data/ndnd/std/engine"
	"github.com/named-data/ndnd/std/log"
	"github.com/named-data/ndnd/std/ndn"
	"github.com/named-data/ndnd/std/ndn/spec_2022"
	"github.com/named-data/ndnd/std/types/optional"
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

	// Send status request interest to repo
	app := engine.NewBasicEngine(engine.NewDefaultFace())
	err := app.Start()
	if err != nil {
		log.Fatal(nil, "Unable to start engine", "err", err)
		return
	}
	defer app.Stop()

	statusRequest := &spec_2022.NameContainer{Name: resourceNameN}
	statusRequestInterest, _ := app.Spec().MakeInterest(
		repoNameN.Append(enc.NewGenericComponent("status")).
			// FIXME: probably don't include this, but leave just in case for now
			Append(enc.NewGenericComponent(strconv.FormatUint(resourceNameN.Hash(), 10))),
		&ndn.InterestConfig{
			MustBeFresh: true,
		},
		statusRequest.Encode(),
		nil,
	)

	log.Info(nil, "Sending status request interest", "interest", statusRequestInterest.FinalName.String())

	ch := make(chan ndn.ExpressCallbackArgs)
	app.Express(statusRequestInterest, func(args ndn.ExpressCallbackArgs) {
		ch <- args
	})

	select {
	case args := <-ch:
		if args.Result == ndn.InterestResultData {
			reply, _ := tlv.ParseRepoStatusResponse(enc.NewWireView(args.Data.Content()), false)
			log.Info(nil, "Received status response", "response", reply)
		} else {
			log.Error(nil, "Failed to receive status response", "result", args.Result)
		}
	case <-time.After(3 * time.Second):
		log.Error(nil, "Timeout waiting for status response")
	}
}

// func main() {
// 	cmdTestStatusChecker.Execute()
// }
