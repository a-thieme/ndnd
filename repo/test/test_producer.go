package main

import (
	"crypto/rand"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/named-data/ndnd/repo/tlv"
	enc "github.com/named-data/ndnd/std/encoding"
	"github.com/named-data/ndnd/std/engine"
	"github.com/named-data/ndnd/std/log"
	"github.com/named-data/ndnd/std/ndn"
	"github.com/named-data/ndnd/std/object"
	"github.com/named-data/ndnd/std/object/storage"
	"github.com/named-data/ndnd/std/types/optional"
)

type CommandType string

const (
	CommandTypeInsert CommandType = "INSERT"
	CommandTypeDelete CommandType = "DELETE"
	CommandTypeJoin   CommandType = "JOIN"
	CommandTypeLeave  CommandType = "LEAVE"
)

type TestRepoProducer struct {
	client        ndn.Client
	store         ndn.Store
	engine        ndn.Engine
	repoName      string
	producerName  string
	repoNameN     enc.Name
	producerNameN enc.Name
	notifyPrefix  enc.Name
	statusPrefix  enc.Name
}

func (p *TestRepoProducer) String() string {
	return "test-repo-producer"
}

func NewTestRepoProducer(repoName string, producerName string) *TestRepoProducer {
	repoNameN, _ := enc.NameFromStr(repoName)
	producerNameN, _ := enc.NameFromStr(producerName)
	notifyPrefix := repoNameN.Append(enc.NewGenericComponent("notify"))

	return &TestRepoProducer{
		repoName:      repoName,
		producerName:  producerName,
		repoNameN:     repoNameN,
		producerNameN: producerNameN,
		notifyPrefix:  notifyPrefix,
	}
}

func (p *TestRepoProducer) Start() error {
	// Create engine
	p.engine = engine.NewBasicEngine(engine.NewDefaultFace())
	err := p.engine.Start()
	if err != nil {
		log.Fatal(nil, "Unable to start engine", "err", err)
		return err
	}

	// Make object store database
	p.store, err = storage.NewBadgerStore("/home/adam/.ndn/producers" + p.producerName + "/badger")
	if err != nil {
		log.Error(nil, "Unable to create store", "err", err)
		return err
	}

	// Create client
	p.client = object.NewClient(p.engine, p.store, nil)

	// Attach interest handler
	p.engine.AttachHandler(p.producerNameN, p.OnInterest)

	return nil
}

func (p *TestRepoProducer) Stop() error {
	// Stop client
	if err := p.client.Stop(); err != nil {
		log.Error(nil, "Unable to stop client", "err", err)
		return err
	}

	// Stop engine
	if err := p.engine.Stop(); err != nil {
		log.Error(nil, "Unable to stop engine", "err", err)
		return err
	}

	return nil
}

func (p *TestRepoProducer) OnInterest(args ndn.InterestHandlerArgs) {
	log.Info(p, "OnInterest", "name", args.Interest.Name())

	name := args.Interest.Name()
	wire, err := p.store.Get(name, args.Interest.CanBePrefix())
	if err != nil || wire == nil {
		log.Warn(p, "No data", "name", name)
		return
	}

	log.Info(p, "Replied data", "name", name)
	args.Reply(enc.Wire{wire})
}

// insertData inserts a randomly generated data of certain size to repo
func (p *TestRepoProducer) insertData(name enc.Name, size int) enc.Name {
	log.Info(p, "Inserting data", "name", name, "size", size)

	// Put data in the store & announce prefix
	p.client.AnnouncePrefix(ndn.Announcement{
		Name:   name,
		Expose: true,
	})

	content := make([]byte, size)
	rand.Read(content)

	finalNameN, err := p.client.Produce(ndn.ProduceArgs{
		Name:    name.WithVersion(enc.VersionUnixMicro),
		Content: enc.Wire{content},
	})
	if err != nil {
		log.Error(p, "Failed to produce data", "finalName", finalNameN, "err", err)
		return finalNameN
	}

	p.sendCommand(CommandTypeInsert, finalNameN)
	return finalNameN
}

// deleteData sends a command to the repo to delete a data packet
func (p *TestRepoProducer) deleteData(name enc.Name) {
	log.Info(p, "Deleting data", "name", name)
	p.sendCommand(CommandTypeDelete, name)
}

// joinGroup sends a command to the repo to join the sync group
func (p *TestRepoProducer) joinGroup(groupName enc.Name) {
	log.Info(p, "Joining repo", "groupName", groupName)
	p.sendCommand(CommandTypeJoin, groupName)
}

// leaveGroup sends a command to the repo to leave the sync group
func (p *TestRepoProducer) leaveGroup(groupName enc.Name) {
	log.Info(p, "Leaving repo", "groupName", groupName)
	p.sendCommand(CommandTypeLeave, groupName)
}

// sendCommand sends a command to the repo
func (p *TestRepoProducer) sendCommand(commandType CommandType, name enc.Name) {
	// Add nil check to prevent segmentation fault
	if name == nil {
		log.Error(p, "Cannot send command with nil name", "commandType", commandType)
		return
	}

	commandData := tlv.RepoCommand{
		Type:   string(commandType),
		Target: name,
	}

	// FIXME: see if this needs to be added. the way it is now, this is equivalent to the digest
	notifyInterestName := p.notifyPrefix.Append(enc.NewGenericComponent(strconv.FormatUint(name.Hash(), 10)))
	log.Info(p, "Sending command", "commandType", commandType, "name", name, "notifyInterestName", notifyInterestName)
	p.client.ExpressR(ndn.ExpressRArgs{
		Name:     p.notifyPrefix,
		AppParam: commandData.Encode(),
		Config: &ndn.InterestConfig{
			MustBeFresh: true,
			Lifetime:    optional.Some(10 * time.Second),
		},
		Retries: 0,
		Callback: func(args ndn.ExpressCallbackArgs) {
			if args.Result == ndn.InterestResultData {
				log.Info(p, "Command received by Repo", "command", commandType, "name", name)
			} else {
				log.Error(p, "Command error", "command", commandType, "name", name, "result", args.Result)
			}
		},
	})
}

// sendStatusRequest sends a status request to the repo
func (p *TestRepoProducer) sendStatusRequest(target enc.Name) {
	log.Info(p, "Sending status request", "name", target)

	// Add nil check to prevent segmentation fault
	if target == nil {
		log.Error(p, "Cannot send status request with nil name")
		return
	}

	statusRequest := &tlv.RepoStatusRequest{
		Target: target,
	}

	// TODO: make sure this is necessary
	statusRequestInterestName := p.statusPrefix.Append(enc.NewGenericComponent(strconv.FormatUint(target.Hash(), 10)))
	log.Info(p, "Sending status request", "target", target, "statusRequestInterestName", statusRequestInterestName)

	p.client.ExpressR(ndn.ExpressRArgs{
		Name:     statusRequestInterestName,
		AppParam: statusRequest.Encode(),
		Config: &ndn.InterestConfig{
			MustBeFresh: true,
			Lifetime:    optional.Some(10 * time.Second),
		},
		Retries: 0,
		Callback: func(args ndn.ExpressCallbackArgs) {
			if args.Result == ndn.InterestResultData {
				reply, err := tlv.ParseRepoStatusResponse(enc.NewWireView(args.Data.Content()), false)
				if err != nil {
					log.Error(p, "Failed to parse status reply", "target", target, "err", err)
					return
				}
				log.Info(p, "Received status request from Repo", "target", target, "status", reply.Status)
			} else {
				log.Error(p, "Status request error", "target", target, "result", args.Result)
			}
		},
	})

}

func main() {
	if len(os.Args) < 3 {
		log.Fatal(nil, "Usage: test_producer <repoName> <producerName>")
		os.Exit(1)
	}
	repoName := os.Args[1]
	producerName := os.Args[2]

	producer := NewTestRepoProducer(repoName, producerName)
	producer.Start()
	defer producer.Stop()

	totalData := 10
	checkData := make([]enc.Name, totalData)

	for i := 0; i < totalData; i++ {
		dataNameN, _ := enc.NameFromStr(producerName + "/data/" + strconv.Itoa(i))
		producer.client.AnnouncePrefix(ndn.Announcement{
			Name:   dataNameN,
			Expose: true,
		})
	}

	time.Sleep(3 * time.Second)

	// for i := 0; i < totalData; i++ {
	dataNameN, err := enc.NameFromStr(producerName + "/data/1")

	if err != nil {
		log.Error(producer, "Failed to parse name", "name", dataNameN, "err", err)
	} else {
		checkData[1] = producer.insertData(dataNameN, 1024*1024) // 1MB
	}
	// }
	//
	// time.Sleep(4 * time.Second) // So the repo has time to process the data
	// for i := 0; i < totalData; i++ {
	// 	dataNameN, err := enc.NameFromStr(producerName + "/data/" + strconv.Itoa(i))
	// 	if err != nil {
	// 		log.Error(producer, "Failed to parse name", "name", dataNameN, "err", err)
	// 		continue
	// 	}
	// 	producer.sendStatusRequest(checkData[i])
	// }
	//
	// time.Sleep(4 * time.Second)
	// for i := 0; i < totalData; i++ {
	// 	groupNameN, err := enc.NameFromStr("/test/group/" + strconv.Itoa(i))
	// 	if err != nil {
	// 		log.Error(producer, "Failed to parse name", "name", groupNameN, "err", err)
	// 		continue
	// 	}
	// 	producer.joinGroup(groupNameN)
	// }
	//
	// time.Sleep(4 * time.Second)
	// for i := 0; i < totalData; i++ {
	// 	groupNameN, err := enc.NameFromStr("/test/group/" + strconv.Itoa(i))
	// 	if err != nil {
	// 		log.Error(producer, "Failed to parse name", "name", groupNameN, "err", err)
	// 		continue
	// 	}
	// 	producer.sendStatusRequest(groupNameN)
	// }

	sigChannel := make(chan os.Signal, 1)
	signal.Notify(sigChannel, os.Interrupt, syscall.SIGTERM)
	<-sigChannel
}
