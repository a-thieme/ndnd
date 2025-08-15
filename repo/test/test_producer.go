package main

import (
	"crypto/rand"
	// "os"
	// "os/signal"
	"strconv"
	// "syscall"
	"time"

	"github.com/named-data/ndnd/repo/tlv"
	enc "github.com/named-data/ndnd/std/encoding"
	"github.com/named-data/ndnd/std/engine"
	"github.com/named-data/ndnd/std/log"
	"github.com/named-data/ndnd/std/ndn"
	"github.com/named-data/ndnd/std/ndn/spec_2022"
	"github.com/named-data/ndnd/std/object"
	"github.com/named-data/ndnd/std/object/storage"
	"github.com/named-data/ndnd/std/types/optional"
)

const (
	repoName     = "/ndn/repo"
	producerName = "/test/producer/1"
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
	repoNameN     enc.Name
	producerNameN enc.Name
	notifyPrefix  enc.Name
}

func (p *TestRepoProducer) String() string {
	return "test-repo-producer"
}

func NewTestRepoProducer() *TestRepoProducer {
	repoNameN, _ := enc.NameFromStr(repoName)
	producerNameN, _ := enc.NameFromStr(producerName)
	notifyPrefix := repoNameN.Append(enc.NewGenericComponent("notify"))

	return &TestRepoProducer{
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
	p.store, err = storage.NewBadgerStore(producerName + "/badger")
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
func (p *TestRepoProducer) insertData(name enc.Name, size int) {
	log.Info(p, "Inserting data", "name", name, "size", size)

	content := make([]byte, size)
	rand.Read(content)

	// Put data in the store & announce prefix
	p.client.AnnouncePrefix(ndn.Announcement{
		Name:   name,
		Expose: true,
	})

	finalNameN, err := p.client.Produce(ndn.ProduceArgs{
		Name:    name.WithVersion(enc.VersionUnixMicro),
		Content: enc.Wire{content},
	})
	if err != nil {
		log.Error(p, "Failed to produce data", "finalName", finalNameN, "err", err)
		return
	}

	p.sendCommand(CommandTypeInsert, finalNameN)
}

// deleteData sends a command to the repo to delete a data packet
func (p *TestRepoProducer) deleteData(name enc.Name) {
	log.Info(p, "Deleting data", "name", name)
	p.sendCommand(CommandTypeDelete, name)
}

// joinRepo sends a command to the repo to join the repo
func (p *TestRepoProducer) joinRepo(groupName enc.Name) {
	log.Info(p, "Joining repo", "groupName", groupName)
	p.sendCommand(CommandTypeJoin, groupName)
}

// leaveRepo sends a command to the repo to leave the repo
func (p *TestRepoProducer) leaveRepo(groupName enc.Name) {
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
		CommandType: string(commandType),
		SrcName:     &spec_2022.NameContainer{Name: name},
		Nonce:       name.Hash(),
	}

	notifyInterestName := p.notifyPrefix.Append(enc.NewGenericComponent(strconv.FormatUint(name.Hash(), 10))) // TODO: embed a nonce
	log.Info(p, "Sending command", "commandType", commandType, "name", name, "notifyInterestName", notifyInterestName)
	p.client.ExpressR(ndn.ExpressRArgs{
		Name:     notifyInterestName,
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

// func main() {
// 	producer := NewTestRepoProducer()
// 	producer.Start()
// 	defer producer.Stop()

// 	for i := 0; i < 10; i++ {
// 		dataNameN, err := enc.NameFromStr(producerName + "/data/" + strconv.Itoa(i))
// 		if err != nil {
// 			log.Error(producer, "Failed to parse name", "name", dataNameN, "err", err)
// 			continue
// 		}
// 		producer.insertData(dataNameN, 1024)
// 	}

// 	sigChannel := make(chan os.Signal, 1)
// 	signal.Notify(sigChannel, os.Interrupt, syscall.SIGTERM)
// 	<-sigChannel
// }
