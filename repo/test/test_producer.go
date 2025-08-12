package main

import (
	"crypto/rand"
	"strconv"
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
	name := args.Interest.Name()
	data, err := p.store.Get(name, false)
	if err != nil {
		log.Warn(p, "No data", "name", name)
		return
	}

	args.Reply(enc.Wire{data})
}

// insertData inserts a randomly generated data of certain size to repo
func (p *TestRepoProducer) insertData(name enc.Name, size int) {
	log.Info(p, "Inserting data", "name", name, "size", size)

	data := make([]byte, size)
	rand.Read(data)

	// Put data in the store
	p.client.Store().Put(name, data)

	p.sendCommand(CommandTypeInsert, name)
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
	})
}

func main() {
	producer := NewTestRepoProducer()
	producer.Start()
	defer producer.Stop()

	for i := 0; i < 10; i++ {
		dataNameN, err := enc.NameFromStr("/test/data/" + strconv.Itoa(i))
		if err != nil {
			log.Error(producer, "Failed to parse name", "name", "/test/data/"+strconv.Itoa(i), "err", err)
			continue
		}
		producer.insertData(dataNameN, 1024)
	}
}
