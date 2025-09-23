package awareness

import (
	"github.com/named-data/ndnd/repo/tlv"
	"github.com/named-data/ndnd/repo/types"
	enc "github.com/named-data/ndnd/std/encoding"
	"github.com/named-data/ndnd/std/log"
	"github.com/named-data/ndnd/std/ndn"
	ndn_sync "github.com/named-data/ndnd/std/sync"
	"sync"
)

// FIXME: do mutex stuff
type Commands struct {
	mutex    sync.RWMutex
	nodeName *enc.Name
	prefix   *enc.Name

	// health ndn client
	client ndn.Client

	// store job targets, bool is whether it should be active
	jobs    map[*enc.Name]bool
	jLookup map[*enc.Name]*tlv.RepoCommand
	// FIXME: do svs group here
	cmdSvs *ndn_sync.SvsALO

	checkJob func(*tlv.RepoCommand)
}

// FIXME: add onUpdate, will require callback for checking a command's replica count
func NewCommands(repo *types.RepoShared) *Commands {
	name, err := enc.NameFromStr(repo.RepoNameN.String() + "/commands")
	if err != nil {
		panic("couldn't convert string to name in NewCommands")
	}
	return &Commands{
		nodeName: &repo.RepoNameN,
		prefix:   &name,
		client:   repo.Client,
	}
}

func (c *Commands) SetCheckJob(f func(*tlv.RepoCommand)) {
	c.checkJob = f
}

func (c *Commands) String() string {
	return "Commands"
}

func (c *Commands) Start() (err error) {
	// FIXME: actually do this correctly
	c.cmdSvs, err = ndn_sync.NewSvsALO(ndn_sync.SvsAloOpts{
		Name: c.nodeName.Clone(),
		Svs: ndn_sync.SvSyncOpts{
			Client:      c.client,
			GroupPrefix: c.prefix.Clone(),
		},
		Snapshot:        &ndn_sync.SnapshotNull{},
		FetchLatestOnly: false,
	})
	if err != nil {
		return err
	}
	// Set error handler
	c.cmdSvs.SetOnError(func(err error) {
		log.Error(c, "SVS ALO error", "err", err)
	})

	// Subscribe to all publishers
	c.cmdSvs.SubscribePublisher(enc.Name{}, func(pub ndn_sync.SvsPub) {
		if pub.IsSnapshot {
			log.Info(c, "Received snapshot publication", "pub", pub.Content)
			panic("Snapshot publications are not supported in Repo Commands")
		} else {
			// Process the publication.
			log.Debug(c, "Received non-snapshot publication", "pub", pub.Content)

			update, err := tlv.ParseRepoCommand(enc.NewWireView(pub.Content), true)
			if err != nil {
				panic(err)
			}

			c.addCommand(update)
		}
	})

	// Announce group prefix route
	for _, route := range []enc.Name{
		c.cmdSvs.SyncPrefix(),
		c.cmdSvs.DataPrefix(),
		c.prefix.Clone(),
	} {
		c.client.AnnouncePrefix(ndn.Announcement{
			Name:   route,
			Expose: true,
		})
	}

	// Start awareness SVS
	log.Info(c, "Starting commands svs")
	if err := c.cmdSvs.Start(); err != nil {
		log.Error(c, "Failed to start commands SVS", "err", err)
		return err
	}

	return err
}
func (c *Commands) Stop() {
	// FIXME: actually do this correctly
	c.cmdSvs.Stop()
}

func (c *Commands) Get(name *enc.Name) *tlv.RepoCommand {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	return c.jLookup[name]
}

// call this when you get an update from the Commands svs group
func (c *Commands) addCommand(command *tlv.RepoCommand) {
	log.Debug(c, "addCommand for", command)
	n := &command.Target
	// FIXME: could do 2 different mutexes, 1 for each map, but this shouldn't cause much delay
	c.mutex.Lock()
	c.jLookup[n] = command
	current := c.jobs[n]
	// NOTE: this checks the job only if the command resulted in a change in state
	// if the state didn't change, then there's no reason to trigger a check
	// FIXME: get this typing thing down, need to modify tlv, probably
	if command.Type == "INSERT" || command.Type == "JOIN" {
		if !current {
			c.jobs[n] = true
			c.mutex.Unlock()
			c.checkJob(command)
		} else {
			c.mutex.Unlock()
		}
	} else { // remove thing
		if current {
			c.jobs[n] = false
			c.mutex.Unlock()
			c.checkJob(command)
		} else {
			c.mutex.Unlock()
		}
	}
}

// FIXME: mutex and check if command exists
func (c *Commands) ShouldBeActive(command *tlv.RepoCommand) bool {
	return c.jobs[&command.Target]
}

// add command to local understanding and publish to the svs group
func (c *Commands) PublishCommand(command *tlv.RepoCommand) {
	// FIXME: decide here what to name it and signing and all that
	c.cmdSvs.Publish(command.Encode())

	c.addCommand(command)
}
