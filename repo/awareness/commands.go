package awareness

import (
	"github.com/named-data/ndnd/repo/tlv"
	enc "github.com/named-data/ndnd/std/encoding"
	ndn_sync "github.com/named-data/ndnd/std/sync"
)

type Commands struct {
	// store job targets, bool is whether it should be active
	jobs map[*enc.Name]bool
	// do svs group here
	cmdSvs   *ndn_sync.SvsALO
	checkJob func(*tlv.RepoCommand)
}

// FIXME: add onUpdate, will require callback for checking a command's replica count
func NewCommands() *Commands {
	return &Commands{}
}

func (c *Commands) Start() {
	// FIXME: actually do this correctly
	c.cmdSvs.Start()
}
func (c *Commands) Stop() {
	// FIXME: actually do this correctly
	c.cmdSvs.Stop()
}

// call this when you get an update from the Commands svs group
func (c *Commands) addCommand(command *tlv.RepoCommand) {
	// FIXME: get this typing thing down, need to modify tlv, probably
	n := command.Target
	current := c.jobs[n]
	if command.Type == "INSERT" || command.Type == "JOIN" {
		if !current {
			c.jobs[n] = true
			c.checkJob(command)
		}
	} else { // remove thing
		if current {
			c.jobs[n] = false
			// FIXME: checkJob should only check the push but this command is a pop
			c.checkJob(command)
		}
	}
}

// add command to local understanding and publish to the svs group
func (c *Commands) PublishCommand(command *tlv.RepoCommand) {
	c.addCommand(command)

	// FIXME: decide here what to name it and signing and all that
	c.cmdSvs.Publish(command.Encode())
}
