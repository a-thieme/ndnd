package management

import (
	"math/rand"
	"strconv"
	"time"

	// "github.com/named-data/ndnd/repo/tlv"
	// "github.com/named-data/ndnd/repo/utils"
	// enc "github.com/named-data/ndnd/std/encoding"
	"github.com/named-data/ndnd/std/log"
)

// TODO: should rely on external configuration
const (
	NumPartitions = 32
	NumReplicas   = 1
)

// TODO: we can make these strategy pattern, if desired in the future
// under-replication handler, retries with exponential backoff
// blocking, should be ran in a separate goroutine
func (m *RepoManagement) UnderReplicationHandler(partition uint64) {
	log.Info(m, "Under-replicated partition, handling after a random delay", "id", partition)
	item := strconv.FormatUint(partition, 10)

	retries := -1 // -1 means infinite retries - the job is considered successful only if the partition is no longer under-replicated
	waitPeriod := 5 * time.Second
	maxBackoff := 20 * time.Second
	randomness := 2.0 // TODO: make this configurable, factory?

	time.Sleep(time.Duration(rand.Float64() * randomness))
	for retries != 0 {
		if m.awareness.GetNumReplicas(partition) >= NumReplicas {
			break // early-termination if requirements met
		}

		log.Info(m, "Auctioning under-replicated partition", "id", partition)
		m.auction.AuctionItem(item)
		time.Sleep(min(waitPeriod, maxBackoff) + time.Duration(rand.Float64()*randomness))
		waitPeriod *= 2
		retries--
	}
}

// blocking, should be ran in a separate goroutine
func (m *RepoManagement) OverReplicationHandler(partition uint64) {
	log.Info(m, "Over-replicated partition, handling after a random delay", "id", partition)
	randomness := 10.0 // TODO: make this configurable
	time.Sleep(time.Duration(rand.Float64() * randomness))

	// TODO: check if the partition is still over-replicated
	if m.awareness.GetNumReplicas(partition) <= NumReplicas {
		return
	}

	// Drop partition if it exists
	log.Info(m, "Dropping over-replicated partition", "id", partition)
	m.awareness.DropLocalPartition(partition)
	m.storage.UnregisterPartition(partition)
}

// non-blocking, but should be ran in a separate goroutine
func (m *RepoManagement) WonAuctionHandler(item string) {
	partitionId, _ := strconv.ParseUint(item, 10, 64)

	err := m.storage.RegisterPartition(partitionId)
	if err != nil {
		log.Error(m, "Failed to commit partition registration on auction win", "id", partitionId, "err", err)
		return
	}

	log.Info(m, "Won partition", "id", partitionId)
	m.awareness.AddLocalPartition(partitionId)
}

// Producer message handlers
// TODO: handle repo command interest
// func (m *RepoManagement) NotifyReplicasHandler(command *tlv.RepoCommand) {
// 	partitionId := utils.PartitionIdFromEncName(command.CommandName.Name, NumPartitions)
// 	replicas := m.awareness.GetReplicas(partitionId) // get relevant replicas

// 	notifyReplicaPrefix := []enc.Name{} // TODO: construct notify name
// 	for prefix := range notifyReplicaPrefix {
// 		m.client.ExpressR()
// 	}
// }
