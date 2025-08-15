package management

import (
	"math"
	"math/rand"
	"strconv"
	"sync"
	"time"

	"github.com/named-data/ndnd/repo/tlv"
	"github.com/named-data/ndnd/repo/types"
	"github.com/named-data/ndnd/repo/utils"
	enc "github.com/named-data/ndnd/std/encoding"
	"github.com/named-data/ndnd/std/log"
	"github.com/named-data/ndnd/std/ndn"
	"github.com/named-data/ndnd/std/types/optional"
)

// NOTE: every handler registered by the management module will be ran in a separate goroutine, so blocking is not a concern

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
		if m.awareness.GetNumReplicas(partition) >= m.repo.NumReplicas {
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
	if m.awareness.GetNumReplicas(partition) <= m.repo.NumReplicas {
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
func (m *RepoManagement) NotifyReplicasHandler(command *tlv.RepoCommand) {
	partitionId := utils.PartitionIdFromEncName(command.SrcName.Name, m.repo.NumPartitions)
	replicas := m.awareness.GetPartitionReplicas(partitionId) // get relevant replicas
	log.Info(m, "Notifying replicas", "partitionId", partitionId, "replicas", replicas)

	for _, replica := range replicas {
		if replica.Equal(m.repo.NodeNameN) { // if local node is responsible for the partition
			log.Info(m, "Sending command to local replica", "replica", replica)
			m.storage.HandleCommand(command) // directly handles the command
			continue
		}

		notifyReplicaPrefix := replica.Append(m.repo.RepoNameN...).
			// Append(enc.NewGenericComponent(strconv.FormatUint(partitionId, 10))).
			Append(enc.NewGenericComponent("notify")).
			Append(enc.NewGenericComponent(strconv.FormatUint(command.Nonce, 10)))

		log.Info(m, "Sending command to replica", "replica", replica, "notifyReplicaPrefix", notifyReplicaPrefix)
		m.repo.Client.ExpressR(ndn.ExpressRArgs{
			Name: notifyReplicaPrefix,
			Config: &ndn.InterestConfig{
				CanBePrefix: false,
				MustBeFresh: true,
			},
			Retries:  2, // TODO: configurable
			AppParam: command.Encode(),
			Callback: func(args ndn.ExpressCallbackArgs) {
				switch args.Result {
				// TODO: more granular error handling
				default:
					// log.Info(m, "Replica notified", "replica", replica)
				}
			},
		})
	}
}

// TODO: handle node-level command interest
func (m *RepoManagement) ProcessCommandHandler(command *tlv.RepoCommand) {
	partitionId := utils.PartitionIdFromEncName(command.SrcName.Name, m.repo.NumPartitions)

	// don't handle command if we are not responsible for it
	// TODO: should this be handled by the storage module?
	if !m.awareness.OwnsPartition(partitionId) {
		return
	}

	// handle the command
	m.storage.HandleCommand(command)
}

// Launches a job to fetch a data from the network
func (m *RepoManagement) FetchDataHandler(dataNameN enc.Name) {
	partitionId := utils.PartitionIdFromEncName(dataNameN, m.repo.NumPartitions)

	if !m.awareness.OwnsPartition(partitionId) {
		return
	}

	log.Info(m, "Fetching data", "name", dataNameN)

	retries := -1 // -1 means infinite retries - the job is considered successful only if the relevant data is received
	waitPeriod := 500 * time.Millisecond
	maxBackoff := 30 * time.Second
	randomness := 5.0 // TODO: make this configurable, factory?

	ch := make(chan ndn.ConsumeState, 1)

	for retries != 0 {
		// This line checks if the data object is in store, not any segments
		if wire, _ := m.repo.Store.Get(dataNameN, false); wire != nil {
			break // early-termination if data is already in the store
			// TODO: this is not very efficient - we should have a method to check if a data object is in store
		}

		// Fetch data blob
		m.repo.Client.Consume(dataNameN, func(status ndn.ConsumeState) {
			ch <- status
		})

		status := <-ch
		m.repo.Store.RemovePrefix(status.Name())                 // remove all fetched segments
		m.repo.Store.Put(status.Name(), status.Content().Join()) // put the full data object in instead

		if status.Error() != nil {
			log.Warn(m, "BlobFetch error, retrying", "err", status.Error(), "name", dataNameN)
			time.Sleep(min(waitPeriod, maxBackoff) + time.Duration(rand.Float64()*randomness*float64(time.Second)))
			waitPeriod *= 2
			retries--
		} else {
			log.Info(m, "Data fetched", "name", dataNameN)
			break // early-termination if data is received
			// we don't need to specifically handle the content since there is an engine hook to store all received data
		}
	}

	// TODO: the assumption here is that command can be committed to the group state before the data is available, hence we keep retrying to fetch the relevant data. This, however, cause unnecessary traffics. Also, if the producer, for any reason, send the command again, we need to fetch the data again immediately, so the handler's id should not be tied to the data name.
}

// Launches a coordination job to check the status from responsible node
func (m *RepoManagement) ExternalStatusRequestHandler(interestHandler *ndn.InterestHandlerArgs, statusRequest *tlv.RepoStatus) {
	resourceNameN := statusRequest.Name.Name
	partitionId := utils.PartitionIdFromEncName(resourceNameN, m.repo.NumPartitions)

	replicas := m.awareness.GetPartitionReplicas(partitionId) // get relevant replicas
	nonlocalReplicas := len(replicas)
	log.Info(m, "Requesting status from replicas", "partitionId", partitionId, "replicas", replicas)

	appParam := statusRequest.Encode()

	// Create a channel for each replica to handle responses
	responseCh := make(chan ndn.ExpressCallbackArgs, len(replicas))

	// Prepare reply
	reply := tlv.RepoStatusReply{
		Name: statusRequest.Name,
	}
	defer func() {
		interestHandler.Reply(reply.Encode())
	}()

	// Track responses
	quorumRatio := 0.5
	quorum := int(math.Ceil(quorumRatio * float64(m.repo.NumReplicas))) // TODO: make this configurable, and it should be a percentage (with rounding) of the replication factor

	// Counter for success
	successes := 0

	// Wait group to coordinate job finish
	var wg sync.WaitGroup
	wg.Add(nonlocalReplicas)

	// Send requests to remote replicas
	for _, replica := range replicas {
		if replica.Equal(m.repo.NodeNameN) {
			// TODO: check local status
			if m.storage.OwnsResource(resourceNameN) {
				log.Info(m, "Local status request successful", "name", resourceNameN)
				successes++
			} else {
				log.Info(m, "Local status request failed: no content", "name", resourceNameN)
			}
			wg.Done()
			continue
		}

		statusRequestReplicaPrefix := replica.Append(m.repo.RepoNameN...).
			// Append(enc.NewGenericComponent(strconv.FormatUint(partitionId, 10))).
			Append(enc.NewGenericComponent("status")).
			Append(enc.NewGenericComponent(strconv.FormatUint(statusRequest.Nonce, 10)))

		log.Info(m, "Sending status request to replica", "replica", replica, "statusRequestReplicaPrefix", statusRequestReplicaPrefix)
		m.repo.Client.ExpressR(ndn.ExpressRArgs{
			Name: statusRequestReplicaPrefix,
			Config: &ndn.InterestConfig{
				CanBePrefix: false,
				MustBeFresh: true,
				Lifetime:    optional.Some(5 * time.Second),
			},
			Retries:  0, // No retries for status requests
			AppParam: appParam,
			Callback: func(args ndn.ExpressCallbackArgs) {
				responseCh <- args // shouldn't block since responseCh is sized properly
				wg.Done()
			},
		})
	}

	// Close results when all goroutines are finished
	go func() {
		wg.Wait()
		close(responseCh)
		log.Info(m, "Status request coordination completed", "target", resourceNameN, "quorum", quorum)
	}()

	// Collect results
	// TODO: currently we only return success / unsuccessful. We can do more complicated parsings here
	// before that, we need to carefully separate inner-Repo and outer-Repo communication
	for r := range responseCh {
		log.Info(m, "Status request response received", "result", r.Result)
		switch r.Result {
		case ndn.InterestResultData:
			replicaStatus, _ := tlv.ParseRepoStatusReply(enc.NewWireView(r.Data.Content()), false)
			if replicaStatus.Status == types.ReplyStatusSuccess {
				successes++
			}
		default:
			// pass
		}
	}

	// Check final result
	if successes >= quorum {
		reply.Status = types.ReplyStatusSuccess
		log.Info(m, "Status request completed successfully", "successes", successes, "quorum", quorum, "status-code", reply.Status)
	} else {
		reply.Status = types.ReplyStatusInProgress
		log.Error(m, "Status request failed - insufficient quorum", "successes", successes, "quorum", quorum, "status-code", reply.Status)
	}

	// TODO: reply to the original interest
	data, _ := m.repo.Engine.Spec().MakeData(
		interestHandler.Interest.Name(),
		&ndn.DataConfig{},
		reply.Encode(),
		nil, // TODO: security
	)

	interestHandler.Reply(data.Wire)
}

func (m *RepoManagement) InternalStatusRequestHandler(interestHandler *ndn.InterestHandlerArgs, status *tlv.RepoStatus) {
	reply := m.storage.HandleStatus(status)
	data, _ := m.repo.Engine.Spec().MakeData(
		interestHandler.Interest.Name(),
		&ndn.DataConfig{},
		reply.Encode(),
		nil, // TODO: security
	)

	interestHandler.Reply(data.Wire)
}
