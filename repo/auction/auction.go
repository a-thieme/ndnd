// Credits to Adam Thieme for the original implementation

package auction

import (
	"fmt"
	"math/rand"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/named-data/ndnd/repo/types"
	enc "github.com/named-data/ndnd/std/encoding"
	"github.com/named-data/ndnd/std/log"
	"github.com/named-data/ndnd/std/ndn"
	"github.com/named-data/ndnd/std/object"
	"github.com/named-data/ndnd/std/types/optional"
	"github.com/named-data/ndnd/std/utils"
)

type Auction struct {
	itemID       string
	nonce        string
	expectedBids int
	bids         []Bid
	numBids      int
	results      string
}
type Bid struct {
	node string
	bid  int
}

func (a *Auction) String() string {
	return fmt.Sprintf("auction (itemID=%s, nonce=%s)", a.itemID, a.nonce)
}

func NewAuction(itemID string, size int) Auction {
	a := new(Auction)
	a.bids = make([]Bid, size)
	a.itemID = itemID
	a.nonce = strconv.Itoa(rand.Int())
	a.numBids = 0
	a.expectedBids = size
	a.results = ""
	return *a
}

func (a *Auction) determineWinners(numWinners int) {
	var out = ""
	sort.Slice(a.bids, func(i, j int) bool {
		return a.bids[i].bid > a.bids[j].bid || (a.bids[i].bid == a.bids[j].bid && a.bids[i].node > a.bids[j].node) // Just so we have determinisitc ordering when the bids are the same
	})

	for i := 0; i < numWinners && i < a.numBids; i++ {
		out += a.bids[i].node + " "
	}
	log.Info(a, "Determined winners", "winners", out)
	a.results = out
}

type AuctionEngine struct {
	mutex sync.RWMutex

	// ndn communication
	repo           *types.RepoShared
	availableNodes func() []enc.Name
	auctions       map[string]Auction
	interestCfg    ndn.InterestConfig
	calculateBid   func(string) int
	onWin          func(string)

	// auxiliary fields
	auctionPrefix enc.Name
	bidPrefix     enc.Name
	resultsPrefix enc.Name
}

func (a *AuctionEngine) String() string {
	return "auction-engine"
}

func NewAuctionEngine(repo *types.RepoShared, availableNodes func() []enc.Name, calculateBid func(string) int, onWin func(string)) *AuctionEngine {
	a := new(AuctionEngine)
	a.repo = repo
	a.auctions = make(map[string]Auction)
	a.interestCfg = ndn.InterestConfig{
		MustBeFresh: true,
		Lifetime:    optional.Some(time.Second * 1),
	}
	a.availableNodes = availableNodes
	a.calculateBid = calculateBid
	a.onWin = onWin

	a.auctionPrefix = repo.NodeNameN.Append(repo.RepoNameN...)
	a.bidPrefix = a.auctionPrefix.Append(enc.NewGenericComponent("bid"))
	a.resultsPrefix = a.auctionPrefix.Append(enc.NewGenericComponent("results"))

	return a
}

func (a *AuctionEngine) Start() error {
	log.Info(a, "Starting Repo Auction Engine")

	// Announce auction prefix
	for _, prefix := range []enc.Name{a.auctionPrefix} {
		a.repo.Client.AnnouncePrefix(ndn.Announcement{
			Name:   prefix,
			Expose: true,
		})
	}

	// Auction engine interest handler
	if err := a.repo.Client.Engine().AttachHandler(a.auctionPrefix, a.onInterest); err != nil {
		return err
	}
	// if err := a.client.Engine().AttachHandler(a.bidPrefix, a.onBidInterest); err != nil {
	// 	return err
	// }
	// if err := a.client.Engine().AttachHandler(a.resultsPrefix, a.onAuctionResultsInterest); err != nil {
	// 	return err
	// }

	return nil
}

func (a *AuctionEngine) Stop() error {
	a.mutex.RLock()
	defer a.mutex.RUnlock()

	log.Info(a, "Stopping Repo Auction Engine")

	// Withdraw auction prefix
	for _, prefix := range []enc.Name{a.auctionPrefix} {
		a.repo.Client.WithdrawPrefix(prefix, nil)
	}

	// Detach interest handler
	if err := a.repo.Client.Engine().DetachHandler(a.auctionPrefix); err != nil {
		return err
	}
	// a.client.Engine().DetachHandler(a.bidPrefix)
	// a.client.Engine().DetachHandler(a.resultsPrefix)

	return nil
}

func (a *AuctionEngine) addBid(itemId string, node enc.Name, bid int) {
	a.mutex.Lock()
	defer a.mutex.Unlock()

	a.auctions[itemId].bids[a.auctions[itemId].numBids] = Bid{node.String(), bid}
	// https://stackoverflow.com/questions/42605337/cannot-assign-to-struct-field-in-a-map
	if entry, ok := a.auctions[itemId]; ok {
		log.Info(a, "Adding bid", "itemId", itemId, "node", node, "bid", bid)
		entry.numBids++
		if entry.numBids == entry.expectedBids {
			entry.determineWinners(a.repo.NumReplicas)

			// if we are a winner, notify the management module
			if slices.Contains(strings.Fields(entry.results), a.repo.NodeNameN.String()) {
				a.onWin(itemId)
			}
		}
		a.auctions[itemId] = entry
	}
}

func (a *AuctionEngine) AuctionItem(itemId string) {
	// get list of node prefixes
	nodes := a.availableNodes()
	numNodes := len(nodes)

	// Protect the map write with mutex
	a.mutex.Lock()
	a.auctions[itemId] = NewAuction(itemId, numNodes)
	nonce := a.auctions[itemId].nonce
	a.mutex.Unlock()
	// /<node>/<repo>/<itemID>/bid/<auctioneer>/<nonce>
	for _, node := range nodes {
		if node.Equal(a.repo.NodeNameN) {
			a.addBid(itemId, node, a.calculateBid(itemId))
			continue
		}
		// set up Interest
		intCfg := a.interestCfg
		intCfg.Nonce = utils.ConvertNonce(a.repo.Client.Engine().Timer().Nonce())

		// probably a better way to do this
		var n = node.String() + a.repo.RepoNameN.String() + "/" + itemId + "/bid/" + enc.Component{Typ: 8, Val: a.repo.NodeNameN.Bytes()}.String() + "/" + nonce
		iName, _ := enc.NameFromStr(n)

		log.Info(a, "Sent bid interest", "itemId", itemId, "node", node, "nonce", nonce)
		object.ExpressR(a.repo.Engine, ndn.ExpressRArgs{
			Name:    iName,
			Retries: 5,
			Config:  &intCfg,
			Callback: func(args ndn.ExpressCallbackArgs) {
				switch args.Result {
				case ndn.InterestResultData:
					data := args.Data
					// dName := data.Name()
					log.Info(a, "Received bid", "itemId", itemId, "node", node, "nonce", nonce, "bid", string(data.Content().Join()))
					bid, _ := strconv.Atoi(string(data.Content().Join()))
					a.addBid(itemId, node, bid)
				case ndn.InterestCancelled:
					log.Info(a, "Interest cancelled", "itemId", itemId, "node", node, "nonce", nonce)
				case ndn.InterestResultNack:
					log.Info(a, "Received Nack", "itemId", itemId, "node", node, "nonce", nonce, "reason", args.NackReason)
				case ndn.InterestResultError:
					log.Info(a, "Received Error", "itemId", itemId, "node", node, "nonce", nonce)
				case ndn.InterestResultTimeout:
					log.Info(a, "Received Timeout", "itemId", itemId, "node", node, "nonce", nonce)
				default:
					log.Info(a, "Unhandled default case", "itemId", itemId, "node", node, "nonce", nonce, "result", args.Result)
				}
			},
		})
	}
}

func (a *AuctionEngine) fetchResults(auctioneer []byte, itemId string, nonce string) {
	auctioneerName, _ := enc.NameFromBytes(auctioneer)
	iName, _ := enc.NameFromStr(auctioneerName.String() + a.repo.RepoNameN.String() + "/" + itemId + "/results/" + nonce)
	log.Info(a, "Received results interest", "itemId", itemId, "auctioneer", auctioneerName, "nonce", nonce)
	intCfg := a.interestCfg
	intCfg.Nonce = utils.ConvertNonce(a.repo.Engine.Timer().Nonce())
	object.ExpressR(a.repo.Engine, ndn.ExpressRArgs{
		Name:    iName,
		Retries: 5,
		Config:  &intCfg,
		Callback: func(args ndn.ExpressCallbackArgs) {
			switch args.Result {
			case ndn.InterestResultData:
				data := args.Data
				// dName := data.Name()
				winners := strings.Fields(string(data.Content().Join()))
				log.Info(a, "Fetched winners", "itemId", itemId, "nonce", nonce, "auctioneer", auctioneerName, "winners", winners)
				if slices.Contains(winners, a.repo.NodeNameN.String()) {
					a.onWin(itemId)
				}
			case ndn.InterestCancelled:
				log.Info(a, "Interest cancelled", "itemId", itemId, "auctioneer", auctioneerName, "nonce", nonce)
			case ndn.InterestResultNack:
				log.Info(a, "Received Nack", "itemId", itemId, "auctioneer", auctioneerName, "nonce", nonce, "reason", args.NackReason)
			case ndn.InterestResultError:
				log.Info(a, "Received Error", "itemId", itemId, "auctioneer", auctioneerName, "nonce", nonce)
			case ndn.InterestResultTimeout:
				log.Info(a, "Received Timeout", "itemId", itemId, "auctioneer", auctioneerName, "nonce", nonce)
			default:
				log.Info(a, "Unhandled default case", "itemId", itemId, "auctioneer", auctioneerName, "nonce", nonce, "result", args.Result)
			}
		},
	})
}

func (a *AuctionEngine) onInterest(args ndn.InterestHandlerArgs) {
	interest := args.Interest
	n := interest.Name()
	tmp := n.At(-3).String()
	var content []byte
	if tmp == "bid" {
		log.Info(a, "Received bid interest", "name", n)

		itemId := n.At(-4).String()
		auctioneer := n.At(-2).Val
		nonce := n.At(-1).String()
		a.fetchResults(auctioneer, itemId, nonce)

		content = []byte(strconv.Itoa(a.calculateBid(itemId)))
	} else if n.At(-2).String() == "results" {
		log.Info(a, "Received auction results interest", "name", n)

		itemId := n.At(-3).String()
		// todo: add nonce to itemID
		nonce := n.At(-1).String()

		// Protect map access with mutex
		a.mutex.RLock()
		if a.auctions[itemId].nonce != nonce {
			// todo: nack?
			// requested results were for a previous auction of the item, not the latest
			a.mutex.RUnlock()
			return
		}
		r := a.auctions[itemId].results
		a.mutex.RUnlock()
		// todo: wait until < interest timeout and keep checking if r is changed
		// r should be changed after an auction timeout to something like "unsuccessful auction"
		if r == "" {
			// don't respond until there's a result
			return
		}
		content = []byte(r)
	} else {
		log.Info(a, "Received unknown interest", "name", n)
		content = []byte("unknown")
	}

	// Create data packet
	data, err := a.repo.Engine.Spec().MakeData(
		n,
		&ndn.DataConfig{},
		enc.Wire{content},
		a.repo.Client.SuggestSigner(n),
	)
	if err != nil {
		log.Error(a, "Failed to make data", "name", n, "error", err)
		return
	}
	err = args.Reply(data.Wire)
	if err != nil {
		log.Error(a, "Failed to reply to interest", "name", n, "error", err)
		return
	}

	log.Info(a, "Replied to bid interest", "name", n, "content", string(content))
}

func (a *AuctionEngine) SetOnAuctionWin(onWin func(string)) {
	a.onWin = onWin
}
