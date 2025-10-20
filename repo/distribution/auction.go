// Credits to Adam Thieme for the original implementation

package distribution

import (
	"fmt"
	"hash/fnv"
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
	needed       int
	bids         []Bid
	numBids      int
	results      string
	bidders      []string
	startTime    time.Time
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
	a.bidders = make([]string, size)
	a.itemID = itemID
	a.nonce = strconv.Itoa(rand.Int())
	a.numBids = 0
	a.expectedBids = size
	a.results = ""
	a.startTime = time.Now()
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
	mutex sync.Mutex

	// ndn communication
	repo           *types.RepoShared
	availableNodes func() []string
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

func NewAuctionEngine(repo *types.RepoShared, availableNodes func() []string, calculateBid func(string) int, onWin func(string)) *AuctionEngine {
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
	a.mutex.Lock()
	defer a.mutex.Unlock()

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

func (a *AuctionEngine) addBid(itemId string, node string, bid int) {
	a.mutex.Lock()
	log.Info(a, "addbid", "item", itemId, "node", node, "bid", bid)
	if slices.Contains(a.auctions[itemId].bidders, node) {
		log.Info(a, "already got bid, skipping")
		a.mutex.Unlock()
		return
	}
	a.auctions[itemId].bidders[a.auctions[itemId].numBids] = node
	a.auctions[itemId].bids[a.auctions[itemId].numBids] = Bid{node, bid}
	iWin := false
	// https://stackoverflow.com/questions/42605337/cannot-assign-to-struct-field-in-a-map
	if entry, ok := a.auctions[itemId]; ok {
		log.Info(a, "Adding bid", "itemId", itemId, "node", node, "bid", bid)
		entry.numBids++
		if entry.numBids == entry.expectedBids {
			entry.determineWinners(a.repo.NumReplicas)
			// if we are a winner, notify the management module
			if slices.Contains(strings.Fields(entry.results), a.repo.NodeNameN.String()) {
				iWin = true
			}
		}
		a.auctions[itemId] = entry
	}
	a.mutex.Unlock()
	if iWin {
		a.onWin(itemId)
	}
}

func (a *AuctionEngine) AuctionItem(itemId string) {
	// get list of node prefixes
	nodes := a.availableNodes()
	nodes = append(nodes, a.repo.NodeNameN.String())
	sort.Strings(nodes)
	numNodes := len(nodes)
	if nodes[HashAndMod(itemId, numNodes)] != a.repo.NodeNameN.String() {
		return
	}

	// Protect the map write with mutex
	a.mutex.Lock()

	// Check if an auction for this item already exists and is recent
	// if existingAuction, ok := a.auctions[itemId]; ok {
	// 	if time.Since(existingAuction.startTime) < (5 * time.Second) {
	// 		// It's too soon to start a new auction for this item
	// 		log.Info(a, "Skipping new auction, existing one is too recent", "item", itemId, "created", existingAuction.startTime)
	// 		a.mutex.Unlock() // Don't forget to unlock before returning
	// 		return
	// 	}
	// }

	log.Info(a, "auctioning", "item", itemId)
	a.auctions[itemId] = NewAuction(itemId, numNodes)
	nonce := a.auctions[itemId].nonce
	a.mutex.Unlock()
	// /<node>/<repo>/<itemID>/bid/<auctioneer>/<nonce>
	for _, node := range nodes {
		if node == a.repo.NodeNameN.String() {
			a.addBid(itemId, node, a.calculateBid(itemId))
			continue
		}
		// set up Interest
		intCfg := a.interestCfg
		intCfg.Nonce = utils.ConvertNonce(a.repo.Client.Engine().Timer().Nonce())

		// probably a better way to do this
		nme, _ := enc.NameFromStr(itemId)
		var n = node + a.repo.RepoNameN.String() + "/" + enc.Component{Typ: 8, Val: nme.Bytes()}.String() + "/bid/" + enc.Component{
			Typ: 8,
			Val: a.repo.NodeNameN.Bytes()}.String() + "/" + nonce

		iName, _ := enc.NameFromStr(n)

		log.Info(a, "Sent bid interest", "itemId", itemId, "node", node, "nonce", nonce)
		object.ExpressR(a.repo.Engine, ndn.ExpressRArgs{
			Name:    iName,
			Retries: 0,
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

func HashAndMod(s string, m int) int {
	// A divisor of 0 is not allowed for a modulo operation.
	if m == 0 {
		panic("error: divisor cannot be zero")
	}

	// Create a new FNV-1a 32-bit hash object.
	// FNV (Fowler-Noll-Vo) is a fast, non-cryptographic hash function.
	h := fnv.New32a()

	// Write the string's byte representation to the hash object.
	// The Write method never returns an error.
	_, _ = h.Write([]byte(s))

	// Sum32 returns the 32-bit hash value as a uint32.
	hashValue := h.Sum32()

	// Convert the uint32 hash value to an int and perform the modulo operation.
	// We take the absolute value of m to handle potential negative divisors,
	// ensuring the result is always non-negative.
	divisor := m
	if divisor < 0 {
		divisor = -divisor
	}

	result := int(hashValue) % divisor

	return result
}

func (a *AuctionEngine) fetchResults(auctioneer []byte, itemId string, nonce string) {
	auctioneerName, _ := enc.NameFromBytes(auctioneer)
	tmp, _ := enc.NameFromStr(itemId)
	iName, _ := enc.NameFromStr(auctioneerName.String() + a.repo.RepoNameN.String() + "/" + enc.Component{Typ: 8, Val: tmp.Bytes()}.String() + "/results/" + nonce)
	log.Info(a, "fetching results", "name", iName, "itemId", itemId, "auctioneer", auctioneerName, "nonce", nonce)
	intCfg := a.interestCfg
	intCfg.Nonce = utils.ConvertNonce(a.repo.Engine.Timer().Nonce())
	object.ExpressR(a.repo.Engine, ndn.ExpressRArgs{
		Name:    iName,
		Retries: 15,
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
	log.Info(a, "got interest", "name", n)
	tmp := n.At(-3).String()
	var content []byte
	if tmp == "bid" {
		tmp := n.At(-4).Val
		name, _ := enc.NameFromBytes(tmp)
		itemId := name.String()
		auctioneer := n.At(-2).Val
		log.Info(a, "Received bid interest", "name", n, "auctioneer", auctioneer)
		nonce := n.At(-1).String()
		a.fetchResults(auctioneer, itemId, nonce)

		content = []byte(strconv.Itoa(a.calculateBid(itemId)))
	} else if n.At(-2).String() == "results" {
		log.Info(a, "Received auction results interest", "name", n)

		tmp := n.At(-3).Val
		anothertmp, _ := enc.NameFromBytes(tmp)
		itemId := anothertmp.String()
		log.Info(a, "asked results for", "item", itemId)

		// todo: add nonce to itemID
		nonce := n.At(-1).String()

		// Protect map access with mutex
		log.Info(a, "locking")
		a.mutex.Lock()
		cnon := a.auctions[itemId].nonce
		log.Info(a, "unlocking")
		a.mutex.Unlock()
		log.Info(a, "unlocked")
		if cnon != nonce {
			log.Info(a, "nonces don't match", a.auctions[itemId].nonce, nonce)
			// todo: nack?
			// requested results were for a previous auction of the item, not the latest
			return
		}
		a.mutex.Lock()
		r := a.auctions[itemId].results
		a.mutex.Unlock()
		// FIXME: while r is unchanged, wait a small amount of time
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
