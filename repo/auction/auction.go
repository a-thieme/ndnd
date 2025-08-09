package auction

import (
	"fmt"
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

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
		return a.bids[i].bid > a.bids[j].bid
	})
	for i := 0; i < numWinners; i++ {
		out += a.bids[i].node + " "
	}
	log.Info(a, "Determined winners", "winners", out)
	a.results = out
}

type AuctionEngine struct {
	mutex sync.RWMutex

	// ndn communication
	client         ndn.Client
	engine         ndn.Engine
	nodeNameN      enc.Name
	repoNameN      enc.Name
	availableNodes func() []enc.Name
	auctions       map[string]Auction
	interestCfg    ndn.InterestConfig
	calculateBid   func(string) int
	replications   int
	onWin          func(string)

	// auxiliary fields
	auctionPrefix enc.Name
}

func (a *AuctionEngine) String() string {
	return "auction-engine"
}

func NewAuctionEngine(nodeNameN enc.Name, repoNameN enc.Name, replications int, client ndn.Client, availableNodes func() []enc.Name, calculateBid func(string) int, onWin func(string)) *AuctionEngine {
	a := new(AuctionEngine)
	a.client = client
	a.engine = client.Engine()
	a.nodeNameN = nodeNameN
	a.repoNameN = repoNameN
	a.auctionPrefix = nodeNameN.Append(enc.NewGenericComponent("auction"))
	a.auctions = make(map[string]Auction)
	a.interestCfg = ndn.InterestConfig{
		MustBeFresh: true,
		Lifetime:    optional.Some(time.Second * 1),
	}
	a.availableNodes = availableNodes
	a.calculateBid = calculateBid
	a.replications = replications
	a.onWin = onWin
	return a
}

func (a *AuctionEngine) Start() error {
	log.Info(a, "Starting Repo Auction Engine")

	// Announce auction prefix
	a.client.AnnouncePrefix(ndn.Announcement{
		Name:   a.auctionPrefix,
		Expose: true,
	})

	// Auction engine interest handler
	if err := a.client.Engine().AttachHandler(a.auctionPrefix, a.onInterest); err != nil {
		return err
	}

	return nil
}

func (a *AuctionEngine) Stop() error {
	log.Info(a, "Stopping Repo Auction Engine")

	// Withdraw auction prefix
	a.client.WithdrawPrefix(a.auctionPrefix, nil)

	// Detach interest handler
	a.client.Engine().DetachHandler(a.auctionPrefix)

	return nil
}

func (a *AuctionEngine) addBid(itemId string, node enc.Name, bid int) {
	a.mutex.Lock()
	defer a.mutex.Unlock()

	a.auctions[itemId].bids[a.auctions[itemId].numBids] = Bid{node.String(), bid}
	// https://stackoverflow.com/questions/42605337/cannot-assign-to-struct-field-in-a-map
	if entry, ok := a.auctions[itemId]; ok {
		entry.numBids++
		if entry.numBids == entry.expectedBids {
			entry.determineWinners(a.replications)
		}
		a.auctions[itemId] = entry
	}
}

func (a *AuctionEngine) AuctionItem(itemId string) {
	// get list of node prefixes
	nodes := a.availableNodes()
	numNodes := len(nodes)
	a.auctions[itemId] = NewAuction(itemId, numNodes)
	// /<node>/<repo>/<itemID>/bid/<auctioneer>/<nonce>
	for _, node := range nodes {
		if node.Equal(a.nodeNameN) {
			a.addBid(itemId, node, a.calculateBid(itemId))
			continue
		}
		// set up Interest
		intCfg := a.interestCfg
		intCfg.Nonce = utils.ConvertNonce(a.engine.Timer().Nonce())

		// probably a better way to do this
		var n = node.String() + a.repoNameN.String() + "/" + itemId + "/bid/" + enc.Component{Typ: 8, Val: a.nodeNameN.Bytes()}.String() + "/" + a.auctions[itemId].nonce
		iName, _ := enc.NameFromStr(n)
		log.Info(a, "Sent bid interest", "itemId", itemId, "node", node, "nonce", a.auctions[itemId].nonce)
		object.ExpressR(a.engine, ndn.ExpressRArgs{
			Name:    iName,
			Retries: 5,
			Config:  &intCfg,
			Callback: func(args ndn.ExpressCallbackArgs) {
				switch args.Result {
				case ndn.InterestResultData:
					data := args.Data
					// dName := data.Name()
					log.Info(a, "Received bid", "itemId", itemId, "node", node, "nonce", a.auctions[itemId].nonce, "bid", string(data.Content().Join()))
					bid, _ := strconv.Atoi(string(data.Content().Join()))
					a.addBid(itemId, node, bid)
				case ndn.InterestCancelled:
					log.Info(a, "Interest cancelled", "itemId", itemId, "node", node, "nonce", a.auctions[itemId].nonce)
				case ndn.InterestResultNack:
					log.Info(a, "Received Nack", "itemId", itemId, "node", node, "nonce", a.auctions[itemId].nonce, "reason", args.NackReason)
				case ndn.InterestResultError:
					log.Info(a, "Received Error", "itemId", itemId, "node", node, "nonce", a.auctions[itemId].nonce)
				case ndn.InterestResultTimeout:
					log.Info(a, "Received Timeout", "itemId", itemId, "node", node, "nonce", a.auctions[itemId].nonce)
				default:
					log.Info(a, "Unhandled default case", "itemId", itemId, "node", node, "nonce", a.auctions[itemId].nonce, "result", args.Result)
				}
			},
		})
	}
}

func (a *AuctionEngine) fetchResults(auctioneer []byte, itemId string, nonce string) {
	auctioneerName, _ := enc.NameFromBytes(auctioneer)
	iName, _ := enc.NameFromStr(auctioneerName.String() + a.repoNameN.String() + "/" + itemId + "/results/" + nonce)
	log.Info(a, "Received results interest", "itemId", itemId, "auctioneer", auctioneerName, "nonce", nonce)
	intCfg := a.interestCfg
	intCfg.Nonce = utils.ConvertNonce(a.engine.Timer().Nonce())
	object.ExpressR(a.engine, ndn.ExpressRArgs{
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
				for _, winner := range winners {
					if a.nodeNameN.String() == winner {
						a.onWin(itemId)
					}
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
	log.Info(a, "Received bid interest", "name", n)
	tmp := n.At(-3).String()
	var content []byte
	if tmp == "bid" {
		itemId := n.At(-4).String()
		auctioneer := n.At(-2).Val
		nonce := n.At(-1).String()
		a.fetchResults(auctioneer, itemId, nonce)

		content = []byte(strconv.Itoa(a.calculateBid(itemId)))
	} else if n.At(-2).String() == "results" {
		itemId := n.At(-3).String()
		// todo: add nonce to itemID
		nonce := n.At(-1).String()
		if a.auctions[itemId].nonce != nonce {
			// todo: nack?
			// requested results were for a previous auction of the item, not the latest
			return
		}
		r := a.auctions[itemId].results
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
	data, err := a.engine.Spec().MakeData(
		n,
		&ndn.DataConfig{},
		enc.Wire{content},
		nil)
	if err != nil {
		log.Error(a, "Failed to make data", "name", n, "error", err)
		return
	}
	err = args.Reply(data.Wire)
	if err != nil {
		log.Error(a, "Failed to reply to interest", "name", n, "error", err)
		return
	}
}
