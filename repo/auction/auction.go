package auction

import (
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/named-data/ndnd/std/encoding"
	"github.com/named-data/ndnd/std/engine"
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
	fmt.Println("winners:", out)
	a.results = out
}

type AuctionEngine struct {
	mutex sync.RWMutex

	// ndn communication
	engine         ndn.Engine
	myPrefix       encoding.Name
	repoPrefix     encoding.Name
	availableNodes func() []encoding.Name
	auctions       map[string]Auction
	interestCfg    ndn.InterestConfig
	calculateBid   func(string) int
	replications   int
	onWin          func(string)
}

func NewAuctionEngine(myPrefix encoding.Name, repoPrefix encoding.Name, availableNodes func() []encoding.Name, calculateBid func(string) int, app ndn.Engine, replications int, onWin func(string)) *AuctionEngine {
	a := new(AuctionEngine)
	if app == nil {
		a.engine = engine.NewBasicEngine(engine.NewDefaultFace())
	} else {
		a.engine = app
	}
	a.myPrefix = myPrefix
	a.auctions = make(map[string]Auction)
	a.availableNodes = availableNodes
	a.interestCfg = ndn.InterestConfig{
		MustBeFresh: true,
		Lifetime:    optional.Some(time.Second * 1),
	}
	a.calculateBid = calculateBid
	a.repoPrefix = repoPrefix
	a.replications = replications
	a.onWin = onWin
	return a
}

func (a *AuctionEngine) Start() error {
	return a.engine.Start()
}

func (a *AuctionEngine) Stop() error {
	return a.engine.Stop()
}

func (a *AuctionEngine) addBid(itemId string, node encoding.Name, bid int) {
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
		if node.Equal(a.myPrefix) {
			a.addBid(itemId, node, a.calculateBid(itemId))
			continue
		}
		// set up Interest
		intCfg := a.interestCfg
		intCfg.Nonce = utils.ConvertNonce(a.engine.Timer().Nonce())

		// probably a better way to do this
		var n = node.String() + a.repoPrefix.String() + "/" + itemId + "/bid/" + encoding.Component{Typ: 8, Val: a.myPrefix.Bytes()}.String() + "/" + a.auctions[itemId].nonce
		iName, _ := encoding.NameFromStr(n)
		fmt.Println("interest: ", iName)
		object.ExpressR(a.engine, ndn.ExpressRArgs{
			Name:    iName,
			Retries: 5,
			Config:  &intCfg,
			Callback: func(args ndn.ExpressCallbackArgs) {
				switch args.Result {
				case ndn.InterestResultData:
					data := args.Data
					dName := data.Name()
					fmt.Printf("Received Data Name: %s\n", dName.String())
					bid, _ := strconv.Atoi(string(data.Content().Join()))
					a.addBid(itemId, node, bid)
				case ndn.InterestCancelled:
					fmt.Println("Cancelled")
				case ndn.InterestResultNack:
					fmt.Printf("Received Nack with reason=%d\n", args.NackReason)
				case ndn.InterestResultError:
					fmt.Println("Received Error")
				case ndn.InterestResultTimeout:
					fmt.Printf("Received Timeout for name %s\n", node.String())
				default:
					fmt.Println("unhandled default case for ", args.Result)
				}
			},
		})
	}
}

func (a *AuctionEngine) fetchResults(auctioneer []byte, itemId string, nonce string) {
	auctioneerName, _ := encoding.NameFromBytes(auctioneer)
	iName, _ := encoding.NameFromStr(auctioneerName.String() + a.repoPrefix.String() + "/" + itemId + "/results/" + nonce)
	fmt.Println("results interest: ", iName)
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
				dName := data.Name()
				fmt.Printf("Received Data Name: %s\n", dName.String())
				winners := strings.Fields(string(data.Content().Join()))
				fmt.Println("winners: ", winners)
				for _, winner := range winners {
					if a.myPrefix.String() == winner {
						a.onWin(itemId)
					}
				}
			case ndn.InterestCancelled:
				fmt.Println("Cancelled")
			case ndn.InterestResultNack:
				fmt.Printf("Received Nack with reason=%d\n", args.NackReason)
			case ndn.InterestResultError:
				fmt.Println("Received Error")
			case ndn.InterestResultTimeout:
				fmt.Printf("Received Timeout for name %s\n", iName.String())
			default:
				fmt.Println("unhandled default case for ", args.Result)
			}
		},
	})
}

func (a *AuctionEngine) onInterest(args ndn.InterestHandlerArgs) {
	interest := args.Interest
	n := interest.Name()
	fmt.Println("got interest for name: ", n)
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
		fmt.Println("not results or bid")
		content = []byte("unknown")
	}
	data, err := a.engine.Spec().MakeData(
		n,
		&ndn.DataConfig{},
		encoding.Wire{content},
		nil)
	if err != nil {
		fmt.Println(err)
		return
	}
	err = args.Reply(data.Wire)
	if err != nil {
		fmt.Println(err)
		return
	}
}

func getOnline() []encoding.Name {
	out := make([]encoding.Name, 2)
	var comp = encoding.Component{}
	for i := 0; i < 2; i++ {
		comp, _ = encoding.ComponentFromStr("prefix")
		out[i] = out[i].Append(comp)
		comp, _ = encoding.ComponentFromStr("node" + strconv.Itoa(i+1))
		out[i] = out[i].Append(comp)
	}
	return out
}

func doBid(itemID string) int {
	b := rand.Int() % 256
	fmt.Println(itemID, "bid: ", b)
	return b
}

func onWin(itemID string) {
	fmt.Println("I won", itemID)
}

func main() {
	fmt.Println(getOnline())
	prefix, err := encoding.NameFromStr(os.Args[1])
	if err != nil {
		fmt.Println(err)
		return
	}
	itemID := os.Args[2]
	repo, err := encoding.NameFromStr("<repo>")
	if err != nil {
		fmt.Println(err)
		return
	}

	a := NewAuctionEngine(prefix, repo, getOnline, doBid, nil, 3, onWin)
	defer a.Stop()
	if a.Start() != nil {
		fmt.Println("Error starting:", a.Start())
		return
	}
	if a.engine.AttachHandler(prefix, a.onInterest) != nil {
		fmt.Println("Error attaching handler")
		return
	}
	if a.engine.RegisterRoute(prefix) != nil {
		fmt.Println("Error registering route:", prefix)
		return
	}
	time.Sleep(2 * time.Second)

	a.AuctionItem(itemID)
	time.Sleep(3 * time.Second)
	fmt.Println(a.auctions)

	// do an additional auction if you want
	//a.AuctionItem(itemID)
	//time.Sleep(3 * time.Second)
	//fmt.Println(a.auctions)

	fmt.Println("Finished")
}
