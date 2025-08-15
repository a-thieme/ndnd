package producerfacing

import (
	"github.com/named-data/ndnd/repo/tlv"
	enc "github.com/named-data/ndnd/std/encoding"
	"github.com/named-data/ndnd/std/log"
	"github.com/named-data/ndnd/std/ndn"
)

// onExternalStatusRequest processes an incoming status check interest
func (p *RepoProducerFacing) onExternalStatusRequest(args ndn.InterestHandlerArgs) {
	interest := args.Interest

	if interest.AppParam() == nil {
		log.Warn(p, "Status interest has no app apram, ignoring")
		return
	}

	statusRequest, err := tlv.ParseRepoStatus(enc.NewWireView(interest.AppParam()), true)

	if err != nil {
		log.Warn(p, "Failed to parse status request", "err", err)
		return
	}

	resourceNameN := statusRequest.Name // could be data object, or sync groups
	log.Info(p, "Received status request", "resourceName", resourceNameN)

	p.externalStatusRequestHandler(&args, statusRequest)
}

// onInternalStatusRequest is called when an internal status request interest is received
func (p *RepoProducerFacing) onInternalStatusRequest(args ndn.InterestHandlerArgs) {
	interest := args.Interest

	if interest.AppParam() == nil {
		log.Warn(p, "Status request interest has no app param, ignoring")
		return
	}

	statusRequest, err := tlv.ParseRepoStatus(enc.NewWireView(interest.AppParam()), true)

	if err != nil {
		log.Warn(p, "Failed to parse status request", "err", err)
		return
	}

	resourceNameN := statusRequest.Name.Name
	log.Info(p, "Received internal status request", "resourceName", resourceNameN) // TODO: need a better name

	p.internalStatusRequestHandler(&args, statusRequest)
}

// setOnExternalStatusRequest sets the handler for external status requests
func (p *RepoProducerFacing) SetOnExternalStatusRequest(handler func(*ndn.InterestHandlerArgs, *tlv.RepoStatus)) {
	p.externalStatusRequestHandler = handler
}

// setOnInternalStatusRequest sets the handler for internal status requests
func (p *RepoProducerFacing) SetOnInternalStatusRequest(handler func(*ndn.InterestHandlerArgs, *tlv.RepoStatus)) {
	p.internalStatusRequestHandler = handler
}
