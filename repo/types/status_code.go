package types

// ReplyStatus determines if a related request is successful or not
// Note: we use uint64 instead of enum because the tlv generator does not currently support enums properly
const (
	ReplyStatusSuccess    uint64 = 200 // successful
	ReplyStatusError      uint64 = 500 // repo encounters an error when trying to process the request
	ReplyStatusNotFound   uint64 = 404 // resource (data / sync group) not found
	ReplyStatusInProgress uint64 = 102 // operations (e.g. replication) still in progress
)
