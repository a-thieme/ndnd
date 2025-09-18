//go:generate gondn_tlv_gen
package tlv

import (
	enc "github.com/named-data/ndnd/std/encoding"
	spec "github.com/named-data/ndnd/std/ndn/spec_2022"
)

var SyncProtocolSvsV3 = enc.Name{
	enc.NewKeywordComponent("ndn"),
	enc.NewKeywordComponent("svs"),
	enc.NewVersionComponent(3),
}

type RepoCommand struct {
	//+field:string
	Type string `tlv:"0x252"`
	//+field:struct:spec.NameContainer
	Target *spec.NameContainer `tlv:"0x253"`
	//+field:natural
	SnapshotThreshold uint64 `tlv:"0x255"`
}

type AwarenessUpdate struct {
	//+field:struct:spec.NameContainer
	Node *spec.NameContainer `tlv:"0x240"`
	//+field:sequence:*RepoCommand:struct:RepoCommand
	ActiveJobs []*RepoCommand `tlv:"0x241"`
}

type RepoStatusResponse struct {
	//+field:struct:spec.NameContainer
	Target *spec.NameContainer `tlv:"0x280"`
	//+field:natural
	Status uint64 `tlv:"0x281"`
}
