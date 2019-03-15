package cmscc

import (
	"github.com/hyperledger/fabric/common/flogging"

	"github.com/hyperledger/fabric/core/chaincode/shim"

	"github.com/hyperledger/fabric/core/scc"

	"github.com/hyperledger/fabric/common/config"
	"github.com/hyperledger/fabric/protos/common"
	pb "github.com/hyperledger/fabric/protos/peer"

	"fmt"

	"github.com/hyperledger/fabric/core/peer"

	"strings"

	"regexp"

	"bytes"
	"encoding/json"

	"strconv"

	"github.com/golang/protobuf/proto"
	"github.com/hyperledger/fabric/common/tools/configtxlator/update"
	"github.com/hyperledger/fabric/common/tools/protolator"
	"github.com/hyperledger/fabric/protos/msp"
)

const (
	AddOrgnization         string = "AddOrgnization"
	ReconfigOrgnizationMSP string = "ReconfigOrgnizationMSP"
	ProposeConfigUpdate    string = "ProposeConfigUpdate"
	AddSignature           string = "AddSignature"
	GetProposal            string = "GetProposal"
	ListProposals          string = "ListProposals"
	UpdateProposals        string = "UpdateProposals"

	NewProposalEvent  = "NewProposalEvent"
	NewSignatureEvent = "NewSignatureEvent"
)

type ConsortiumManager struct {
	sccProvider *scc.Provider
	configMgr   config.Manager
}

// New returns an instance of CMSCC.
// Typically this is called once per peer.
func New(sccProvider *scc.Provider) *ConsortiumManager {
	return &ConsortiumManager{
		sccProvider: sccProvider,
		configMgr:   peer.NewConfigSupport(),
	}
}

var cmscclogger = flogging.MustGetLogger("cmscc")

func (li *ConsortiumManager) Name() string              { return "cmscc" }
func (li *ConsortiumManager) Path() string              { return "github.com/hyperledger/fabric/core/scc/cmscc" }
func (li *ConsortiumManager) InitArgs() [][]byte        { return nil }
func (li *ConsortiumManager) Chaincode() shim.Chaincode { return li }
func (li *ConsortiumManager) InvokableExternal() bool   { return true }
func (li *ConsortiumManager) InvokableCC2CC() bool      { return false }
func (li *ConsortiumManager) Enabled() bool             { return true }

func (li *ConsortiumManager) Init(stub shim.ChaincodeStubInterface) pb.Response {
	cmscclogger.Info("Init CMSCC")

	return shim.Success(nil)
}

func (cm *ConsortiumManager) Invoke(stub shim.ChaincodeStubInterface) pb.Response {
	function, args := stub.GetFunctionAndParameters()
	switch function {
	case AddOrgnization:
		if len(args) != 4 {
			return shim.Error("incorrect number of arguments - expecting 4: ProposalID, OrgnizationName, ConfigGroup, Description")
		}
		proposalID := args[0]
		orgname := args[1]
		if !checkProposalID(proposalID) {
			return shim.Error("invalid ProposalID, ProposalID must be between 3 to 36 characters, and must consist of [a-z,A-Z,0-9,-,_]")
		}
		argCg := make(map[string]interface{})
		if err := json.Unmarshal([]byte(args[2]), &argCg); err != nil {
			return shim.Error(fmt.Sprintf("error happened unmarshalling ConfigGroup to map[string]interface{}: %v", err))
		}
		l1 := make(map[string]interface{})
		l1[orgname] = argCg
		l2 := make(map[string]interface{})
		l2["groups"] = l1
		l3 := make(map[string]interface{})
		l3["Application"] = l2
		l4 := make(map[string]interface{})
		l4["groups"] = l3
		inputCfg := make(map[string]interface{})
		inputCfg["channel_group"] = l4
		cfgBytes, err := json.Marshal(inputCfg)
		if err != nil {
			return shim.Error(fmt.Sprintf("error happened marshalling json {\"channel_group\":{\"groups\":{\"Application\":{\"groups\": {\"<OrgName>\":<ConfigGroup>}}}}}: %v", err))
		}

		finalCg := &common.Config{}
		if err := protolator.DeepUnmarshalJSON(bytes.NewReader(cfgBytes), finalCg); err != nil {
			return shim.Error(fmt.Sprintf("error happened unmarshalling Config to common.Config: %v", err))
		}
		finalCg.ChannelGroup.Groups["Application"].Groups[orgname].Version = uint64(PatchMode_OVERWRITE)

		return cm.proposeConfigUpdate(stub, proposalID, finalCg, args[3])
	case ReconfigOrgnizationMSP:
		if len(args) != 3 {
			return shim.Error("incorrect number of arguments - expecting 4: ProposalID, MSP, Description")
		}
		proposalID := args[0]
		if !checkProposalID(proposalID) {
			return shim.Error("invalid ProposalID, ProposalID must be between 3 to 36 characters, and must consist of [a-z,A-Z,0-9,-,_]")
		}
		creator, err := stub.GetCreator()
		if err != nil {
			return shim.Error("error happened reading the transaction creator: " + err.Error())
		}
		mspID, err := getMSPID(creator)
		if err != nil {
			return shim.Error(err.Error())
		}

		argCv := make(map[string]interface{})
		if err := json.Unmarshal([]byte(args[1]), &argCv); err != nil {
			return shim.Error(fmt.Sprintf("error happened unmarshalling ConfigValue to map[string]interface{}: %v", err))
		}
		l1 := make(map[string]interface{})
		l1["MSP"] = argCv
		l2 := make(map[string]interface{})
		l2["values"] = l1
		l3 := make(map[string]interface{})
		l3[mspID] = l2
		l4 := make(map[string]interface{})
		l4["groups"] = l3
		l5 := make(map[string]interface{})
		l5["Application"] = l4
		l6 := make(map[string]interface{})
		l6["groups"] = l5
		inputCfg := make(map[string]interface{})
		inputCfg["channel_group"] = l6
		cfgBytes, err := json.Marshal(inputCfg)
		if err != nil {
			return shim.Error(fmt.Sprintf("error happened marshalling json {\"channel_group\":{\"groups\":{\"Application\":{\"groups\": {\"<OrgName>\":<ConfigGroup>}}}}}: %v", err))
		}

		finalCg := &common.Config{}
		if err := protolator.DeepUnmarshalJSON(bytes.NewReader(cfgBytes), finalCg); err != nil {
			return shim.Error(fmt.Sprintf("error happened unmarshalling Config to common.Config: %v", err))
		}
		finalCg.ChannelGroup.Groups["Application"].Groups[mspID].Values["MSP"].Version = uint64(PatchMode_OVERWRITE)

		//argCv := &common.ConfigValue{}
		//argCv.Version = uint64(PatchMode_OVERWRITE)
		//if err := protolator.DeepUnmarshalJSON(strings.NewReader(args[1]), argCv); err != nil {
		//	return shim.Error(fmt.Sprintf("error happened unmarshalling ConfigGroup to common.ConfigGroup: %v", err))
		//}
		//
		//
		//cfg := &common.Config{}
		//cfg.ChannelGroup = &common.ConfigGroup{}
		//cfg.ChannelGroup.Groups = make(map[string]*common.ConfigGroup)
		//cfg.ChannelGroup.Groups["Application"] = &common.ConfigGroup{}
		//cfg.ChannelGroup.Groups["Application"].Groups = make(map[string]*common.ConfigGroup)
		//cfg.ChannelGroup.Groups["Application"].Groups[mspID] = &common.ConfigGroup{}
		//cfg.ChannelGroup.Groups["Application"].Groups[mspID].Values = make(map[string]*common.ConfigValue)
		//cfg.ChannelGroup.Groups["Application"].Groups[mspID].Values["MSP"] = argCv

		return cm.proposeConfigUpdate(stub, proposalID, finalCg, args[2])
	case ProposeConfigUpdate:
		if len(args) != 3 {
			return shim.Error("incorrect number of arguments - expecting 3: ProposalID, Config, Description")
		}
		proposalID := args[0]

		if !checkProposalID(proposalID) {
			return shim.Error("invalid ProposalID, ProposalID must be between 3 to 36 characters, and must consist of [a-z,A-Z,0-9,-,_]")
		}

		argCg := &common.Config{}
		if err := protolator.DeepUnmarshalJSON(strings.NewReader(args[1]), argCg); err != nil {
			return shim.Error(fmt.Sprintf("error happened unmarshalling Config to common.Config: %v", err))
		}

		return cm.proposeConfigUpdate(stub, proposalID, argCg, args[2])

	case AddSignature:
		return cm.addSignature(stub)

	case GetProposal:
		return cm.getProposal(stub)

	case ListProposals:
		return cm.listProposals(stub)

	case UpdateProposals:
		return cm.updateProposals(stub)

	default:
		return shim.Error("Invalid invoke function name. Expecting \"AddOrgnization\" \"ReconfigOrgnizationMSP\" \"ProposeConfigUpdate\" \"SignProposal\" \"ListProposal\" \"ListProposals\" \"UpdateProposals\"")
	}
}

func (cm *ConsortiumManager) proposeConfigUpdate(stub shim.ChaincodeStubInterface, proposalID string, config *common.Config, description string) pb.Response {
	// apply patch to current ConfigGroup
	channel := stub.GetChannelID()
	channelCfg := cm.configMgr.GetChannelConfig(channel)
	curCg, ok := proto.Clone(channelCfg.ConfigProto().ChannelGroup).(*common.ConfigGroup)
	if !ok {
		return shim.Error("error happened cloning current ChannelGroup")
	}
	patchedCg, err := patchConfigGroup(curCg, config.ChannelGroup)
	if err != nil {
		return shim.Error("error happened patching ConfigGroup, error: " + err.Error())
	}
	patchedConfig := &common.Config{
		Sequence:     0,
		ChannelGroup: patchedCg,
	}

	// configtxlator compute
	cu, err := update.Compute(channelCfg.ConfigProto(), patchedConfig)
	if err != nil {
		return shim.Error("error happened computing ConfigUpdate, error: " + err.Error())
	}
	cu.ChannelId = channel
	cuBytes, err := protolator.MostlyDeterministicMarshal(cu)
	if err != nil {
		return shim.Error("error happened marshalling ConfigUpdate, error: " + err.Error())
	}

	config.Sequence = channelCfg.ConfigProto().Sequence
	cfgBytes, err := protolator.MostlyDeterministicMarshal(config)
	if err != nil {
		return shim.Error("error happened marshalling Config, error: " + err.Error())
	}

	proposal := &Proposal{
		Description:  description,
		Config:       cfgBytes,
		ConfigUpdate: cuBytes,
	}

	pBytes, err := protolator.MostlyDeterministicMarshal(proposal)
	if err != nil {
		return shim.Error("error happened marshalling Proposal, error: " + err.Error())
	}
	if err := stub.PutState(string(proposalID), pBytes); err != nil {
		return shim.Error("error happened persisting the new proposal on the ledger: " + err.Error())
	}
	if err = stub.SetEvent(NewProposalEvent, []byte(proposalID)); err != nil {
		return shim.Error("error happened emitting event: " + err.Error())
	}
	return shim.Success(nil)
}

func (cm *ConsortiumManager) addSignature(stub shim.ChaincodeStubInterface) pb.Response {
	_, args := stub.GetFunctionAndParameters()
	if len(args) != 2 {
		return shim.Error("incorrect number of arguments - expecting 2: ProposalID, Signature")
	}
	proposalID := args[0]

	if !checkProposalID(proposalID) {
		return shim.Error("invalid ProposalID, ProposalID must be between 3 to 36 characters, and must consist of [a-z,A-Z,0-9,-,_]")
	}
	sig := &common.ConfigSignature{}
	if err := protolator.DeepUnmarshalJSON(strings.NewReader(args[1]), sig); err != nil {
		return shim.Error(fmt.Sprintf("error happened unmarshalling Signature to common.ConfigSignature: %v", err))
	}
	sigBytes, err := protolator.MostlyDeterministicMarshal(sig)
	if err != nil {
		return shim.Error("error happened marshalling ConfigSignature, error: " + err.Error())
	}
	creator, err := stub.GetCreator()
	if err != nil {
		return shim.Error("error happened reading the transaction creator: " + err.Error())
	}
	mspID, err := getMSPID(creator)
	if err != nil {
		return shim.Error(err.Error())
	}

	// fetch and update the state of the proposal
	pBytes, err := stub.GetState(proposalID)
	if err != nil {
		return shim.Error(fmt.Sprintf("error happened reading proposal with id (%s) to update signature: %v ", proposalID, err))
	}
	if len(pBytes) == 0 {
		return shim.Error(fmt.Sprintf("proposal with id (%s) not found", proposalID))
	}
	proposal := &Proposal{}
	if err := proto.Unmarshal(pBytes, proposal); err != nil {
		return shim.Error("error happened unmarshalling the proposal representation to struct: " + err.Error())
	}
	if proposal.Signatures == nil {
		proposal.Signatures = make(map[string][]byte)
	}
	proposal.Signatures[mspID] = sigBytes

	// store the updated proposal
	pBytesUpdated, err := protolator.MostlyDeterministicMarshal(proposal)
	if err != nil {
		return shim.Error("error happened marshalling the updated proposal: " + err.Error())
	}
	if err := stub.PutState(proposalID, pBytesUpdated); err != nil {
		return shim.Error("error happened persisting the updated proposal on the ledger: " + err.Error())
	}
	if err = stub.SetEvent(NewSignatureEvent, []byte(proposalID)); err != nil {
		return shim.Error("error happened emitting event: " + err.Error())
	}
	return shim.Success(nil)
}

func (cm *ConsortiumManager) getProposal(stub shim.ChaincodeStubInterface) pb.Response {
	_, args := stub.GetFunctionAndParameters()
	if len(args) != 1 {
		return shim.Error("incorrect number of arguments - expecting 1: ProposalID")
	}
	proposalID := args[0]

	if !checkProposalID(proposalID) {
		return shim.Error("invalid ProposalID, ProposalID must be between 3 to 36 characters, and must consist of [a-z,A-Z,0-9,-,_]")
	}

	pBytes, err := stub.GetState(proposalID)
	if err != nil {
		return shim.Error(fmt.Sprintf("error happened reading proposal with id (%s) to update signature: %v", proposalID, err))
	}
	if len(pBytes) == 0 {
		return shim.Error(fmt.Sprintf("proposal with id (%s) not found", proposalID))
	}
	proposal := &Proposal{}
	if err := proto.Unmarshal(pBytes, proposal); err != nil {
		return shim.Error("error happened unmarshalling the proposal representation to struct: " + err.Error())
	}
	if proposal.Signatures == nil {
		proposal.Signatures = make(map[string][]byte)
	}
	channel := stub.GetChannelID()
	config := cm.configMgr.GetChannelConfig(channel)
	ps, err := analysisProposal(channel, config, proposal)
	if err != nil {
		ps = &ProposalStatus{
			Status:       Broken,
			ErrorMessage: err.Error(),
		}
	}
	if psJson, err := json.Marshal(ps); err != nil {
		return shim.Error("error happened marshalling the ProposalStatus to JSON: " + err.Error())
	} else {
		return shim.Success(psJson)
	}
}

func (cm *ConsortiumManager) listProposals(stub shim.ChaincodeStubInterface) pb.Response {
	_, args := stub.GetFunctionAndParameters()
	if len(args) != 0 {
		return shim.Error("incorrect number of arguments - expecting 0")
	}
	pss := make(map[string]*ProposalStatus)
	psi, err := stub.GetStateByRange("", "")
	if err != nil {
		return shim.Error("error happened reading keys from ledger: " + err.Error())
	}
	defer psi.Close()

	for psi.HasNext() {
		kv, err := psi.Next()
		if err != nil {
			return shim.Error("error happened iterating over available proposals: " + err.Error())
		}
		proposal := &Proposal{}
		if err := proto.Unmarshal(kv.Value, proposal); err != nil {
			return shim.Error("error happened unmarshalling the proposal representation to struct: " + err.Error())
		}
		if proposal.Signatures == nil {
			proposal.Signatures = make(map[string][]byte)
		}
		channel := stub.GetChannelID()
		config := cm.configMgr.GetChannelConfig(channel)
		ps, err := analysisProposal(channel, config, proposal)
		if err != nil {
			ps = &ProposalStatus{
				Status:       Broken,
				ErrorMessage: err.Error(),
			}
		}
		pss[kv.Key] = ps
	}
	if psJson, err := json.Marshal(pss); err != nil {
		return shim.Error("error happened marshalling the ProposalStatus map to JSON: " + err.Error())
	} else {
		return shim.Success(psJson)
	}
}

func (cm *ConsortiumManager) updateProposals(stub shim.ChaincodeStubInterface) pb.Response {
	_, args := stub.GetFunctionAndParameters()
	if len(args) != 1 {
		return shim.Error("incorrect number of arguments - expecting 1: UpdateMode")
	}
	mode, err := strconv.Atoi(args[0])
	if err != nil {
		return shim.Error("error happened covering UpdateMode to int, error: " + err.Error())
	}
	psi, err := stub.GetStateByRange("", "")
	if err != nil {
		return shim.Error("error happened reading keys from ledger: " + err.Error())
	}
	defer psi.Close()

	for psi.HasNext() {
		kv, err := psi.Next()
		if err != nil {
			return shim.Error("error happened iterating over available proposals: " + err.Error())
		}
		proposal := &Proposal{}
		if err := proto.Unmarshal(kv.Value, proposal); err != nil {
			return shim.Error("error happened unmarshalling the proposal representation to struct: " + err.Error())
		}
		if proposal.Signatures == nil {
			proposal.Signatures = make(map[string][]byte)
		}
		channel := stub.GetChannelID()
		config := cm.configMgr.GetChannelConfig(channel)
		ps, err := analysisProposal(channel, config, proposal)
		if err != nil {
			ps = &ProposalStatus{
				Status:       Broken,
				ErrorMessage: err.Error(),
			}
		}
		switch ps.Status {
		case Broken:
			if mode&ModeRemoveBroken != 0 {
				stub.DelState(kv.Key)
			}
		case Submitted:
			if mode&ModeRemoveSubmitted != 0 {
				stub.DelState(kv.Key)
			}
		case Outofdate:
			if mode&ModeUpdateIfNecessary != 0 {
				pBytes, err := protolator.MostlyDeterministicMarshal(proposal)
				if err != nil {
					return shim.Error("error happened marshalling proposal: " + err.Error())
				}
				err = stub.PutState(kv.Key, pBytes)
				if err != nil {
					return shim.Error("error happened persisting the updated proposal on the ledger: " + err.Error())
				}
			}
		}
	}

	return shim.Success(nil)
}

const (
	Broken               string = "Broken"
	Submitted            string = "Submitted"
	Outofdate            string = "Outofdate"
	WaitingForSignatures string = "WaitingForSignatures"
	ReadyToSubmit        string = "ReadyToSubmit"
)
const (
	ModeRemoveBroken int = 1 << iota
	ModeRemoveSubmitted
	ModeUpdateIfNecessary
)

type ProposalStatus struct {
	Status                       string   `json:"status"`
	SimulationResult             string   `json:"simulation_result,omitempty"`
	SignedOrgnizations           []string `json:"signed_orgnizations,omitempty"`
	ErrorMessage                 string   `json:"error_message,omitempty"`
	ConfigUpdateToSign           []byte   `json:"config_update_to_sign,omitempty"`
	ConfigUpdateEnvelopeToSubmit []byte   `json:"config_update_envelope_to_submit,omitempty"`
}

func analysisProposal(channel string, config config.Config, proposal *Proposal) (*ProposalStatus, error) {
	ret := &ProposalStatus{}
	rawProposal := &common.Config{}
	if err := proto.Unmarshal(proposal.Config, rawProposal); err != nil {
		return nil, fmt.Errorf("error happened unmarshalling proposal.Config to struct: %s", err.Error())
	}

	//Configblock has been updated， recompute ConfigUpdate
	if config.ConfigProto().Sequence > rawProposal.Sequence {
		curCg, ok := proto.Clone(config.ConfigProto().ChannelGroup).(*common.ConfigGroup)
		if !ok {
			return nil, fmt.Errorf("error happened cloning current ChannelGroup")
		}
		patchedCg, err := patchConfigGroup(curCg, rawProposal.ChannelGroup)
		if err != nil {
			return nil, fmt.Errorf("error happened patching ConfigGroup, error: %s", err.Error())
		}
		patchedConfig := &common.Config{
			Sequence:     0,
			ChannelGroup: patchedCg,
		}

		// configtxlator compute
		cu, err := update.Compute(config.ConfigProto(), patchedConfig)
		if err != nil {
			if err.Error() == "no differences detected between original and updated config" {
				ret.Status = Submitted
				return ret, nil
			} else {
				return nil, fmt.Errorf("error happend compute config update, error: %s", err.Error())
			}
		}
		cu.ChannelId = channel
		cuBytes, err := protolator.MostlyDeterministicMarshal(cu)
		if err != nil {
			return nil, fmt.Errorf("error happend marshalling ConfigUpdate, error: %s", err.Error())
		}
		if !bytes.Equal(cuBytes, proposal.ConfigUpdate) {
			rawProposal.Sequence = config.ConfigProto().Sequence
			configUpdated, err := protolator.MostlyDeterministicMarshal(rawProposal)
			if err != nil {
				return nil, fmt.Errorf("error happened marshalling Config, error: %s", err.Error())
			}
			proposal.Config = configUpdated
			proposal.ConfigUpdate = cuBytes
			proposal.Signatures = nil
			ret.Status = Outofdate
			return ret, nil
		}
	}
	var sigs []*common.ConfigSignature
	var signedOrgs []string
	if proposal.Signatures != nil && len(proposal.Signatures) > 0 {
		sigs = make([]*common.ConfigSignature, 0, len(proposal.Signatures))
		signedOrgs = make([]string, 0, len(proposal.Signatures))
		for k, v := range proposal.Signatures {
			s := &common.ConfigSignature{}
			if err := proto.Unmarshal(v, s); err != nil {
				return nil, fmt.Errorf("error happened unmarshalling ConfigSignature, error: %s", err.Error())
			}
			sigs = append(sigs, s)
			signedOrgs = append(signedOrgs, k)
		}
	} else {
		sigs = nil
	}

	cuEnv, err := sealEnvelope(channel, proposal.ConfigUpdate, sigs)
	if err != nil {
		return nil, err
	}
	if _, err := config.ProposeConfigUpdate(cuEnv); err != nil {
		ret.Status = WaitingForSignatures
		ret.SimulationResult = err.Error()
		ret.SignedOrgnizations = signedOrgs
		cuToSign, err := sealEnvelope(channel, proposal.ConfigUpdate, nil)
		if err != nil {
			return nil, err
		}
		cuToSignBytes, err := protolator.MostlyDeterministicMarshal(cuToSign)
		if err != nil {
			return nil, fmt.Errorf("error happened marshalling config_update_to_sign to ConfigUpdateEnvelope, error: %s", err.Error())
		}
		ret.ConfigUpdateToSign = cuToSignBytes
		return ret, nil
	} else {
		ret.Status = ReadyToSubmit
		cuenvBytes, err := protolator.MostlyDeterministicMarshal(cuEnv)
		if err != nil {
			return nil, fmt.Errorf("error happened marshalling ConfigUpdateEnvelope, error: %s", err.Error())
		}
		ret.ConfigUpdateEnvelopeToSubmit = cuenvBytes
		return ret, nil
	}
}

func sealEnvelope(channel string, configUpdate []byte, sigs []*common.ConfigSignature) (*common.Envelope, error) {
	cue := &common.ConfigUpdateEnvelope{
		ConfigUpdate: configUpdate,
		Signatures:   sigs,
	}
	data, err := protolator.MostlyDeterministicMarshal(cue)
	if err != nil {
		return nil, fmt.Errorf("error happened marshalling ConfigUpdateEnvelope, error: %s", err.Error())
	}
	channelHeader := &common.ChannelHeader{
		Type:      int32(common.HeaderType_CONFIG_UPDATE),
		ChannelId: channel,
	}
	channelHeaderBytes, err := protolator.MostlyDeterministicMarshal(channelHeader)
	if err != nil {
		return nil, fmt.Errorf("error happened marshalling ChannelHeader, error: %s", err.Error())
	}
	payload := &common.Payload{
		Header: &common.Header{
			ChannelHeader: channelHeaderBytes,
		},
		Data: data,
	}
	payloadBytes, err := protolator.MostlyDeterministicMarshal(payload)
	if err != nil {
		return nil, fmt.Errorf("error happened marshalling Payload, error: %s", err.Error())
	}
	cuEnv := &common.Envelope{
		Payload: payloadBytes,
	}
	return cuEnv, nil
}

func checkProposalID(proposalID string) bool {
	if matched, err := regexp.Match(`^[a-zA-Z0-9_\-]{3,36}$`, []byte(proposalID)); matched && err == nil {
		return true
	} else {
		return false
	}
}

func patchConfigGroup(base, patch *common.ConfigGroup) (*common.ConfigGroup, error) {
	if patch == nil {
		return base, nil
	}
	if base == nil {
		base = &common.ConfigGroup{}
	}
	switch common.PatchMode(patch.Version) {
	case common.PatchMode_OVERWRITE:
		return patch, nil
	case common.PatchMode_DELETE:
		return nil, nil
	case common.PatchMode_PATCH:
		// patch Groups
		if len(patch.Groups) > 0 && base.Groups == nil {
			base.Groups = make(map[string]*common.ConfigGroup)
		}
		for k, v := range patch.Groups {
			child := base.Groups[k]
			patched, err := patchConfigGroup(child, v)
			if err != nil {
				return nil, err
			}
			if patched == nil {
				delete(base.Groups, k)
			} else {
				base.Groups[k] = patched
			}
		}
		if len(base.Groups) == 0 {
			base.Groups = nil
		}

		// patch Values
		if len(patch.Values) > 0 && base.Values == nil {
			base.Values = make(map[string]*common.ConfigValue)
		}
		for k, v := range patch.Values {
			switch common.PatchMode(v.Version) {
			case common.PatchMode_OVERWRITE:
				base.Values[k] = v
			case common.PatchMode_DELETE:
				delete(base.Values, k)
			case common.PatchMode_PATCH:
				if _, ok := base.Values[k]; !ok {
					base.Values[k] = &common.ConfigValue{}
				}
				if v.Value != nil {
					base.Values[k].Value = v.Value
				}
				if v.ModPolicy != "" {
					base.Values[k].ModPolicy = v.ModPolicy
				}
				if base.Values[k].Value == nil && base.Values[k].ModPolicy == "" {
					delete(base.Values, k)
				}
			}
		}
		if len(base.Values) == 0 {
			base.Values = nil
		}

		// patch Policies
		if len(patch.Policies) > 0 && base.Policies == nil {
			base.Policies = make(map[string]*common.ConfigPolicy)
		}
		for k, v := range patch.Policies {
			switch common.PatchMode(v.Version) {
			case common.PatchMode_OVERWRITE:
				base.Policies[k] = v
			case common.PatchMode_DELETE:
				delete(base.Policies, k)
			case common.PatchMode_PATCH:
				if _, ok := base.Policies[k]; !ok {
					base.Policies[k] = &common.ConfigPolicy{}
				}
				if v.Policy != nil {
					base.Policies[k].Policy = v.Policy
				}
				if v.ModPolicy != "" {
					base.Policies[k].ModPolicy = v.ModPolicy
				}
				if base.Policies[k].Policy == nil && base.Policies[k].ModPolicy == "" {
					delete(base.Policies, k)
				}
			}
		}
		if len(base.Policies) == 0 {
			base.Policies = nil
		}

		// check empty
		if base.Groups == nil && base.Values == nil && base.Policies == nil && base.ModPolicy == "" {
			base = nil
		}
		return base, nil
	default:
		return nil, fmt.Errorf("invalid patch mode: %d", patch.Version)
	}
}

func getMSPID(creator []byte) (string, error) {
	identity := &msp.SerializedIdentity{}
	if err := proto.Unmarshal(creator, identity); err != nil {
		return "", fmt.Errorf("error happened unmarshalling the creator: %v", err)
	}
	return identity.Mspid, nil
}
