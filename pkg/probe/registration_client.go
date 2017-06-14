package probe

import (
	"github.com/golang/glog"
	// turbo sdk imports
	"github.com/turbonomic/mesosturbo/pkg/conf"
	"github.com/turbonomic/turbo-go-sdk/pkg/builder"
	"github.com/turbonomic/turbo-go-sdk/pkg/probe"
	"github.com/turbonomic/turbo-go-sdk/pkg/proto"
	"github.com/turbonomic/turbo-go-sdk/pkg/supplychain"
)

// Registration Client for the Mesos Probe
// Implements the TurboRegistrationClient interface
type MesosRegistrationClient struct {
	mesosMasterType conf.MesosMasterType
}

func NewRegistrationClient(mesosMasterType conf.MesosMasterType) probe.TurboRegistrationClient {
	client := &MesosRegistrationClient{
		mesosMasterType: mesosMasterType,
	}
	return client
}

var (
	vmType        proto.EntityDTO_EntityType = proto.EntityDTO_VIRTUAL_MACHINE
	containerType proto.EntityDTO_EntityType = proto.EntityDTO_CONTAINER
	appType       proto.EntityDTO_EntityType = proto.EntityDTO_APPLICATION

	vCpuType            proto.CommodityDTO_CommodityType = proto.CommodityDTO_VCPU
	vMemType            proto.CommodityDTO_CommodityType = proto.CommodityDTO_VMEM
	vCpuProvisionedType proto.CommodityDTO_CommodityType = proto.CommodityDTO_CPU_PROVISIONED
	vMemProvisionedType proto.CommodityDTO_CommodityType = proto.CommodityDTO_MEM_PROVISIONED
	appCommType         proto.CommodityDTO_CommodityType = proto.CommodityDTO_APPLICATION
	clusterType         proto.CommodityDTO_CommodityType = proto.CommodityDTO_CLUSTER
	networkType         proto.CommodityDTO_CommodityType = proto.CommodityDTO_NETWORK

	//Commodity key is optional, when key is set, it serves as a constraint between seller and buyer
	//for example, the buyer can only go to a seller that sells the commodity with the required key
	vCpuTemplateComm     *proto.TemplateCommodity = &proto.TemplateCommodity{CommodityType: &vCpuType}
	vMemTemplateComm     *proto.TemplateCommodity = &proto.TemplateCommodity{CommodityType: &vMemType}
	vCpuProvTemplateComm *proto.TemplateCommodity = &proto.TemplateCommodity{CommodityType: &vCpuProvisionedType}
	vMemProvTemplateComm *proto.TemplateCommodity = &proto.TemplateCommodity{CommodityType: &vMemProvisionedType}

	fakeKey                    string                   = "fake"
	appTemplateCommWithKey     *proto.TemplateCommodity = &proto.TemplateCommodity{CommodityType: &appCommType, Key: &fakeKey}
	clusterTemplateCommWithKey *proto.TemplateCommodity = &proto.TemplateCommodity{CommodityType: &clusterType, Key: &fakeKey}
)

func (registrationClient *MesosRegistrationClient) GetSupplyChainDefinition() []*proto.TemplateDTO {
	// VM Node
	vmSupplyChainNodeBuilder := supplychain.NewSupplyChainNodeBuilder(vmType).
		Sells(vCpuTemplateComm).
		Sells(vMemTemplateComm).
		Sells(vCpuProvTemplateComm).
		Sells(vMemProvTemplateComm).
		Sells(clusterTemplateCommWithKey)

	// Container Node
	containerSupplyChainNodeBuilder := supplychain.NewSupplyChainNodeBuilder(containerType).
		Sells(vCpuTemplateComm).
		Sells(vMemTemplateComm).
		Sells(appTemplateCommWithKey)

	// Container Node to VM Link
	containerSupplyChainNodeBuilder = containerSupplyChainNodeBuilder.
		Provider(vmType, proto.Provider_HOSTING).
		Buys(vCpuTemplateComm).
		Buys(vMemTemplateComm).
		Buys(vCpuProvTemplateComm).
		Buys(vMemProvTemplateComm).
		Buys(clusterTemplateCommWithKey)

	// Application Node
	appSupplyChainNodeBuilder := supplychain.NewSupplyChainNodeBuilder(appType)

	// Application Node to Container Link
	appSupplyChainNodeBuilder = appSupplyChainNodeBuilder.
		Provider(containerType, proto.Provider_HOSTING).
		Buys(vCpuTemplateComm).
		Buys(vMemTemplateComm).
		Buys(appTemplateCommWithKey)

	// External Link from Container (Pod) to VM
	containerVmExtLinkBuilder := supplychain.NewExternalEntityLinkBuilder().
		Link(containerType, vmType,
			proto.Provider_HOSTING).
		Commodity(vCpuType, false).
		Commodity(vMemType, false).
		Commodity(vCpuProvisionedType, false).
		Commodity(vMemProvisionedType, false).
		Commodity(clusterType, true).
		ProbeEntityPropertyDef(supplychain.SUPPLY_CHAIN_CONSTANT_IP_ADDRESS,
			"IP Address where the Container is running").
		ExternalEntityPropertyDef(supplychain.VM_IP)

	containerVmExternalLink, err := containerVmExtLinkBuilder.Build()
	if err != nil {
		glog.Errorf("[MesosRegistrationClient] error creating vm external link : %s", err);
	}
	containerSupplyChainNodeBuilder.ConnectsTo(containerVmExternalLink)

	appNode, err := appSupplyChainNodeBuilder.Create()
	if err != nil {
		glog.Errorf("[MesosRegistrationClient] error creating application node : %s", err);
	}
	containerNode, err := containerSupplyChainNodeBuilder.Create()
	if err != nil {
		glog.Errorf("[MesosRegistrationClient] error creating container node :  %s", err);
	}

	vmNode, err := vmSupplyChainNodeBuilder.Create()
	if err != nil {
		glog.Errorf("[MesosRegistrationClient] error creating virtual machine node : %s", err);
	}

	supplyChainBuilder := supplychain.NewSupplyChainBuilder()
	supplyChainBuilder.
		Top(appNode).
		Entity(containerNode).
		Entity(vmNode)

	supplychain, err := supplyChainBuilder.Create()
	if err != nil {
		glog.Errorf("[MesosRegistrationClient] error creating supply chain  : %s", err);
	}

	glog.Infof("[MesosRegistrationClient] Created supply chain")
	return supplychain
}

func (registrationClient *MesosRegistrationClient) GetIdentifyingFields() string {
	return string(conf.MasterIPPort)
}

// The return type is a list of ProbeInfo_AccountDefProp.
// For a valid definition, targetNameIdentifier, username and password should be contained.
// Account Definition for Mesos Probe
func (registrationClient *MesosRegistrationClient) GetAccountDefinition() []*proto.AccountDefEntry {
	var acctDefProps []*proto.AccountDefEntry

	// master ip port list
	masterIPListAcctDefEntry := builder.NewAccountDefEntryBuilder(string(conf.MasterIPPort), string(conf.MasterIPPort),
		"Comma separated list of `host:port` for each Mesos Master in the cluster", ".*",
		true, false).
		Create()
	acctDefProps = append(acctDefProps, masterIPListAcctDefEntry)

	// username
	usernameAcctDefEntry := builder.NewAccountDefEntryBuilder(string(conf.MasterUsername), string(conf.MasterUsername),
		"Username of the mesos master", ".*",
		false, false).
		Create()
	acctDefProps = append(acctDefProps, usernameAcctDefEntry)

	// password
	passwdAcctDefEntry := builder.NewAccountDefEntryBuilder(string(conf.MasterPassword), string(conf.MasterPassword),
		"Password of the mesos master", ".*",
		false, true).
		Create()
	acctDefProps = append(acctDefProps, passwdAcctDefEntry)

	//if registrationClient.mesosMasterType == conf.Apache {
	//	// framework id
	//	frameworkIpAcctDefEntry := builder.NewAccountDefEntryBuilder(string(conf.FrameworkIP), string(conf.FrameworkIP),
	//		"IP for the Framework", ".*",
	//		false, false).
	//		Create()
	//	acctDefProps = append(acctDefProps, frameworkIpAcctDefEntry)
	//
	//	// framework port
	//	frameworkPortAcctDefEntry := builder.NewAccountDefEntryBuilder(string(conf.FrameworkPort), string(conf.FrameworkPort),
	//		"Port for the Framework", ".*",
	//		false, false).
	//		Create()
	//	acctDefProps = append(acctDefProps, frameworkPortAcctDefEntry)
	//
	//	// username
	//	frameworkUserAcctDefEntry := builder.NewAccountDefEntryBuilder(string(conf.FrameworkUsername), string(conf.FrameworkUsername),
	//		"Username for the framework", ".*",
	//		false, false).
	//		Create()
	//	acctDefProps = append(acctDefProps, frameworkUserAcctDefEntry)
	//
	//	// password
	//	frameworkPwdAcctDefEntry := builder.NewAccountDefEntryBuilder(string(conf.FrameworkPassword), string(conf.FrameworkPassword),
	//		"Password for the framework", ".*",
	//		false, true).
	//		Create()
	//	acctDefProps = append(acctDefProps, frameworkPwdAcctDefEntry)
	//}

	return acctDefProps
}
