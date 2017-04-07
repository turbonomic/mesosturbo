package discovery

import (
	"github.com/turbonomic/turbo-go-sdk/pkg/builder"
	"github.com/turbonomic/turbo-go-sdk/pkg/proto"

	"github.com/golang/glog"
	"fmt"
	"github.com/turbonomic/mesosturbo/pkg/data"
)

const PROXY_VM_IP string = "Proxy_VM_IP"

// Builder for creating VM Entities to represent the Mesos Agents or Slaves in Turbo server
// This will create a proxy VM in the server. Hypervisor probes in the server will discover and manage the Agent VMs
type VMEntityBuilder struct {
	mesosMaster    *data.MesosMaster
	nodeMetricsMap map[string]*data.EntityResourceMetrics
	builderError   []error
}

// Build VM EntityDTO using the agent listed in the 'state' json returned from the Mesos Master
func (nb *VMEntityBuilder) BuildEntities() ([]*proto.EntityDTO, error) {
	glog.V(2).Infof("[BuildEntities] ...... ")
	nb.builderError  = []error{}
	result := []*proto.EntityDTO{}

	agentMap := nb.mesosMaster.AgentMap
	// For each agent
	for _, agent := range agentMap {
		nodeMetrics := nb.getAgentMetrics(agent)
		commoditiesSold, err := nb.vmCommSold(agent, nodeMetrics)
		if err != nil {
			nb.builderError = append(nb.builderError, err)
		}
		entityDTO, err := nb.vmEntity(agent, commoditiesSold)
		if err != nil {
			nb.builderError = append(nb.builderError, err)
		}
		result = append(result, entityDTO)
	}
	glog.V(4).Infof("[BuildEntities] VM DTOs :", result)
	return result, fmt.Errorf("VM entity builder errors: %+v", nb.builderError)
}

func (nb *VMEntityBuilder) getAgentMetrics(agent *data.Agent) *data.EntityResourceMetrics {
	nodeMetrics, ok := nb.nodeMetricsMap[agent.Id]
	if !ok {
		return nil
	}
	return nodeMetrics
}

// Build VM EntityDTO
func (nb *VMEntityBuilder) vmEntity(agentInfo *data.Agent,
	commoditiesSold []*proto.CommodityDTO) (*proto.EntityDTO, error) {
	entityDTOBuilder := builder.NewEntityDTOBuilder(proto.EntityDTO_VIRTUAL_MACHINE, agentInfo.Id).
		DisplayName(agentInfo.Name).
		SellsCommodities(commoditiesSold)
	// Stitching and proxy metadata
	ipAddress := agentInfo.IP
	ipPropName := PROXY_VM_IP // We create a different property for ip address, so the IP object in the server entity
	// is not deleted during reconciliation
	// TODO: create a builder for proxy VMs
	ipProp := &proto.EntityDTO_EntityProperty{
		Namespace: &DEFAULT_NAMESPACE,
		Name:      &ipPropName,
		Value:     &ipAddress,
	}
	entityDTOBuilder = entityDTOBuilder.WithProperty(ipProp)
	glog.Infof("[NodeBuilder] Parse node: The ip of vm to be reconcile with is %s", ipAddress)
	metaData := generateReconciliationMetaData()

	glog.Info("%s: metadata %s", agentInfo.IP, metaData)
	entityDTOBuilder = entityDTOBuilder.ReplacedBy(metaData)
	entityDto, err := entityDTOBuilder.Create()
	if err != nil {
		nb.builderError = append(nb.builderError, err)
	}
	return entityDto, nil
}

func generateReconciliationMetaData() *proto.EntityDTO_ReplacementEntityMetaData {
	replacementEntityMetaDataBuilder := builder.NewReplacementEntityMetaDataBuilder()
	replacementEntityMetaDataBuilder.Matching(PROXY_VM_IP)
	replacementEntityMetaDataBuilder.
	PatchSelling(proto.CommodityDTO_CPU_PROVISIONED).
		PatchSelling(proto.CommodityDTO_MEM_PROVISIONED).
		PatchSelling(proto.CommodityDTO_CLUSTER).
		PatchSelling(proto.CommodityDTO_VCPU).
		PatchSelling(proto.CommodityDTO_VMEM).
		PatchSelling(proto.CommodityDTO_VMPM_ACCESS)
	metaData := replacementEntityMetaDataBuilder.Build()
	return metaData
}

// ========================== Metrics and commodities =========================================
func (nb *VMEntityBuilder) vmCommSold(agentInfo *data.Agent, nodeMetrics *data.EntityResourceMetrics) ([]*proto.CommodityDTO, error) {
	if agentInfo == nil {
		return nil, fmt.Errorf("[VMEntityBuilder] Null agent while creating commodities")
	}
	if nodeMetrics == nil {
		return nil, fmt.Errorf("[VMEntityBuilder] Null node metrics for agent %s", agentInfo.Id)
	}
	var commoditiesSold []*proto.CommodityDTO
	// MemProv
	memProvCap := getValue(nodeMetrics, data.MEM_PROV, data.CAP)
	memProvUsed := getValue(nodeMetrics, data.MEM_PROV, data.USED)

	memProvComm, err := builder.NewCommodityDTOBuilder(proto.CommodityDTO_MEM_PROVISIONED).
		Capacity(*memProvCap).
		Used(*memProvUsed).
		Create()
	if err != nil {
		nb.builderError = append(nb.builderError, err)
	}
	commoditiesSold = append(commoditiesSold, memProvComm)

	// CpuProv
	cpuProvCap := getValue(nodeMetrics, data.CPU_PROV, data.CAP)
	cpuProvUsed := getValue(nodeMetrics, data.CPU_PROV, data.USED)

	cpuProvComm, err := builder.NewCommodityDTOBuilder(proto.CommodityDTO_CPU_PROVISIONED).
		Capacity(*cpuProvCap).
		Used(*cpuProvUsed).
		Create()
	if err != nil {
		nb.builderError = append(nb.builderError, err)
	}
	commoditiesSold = append(commoditiesSold, cpuProvComm)

	// VMem
	memCap := getValue(nodeMetrics, data.MEM, data.CAP)
	memUsed := getValue(nodeMetrics,data.MEM, data.USED)
	vMemComm, err := builder.NewCommodityDTOBuilder(proto.CommodityDTO_VMEM).
		Capacity(*memCap).
		Used(*memUsed).
		Create()
	if err != nil {
		nb.builderError = append(nb.builderError, err)
	}
	commoditiesSold = append(commoditiesSold, vMemComm)

	// VCpu
	cpuCap := getValue(nodeMetrics, data.CPU, data.CAP)
	cpuUsed := getValue(nodeMetrics, data.CPU, data.USED)
	vCpuComm, err := builder.NewCommodityDTOBuilder(proto.CommodityDTO_VCPU).
		Capacity(*cpuCap).
		Used(*cpuUsed).
		Create()
	if err != nil {
		nb.builderError = append(nb.builderError, err)
	}
	commoditiesSold = append(commoditiesSold, vCpuComm)

	// Access Commodities
	// ClusterComm
	clusterComm, err := builder.NewCommodityDTOBuilder(proto.CommodityDTO_CLUSTER).
		Key(nb.mesosMaster.Cluster.ClusterName).
		Create()
	if err != nil {
		nb.builderError = append(nb.builderError, err)
	}
	commoditiesSold = append(commoditiesSold, clusterComm)

	// TODO add port commodity sold

	return commoditiesSold, nil
}

