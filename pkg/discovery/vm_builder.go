package discovery

import (
	"github.com/turbonomic/turbo-go-sdk/pkg/builder"
	"github.com/turbonomic/turbo-go-sdk/pkg/proto"

	"fmt"
	"github.com/golang/glog"
	"github.com/turbonomic/mesosturbo/pkg/data"
)

const PROXY_VM_IP string = "Proxy_VM_IP"

// Builder for creating VM Entities to represent the Mesos Agents or Slaves in Turbo server
// This will create a proxy VM in the server. Hypervisor probes in the server will discover and manage the Agent VMs
type VMEntityBuilder struct {
	nodeRepository *NodeRepository
	errorCollector *ErrorCollector
	agent          *data.Agent
}

// Build VM EntityDTO using the agent listed in the 'state' json returned from the Mesos Master
func (nb *VMEntityBuilder) BuildEntities() ([]*proto.EntityDTO, error) {
	nb.errorCollector = new(ErrorCollector)
	glog.V(3).Infof("[BuildEntities] ...... ")
	result := []*proto.EntityDTO{}

	nodeEntity := nb.nodeRepository.GetAgentEntity()
	if nodeEntity == nil || nodeEntity.node == nil {
		nb.errorCollector.Collect(fmt.Errorf("Null agent %+v", nodeEntity))
		return result, fmt.Errorf("Null agent %+v", nodeEntity)
	}
	nb.agent = nodeEntity.node

	commoditiesSold, err := nb.vmCommSold(nodeEntity)
	nb.errorCollector.Collect(err)

	entityDTO, err := nb.vmEntity(nb.agent, commoditiesSold)
	nb.errorCollector.Collect(err)

	result = append(result, entityDTO)
	glog.V(4).Infof("[BuildEntities] VM DTOs :", result)

	var collectedErrors error
	if nb.errorCollector.Count() > 0 {
		fmt.Printf("All errors %s\n", nb.errorCollector)
		collectedErrors = fmt.Errorf("VM entity builder errors: %+v", nb.errorCollector)
	}
	return result, collectedErrors
}

// Build VM EntityDTO
func (nb *VMEntityBuilder) vmEntity(agentInfo *data.Agent,
	commoditiesSold []*proto.CommodityDTO) (*proto.EntityDTO, error) {
	entityDTOBuilder := builder.NewEntityDTOBuilder(proto.EntityDTO_VIRTUAL_MACHINE, agentInfo.Id).
		DisplayName(agentInfo.IP).
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
	metaData := generateReconciliationMetaData()

	glog.V(3).Infof("%s: vm stitiching metadata %s", agentInfo.IP, metaData)
	entityDTOBuilder = entityDTOBuilder.ReplacedBy(metaData)
	entityDto, err := entityDTOBuilder.Create()
	nb.errorCollector.Collect(err)

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
func (nb *VMEntityBuilder) vmCommSold(agentEntity *AgentEntity) ([]*proto.CommodityDTO, error) {
	if agentEntity == nil || agentEntity.node == nil {
		return nil, fmt.Errorf("[VMEntityBuilder] Null agent while creating commodities")
	}
	agentInfo := agentEntity.node
	if agentEntity.GetResourceMetrics() == nil {
		return nil, fmt.Errorf("[VMEntityBuilder] Null node metrics for agent %s", agentInfo.Id)
	}
	var commoditiesSold []*proto.CommodityDTO
	// MemProv
	memProvCap := getEntityMetricValue(agentEntity, data.MEM_PROV, data.CAP, nb.errorCollector)
	memProvUsed := getEntityMetricValue(agentEntity, data.MEM_PROV, data.USED, nb.errorCollector)

	memProvComm, err := builder.NewCommodityDTOBuilder(proto.CommodityDTO_MEM_PROVISIONED).
		Capacity(*memProvCap).
		Used(*memProvUsed).
		Create()
	nb.errorCollector.Collect(err)
	commoditiesSold = append(commoditiesSold, memProvComm)

	// CpuProv
	cpuProvCap := getEntityMetricValue(agentEntity, data.CPU_PROV, data.CAP, nb.errorCollector)
	cpuProvUsed := getEntityMetricValue(agentEntity, data.CPU_PROV, data.USED, nb.errorCollector)

	cpuProvComm, err := builder.NewCommodityDTOBuilder(proto.CommodityDTO_CPU_PROVISIONED).
		Capacity(*cpuProvCap).
		Used(*cpuProvUsed).
		Create()
	nb.errorCollector.Collect(err)
	commoditiesSold = append(commoditiesSold, cpuProvComm)

	// VMem
	memCap := getEntityMetricValue(agentEntity, data.MEM, data.CAP, nb.errorCollector)
	memUsed := getEntityMetricValue(agentEntity, data.MEM, data.USED, nb.errorCollector)
	vMemComm, err := builder.NewCommodityDTOBuilder(proto.CommodityDTO_VMEM).
		Capacity(*memCap).
		Used(*memUsed).
		Create()
	nb.errorCollector.Collect(err)
	commoditiesSold = append(commoditiesSold, vMemComm)

	// VCpu
	cpuCap := getEntityMetricValue(agentEntity, data.CPU, data.CAP, nb.errorCollector)
	cpuUsed := getEntityMetricValue(agentEntity, data.CPU, data.USED, nb.errorCollector)
	vCpuComm, err := builder.NewCommodityDTOBuilder(proto.CommodityDTO_VCPU).
		Capacity(*cpuCap).
		Used(*cpuUsed).
		Create()
	nb.errorCollector.Collect(err)
	commoditiesSold = append(commoditiesSold, vCpuComm)

	// Access Commodities
	// ClusterComm
	clusterComm, err := builder.NewCommodityDTOBuilder(proto.CommodityDTO_CLUSTER).
		Key(nb.agent.ClusterName).
		Create()
	nb.errorCollector.Collect(err)
	commoditiesSold = append(commoditiesSold, clusterComm)

	// TODO add port commodity sold

	return commoditiesSold, nil
}
