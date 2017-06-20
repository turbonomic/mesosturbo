package discovery

import (
	"fmt"
	"github.com/golang/glog"
	"github.com/turbonomic/mesosturbo/pkg/data"
	"github.com/turbonomic/turbo-go-sdk/pkg/builder"
	"github.com/turbonomic/turbo-go-sdk/pkg/proto"
	"github.com/turbonomic/turbo-go-sdk/pkg/supplychain"
)

// Builder for creating Container Entities to represent the default container Mesos Tasks in Turbo server
type ContainerEntityBuilder struct {
	nodeRepository *NodeRepository
	errorCollector *ErrorCollector
	agent          *data.Agent
}

// Build Container EntityDTO using the tasks listed in 'state' json returned from the Mesos Master
func (cb *ContainerEntityBuilder) BuildEntities() ([]*proto.EntityDTO, error) {
	cb.errorCollector = new(ErrorCollector)
	glog.V(3).Infof("[BuildEntities] ....")
	result := []*proto.EntityDTO{}

	var containerEntitiesMap map[string]*ContainerEntity
	containerEntitiesMap = cb.nodeRepository.GetContainerEntities()
	cb.agent = cb.nodeRepository.agentEntity.node

	for _, containerEntity := range containerEntitiesMap {
		task := containerEntity.task
		if task == nil {
			cb.errorCollector.Collect(fmt.Errorf("Null task object for entity %s", containerEntity.GetId()))
			continue
		}
		// skip non running tasks
		if task.State != "TASK_RUNNING" {
			cb.errorCollector.Collect(fmt.Errorf("Task object is not running %s", containerEntity.GetId()))
			continue
		}

		// Commodities sold
		commoditiesSoldContainer := cb.containerCommsSold(containerEntity)
		// Container Entity for the task
		entityDTOBuilder := cb.buildContainerEntityDTO(task, commoditiesSoldContainer)
		// Commodities bought
		entityDTOBuilder = cb.containerCommoditiesBought(entityDTOBuilder, containerEntity)

		// Entity DTO
		entityDTO, err := entityDTOBuilder.Create()
		cb.errorCollector.Collect(err)

		result = append(result, entityDTO)
	}
	glog.V(4).Infof("[BuildEntities] Container DTOs :", result)

	var collectedErrors error
	if cb.errorCollector.Count() > 0 {
		fmt.Printf("All errors %s\n", cb.errorCollector)
		collectedErrors = fmt.Errorf("Container entity builder errors: %s", cb.errorCollector)
	}
	return result, collectedErrors
}

// Build Container entityDTO
func (cb *ContainerEntityBuilder) buildContainerEntityDTO(task *data.Task, commoditiesSold []*proto.CommodityDTO) *builder.EntityDTOBuilder {
	id := task.Id
	dispName := task.Name

	entityDTOBuilder := builder.NewEntityDTOBuilder(proto.EntityDTO_CONTAINER, id).
		DisplayName(dispName)

	slaveId := task.SlaveId
	if slaveId == "" {
		cb.errorCollector.Collect(fmt.Errorf("Cannot find the hosting slave for task %s", dispName))
		return nil
	}
	glog.V(3).Infof("Container %s is hosted on %s", dispName, slaveId)

	if commoditiesSold != nil {
		entityDTOBuilder.SellsCommodities(commoditiesSold)
	}

	ipAddress := cb.agent.IP
	ipPropName := supplychain.SUPPLY_CHAIN_CONSTANT_IP_ADDRESS
	ipProp := &proto.EntityDTO_EntityProperty{ // TODO: create Property Builder in SDK
		Namespace: &DEFAULT_NAMESPACE,
		Name:      &ipPropName,
		Value:     &ipAddress,
	}
	entityDTOBuilder = entityDTOBuilder.WithProperty(ipProp)
	glog.V(3).Infof("Container %s will be stitched to VM with IP %s", dispName, ipAddress)

	return entityDTOBuilder
}

// Build commodityDTOs for commodity sold by the container
func (cb *ContainerEntityBuilder) containerCommsSold(containerEntity *ContainerEntity) []*proto.CommodityDTO {
	if containerEntity == nil || containerEntity.task == nil {
		cb.errorCollector.Collect(fmt.Errorf("Null task while creating sold commodities"))
		return nil
	}
	task := containerEntity.task

	var commoditiesSold []*proto.CommodityDTO

	vMemCommBuilder := builder.NewCommodityDTOBuilder(proto.CommodityDTO_VMEM)
	vCpuCommBuilder := builder.NewCommodityDTOBuilder(proto.CommodityDTO_VCPU)

	// VMem
	memCap := getEntityMetricValue(containerEntity, data.MEM, data.CAP, cb.errorCollector)
	memUsed := getEntityMetricValue(containerEntity, data.MEM, data.USED, cb.errorCollector)
	vMemCommBuilder.
		Capacity(*memCap).Used(*memUsed)

	// VCpu
	cpuCap := getEntityMetricValue(containerEntity, data.CPU, data.CAP, cb.errorCollector)
	cpuUsed := getEntityMetricValue(containerEntity, data.CPU, data.USED, cb.errorCollector)
	vCpuCommBuilder.Capacity(*cpuCap).Used(*cpuUsed)

	vMemComm, err := vMemCommBuilder.Create()
	cb.errorCollector.Collect(err)
	commoditiesSold = append(commoditiesSold, vMemComm)

	vCpuComm, err := vCpuCommBuilder.Create()
	cb.errorCollector.Collect(err)
	commoditiesSold = append(commoditiesSold, vCpuComm)

	// Application with task id as the key
	applicationComm, err := builder.NewCommodityDTOBuilder(proto.CommodityDTO_APPLICATION).
		Key(task.Id).
		Create()
	cb.errorCollector.Collect(err)
	commoditiesSold = append(commoditiesSold, applicationComm)

	return commoditiesSold
}

// Build commodityDTOs for commodity sold by the container
func (cb *ContainerEntityBuilder) containerCommoditiesBought(containerDto *builder.EntityDTOBuilder, containerEntity *ContainerEntity) *builder.EntityDTOBuilder {
	task := containerEntity.task

	var commoditiesBought []*proto.CommodityDTO

	memProvCommBuilder := builder.NewCommodityDTOBuilder(proto.CommodityDTO_MEM_PROVISIONED)
	cpuProvCommBuilder := builder.NewCommodityDTOBuilder(proto.CommodityDTO_CPU_PROVISIONED)
	vMemCommBuilder := builder.NewCommodityDTOBuilder(proto.CommodityDTO_VMEM)
	vCpuCommBuilder := builder.NewCommodityDTOBuilder(proto.CommodityDTO_VCPU)

	// MemProv
	memProvUsed := getEntityMetricValue(containerEntity, data.MEM_PROV, data.USED, cb.errorCollector)
	memProvCommBuilder.Used(*memProvUsed)

	// CpuProv
	cpuProvUsed := getEntityMetricValue(containerEntity, data.CPU_PROV, data.USED, cb.errorCollector)
	cpuProvCommBuilder.Used(*cpuProvUsed)

	// VMem
	memUsed := getEntityMetricValue(containerEntity, data.MEM, data.USED, cb.errorCollector)
	vMemCommBuilder.Used(*memUsed)

	// VCpu
	cpuUsed := getEntityMetricValue(containerEntity, data.CPU, data.USED, cb.errorCollector)
	vCpuCommBuilder.Used(*cpuUsed)

	memProvComm, err := memProvCommBuilder.Create()
	cb.errorCollector.Collect(err)
	commoditiesBought = append(commoditiesBought, memProvComm)

	cpuProvComm, err := cpuProvCommBuilder.Create()
	cb.errorCollector.Collect(err)
	commoditiesBought = append(commoditiesBought, cpuProvComm)

	vMemComm, err := vMemCommBuilder.Create()
	cb.errorCollector.Collect(err)
	commoditiesBought = append(commoditiesBought, vMemComm)

	vCpuComm, err := vCpuCommBuilder.Create()
	cb.errorCollector.Collect(err)
	commoditiesBought = append(commoditiesBought, vCpuComm)

	// Cluster
	clusterCommBought, err := builder.NewCommodityDTOBuilder(proto.CommodityDTO_CLUSTER).
		Key(cb.agent.ClusterName).
		Create()
	cb.errorCollector.Collect(err)
	commoditiesBought = append(commoditiesBought, clusterCommBought)

	providerDto := builder.CreateProvider(proto.EntityDTO_VIRTUAL_MACHINE, task.SlaveId)
	containerDto.Provider(providerDto)
	containerDto.BuysCommodities(commoditiesBought)

	return containerDto
}
