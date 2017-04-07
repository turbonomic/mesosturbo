package discovery

import (
	"fmt"
	"github.com/golang/glog"
	"github.com/turbonomic/turbo-go-sdk/pkg/proto"
	"github.com/turbonomic/turbo-go-sdk/pkg/builder"
	"github.com/turbonomic/turbo-go-sdk/pkg/supplychain"
	"github.com/turbonomic/mesosturbo/pkg/data"
)

// Builder for creating Container Entities to represent the default container Mesos Tasks in Turbo server
type ContainerEntityBuilder struct {
	mesosMaster         *data.MesosMaster
	containerMetricsMap map[string]*data.EntityResourceMetrics
	builderError        []error
}

// Build Container EntityDTO using the tasks listed in 'state' json returned from the Mesos Master
func (cb *ContainerEntityBuilder) BuildEntities() ([]*proto.EntityDTO, error) {
	glog.V(2).Infof("[BuildEntities] ....")
	cb.builderError  = []error{}
	result := []*proto.EntityDTO{}

	taskMap := cb.mesosMaster.TaskMap
	for _, task := range taskMap {
		// skip non running tasks
		if task.State != "TASK_RUNNING" {
			continue
		}
		containerMetrics := cb.getContainerMetrics(task)

		commoditiesSoldContainer := cb.containerCommsSold(task, containerMetrics)
		entityDTOBuilder, _ := cb.buildContainerEntityDTO(task, commoditiesSoldContainer)
		entityDTOBuilder = cb.containerCommoditiesBought(entityDTOBuilder, task, containerMetrics)
		entityDTO, err := entityDTOBuilder.Create()
		if err != nil {
			cb.builderError = append(cb.builderError, err)
		}
		result = append(result, entityDTO)
	}
	glog.V(4).Infof("[BuildEntities] Container DTOs :", result)
	return result, fmt.Errorf("VM entity builder errors: %+v", cb.builderError)
}

func (cb *ContainerEntityBuilder) getContainerMetrics(task *data.Task) *data.EntityResourceMetrics {
	containerMetrics, ok := cb.containerMetricsMap[task.Id]
	if !ok {
		return nil
	}
	return containerMetrics
}


// Build Container entityDTO
func (cb *ContainerEntityBuilder) buildContainerEntityDTO(task *data.Task, commoditiesSold []*proto.CommodityDTO) (*builder.EntityDTOBuilder, error) {
	id := task.Id
	dispName := task.Name

	entityDTOBuilder := builder.NewEntityDTOBuilder(proto.EntityDTO_CONTAINER, id).
					DisplayName(dispName)

	slaveId := task.SlaveId
	if slaveId == "" {
		cb.builderError = append(cb.builderError, fmt.Errorf("Cannot find the hosting slave for task %s", dispName))
		return nil, fmt.Errorf("Cannot find the hosting slave for task %s", dispName)
	}
	glog.V(2).Infof("Pod %s is hosted on %s", dispName, slaveId)

	if commoditiesSold != nil {
		entityDTOBuilder.SellsCommodities(commoditiesSold)
	}

	////	providerUid := nodeUidTranslationMap[slaveId]
	agentMap := cb.mesosMaster.AgentMap
	agent := agentMap[task.SlaveId]
	ipAddress := agent.IP
	ipPropName := supplychain.SUPPLY_CHAIN_CONSTANT_IP_ADDRESS
	ipProp := &proto.EntityDTO_EntityProperty {	// TODO: create Property Builder
		Namespace: &DEFAULT_NAMESPACE,
		Name: &ipPropName,
		Value: &ipAddress,
	}
	entityDTOBuilder = entityDTOBuilder.WithProperty(ipProp)
	glog.V(2).Infof("Pod %s will be stitched to VM with IP %s", dispName, ipAddress)

	return entityDTOBuilder, nil
}

// Build commodityDTOs for commodity sold by the container
func  (cb *ContainerEntityBuilder) containerCommsSold(task *data.Task, containerMetrics  *data.EntityResourceMetrics) []*proto.CommodityDTO {
	if task == nil {
		cb.builderError = append(cb.builderError, fmt.Errorf("Null task while creating sold commodities"))
		return nil
	}
	var commoditiesSold []*proto.CommodityDTO
	vMemCommBuilder := builder.NewCommodityDTOBuilder(proto.CommodityDTO_VMEM)
	vCpuCommBuilder := builder.NewCommodityDTOBuilder(proto.CommodityDTO_VCPU)

	if containerMetrics == nil {
		// VMem
		memCap := getValue(containerMetrics, data.MEM, data.CAP)
		memUsed := getValue(containerMetrics, data.MEM, data.USED)
		vMemCommBuilder.
			Capacity(*memCap).Used(*memUsed);

		// VCpu
		cpuCap := getValue(containerMetrics, data.CPU, data.CAP)
		cpuUsed := getValue(containerMetrics, data.CPU, data.USED)
		vCpuCommBuilder.
			Capacity(*cpuCap).Used(*cpuUsed);
	} else {
		cb.builderError = append(cb.builderError, fmt.Errorf("Null metrics for container %s", task.Id))
	}

	vMemComm, err := vMemCommBuilder.Create()
	if err != nil {
		cb.builderError = append(cb.builderError, err)
	}
	commoditiesSold = append(commoditiesSold, vMemComm)
	vCpuComm, err := vCpuCommBuilder.Create()
	if err != nil {
		cb.builderError = append(cb.builderError, err)
	}
	commoditiesSold = append(commoditiesSold, vCpuComm)

	// Application with task id as the key
	applicationComm, err := builder.NewCommodityDTOBuilder(proto.CommodityDTO_APPLICATION).
					Key(task.Id).
					Create()
	if err != nil {
		cb.builderError = append(cb.builderError, err)
	}
	commoditiesSold = append(commoditiesSold, applicationComm)
	return commoditiesSold
}


// Build commodityDTOs for commodity sold by the container
func (cb *ContainerEntityBuilder) containerCommoditiesBought(containerDto *builder.EntityDTOBuilder, task *data.Task,
							containerMetrics *data.EntityResourceMetrics) *builder.EntityDTOBuilder {
	var commoditiesBought []*proto.CommodityDTO

	memProvCommBuilder := builder.NewCommodityDTOBuilder(proto.CommodityDTO_MEM_PROVISIONED)
	cpuProvCommBuilder := builder.NewCommodityDTOBuilder(proto.CommodityDTO_CPU_PROVISIONED)
	vMemCommBuilder := builder.NewCommodityDTOBuilder(proto.CommodityDTO_VMEM)
	vCpuCommBuilder := builder.NewCommodityDTOBuilder(proto.CommodityDTO_VCPU)
	if containerMetrics != nil {
		// MemProv
		memProvUsed := getValue(containerMetrics, data.MEM_PROV, data.USED)
		memProvCommBuilder.
			Used(*memProvUsed)

		// CpuProv
		cpuProvUsed := getValue(containerMetrics, data.CPU_PROV, data.USED)
		cpuProvCommBuilder.
			Used(*cpuProvUsed)

		// VMem
		memUsed := getValue(containerMetrics, data.MEM, data.USED)
		vMemCommBuilder.
			Used(*memUsed)

		// VCpu
		cpuUsed := getValue(containerMetrics, data.CPU, data.USED)
		vCpuCommBuilder.
			Used(*cpuUsed)
	} else {
		cb.builderError = append(cb.builderError, fmt.Errorf("Null metrics for container %s", task.Id))
	}

	memProvComm, err := memProvCommBuilder.Create()
	if err != nil {
		cb.builderError = append(cb.builderError, err)
	}
	commoditiesBought = append(commoditiesBought, memProvComm)
	cpuProvComm, err := cpuProvCommBuilder.Create()
	if err != nil {
		cb.builderError = append(cb.builderError, err)
	}
	commoditiesBought = append(commoditiesBought, cpuProvComm)
	vMemComm, err := vMemCommBuilder.Create()
	if err != nil {
		cb.builderError = append(cb.builderError, err)
	}
	commoditiesBought = append(commoditiesBought, vMemComm)
	vCpuComm, err := vCpuCommBuilder.Create()
	if err != nil {
		cb.builderError = append(cb.builderError, err)
	}
	commoditiesBought = append(commoditiesBought, vCpuComm)

	// Cluster
	clusterCommBought, err := builder.NewCommodityDTOBuilder(proto.CommodityDTO_CLUSTER).
					Key(cb.mesosMaster.Cluster.ClusterName).
					Create()
	if err != nil {
		cb.builderError = append(cb.builderError, err)
	}
	commoditiesBought = append(commoditiesBought, clusterCommBought)

	providerDto := builder.CreateProvider(proto.EntityDTO_VIRTUAL_MACHINE, task.SlaveId)
	containerDto.Provider(providerDto)
	containerDto.BuysCommodities(commoditiesBought)

	return containerDto
}