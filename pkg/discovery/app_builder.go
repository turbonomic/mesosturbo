package discovery

import (
	"fmt"
	"github.com/golang/glog"
	"github.com/turbonomic/mesosturbo/pkg/data"
	"github.com/turbonomic/turbo-go-sdk/pkg/builder"
	"github.com/turbonomic/turbo-go-sdk/pkg/proto"
	"strings"
)

var (
	APP_ENTITY_PREFIX string = "APP-"
)

// Builder for creating Application Entities to represent the Mesos Tasks in Turbo server
type AppEntityBuilder struct {
	nodeRepository *NodeRepository
	errorCollector *ErrorCollector
}

// Build Application EntityDTO using the tasks listed in the 'state' json returned from the Mesos Master
func (tb *AppEntityBuilder) BuildEntities() ([]*proto.EntityDTO, error) {
	tb.errorCollector = new(ErrorCollector)
	glog.V(2).Infof("[BuildEntities] ...... ")
	result := []*proto.EntityDTO{}

	var taskEntitiesMap map[string]*TaskEntity
	taskEntitiesMap = tb.nodeRepository.GetTaskEntities()
	for _, taskEntity := range taskEntitiesMap {
		task := taskEntity.task
		if task == nil {
			tb.errorCollector.Collect(fmt.Errorf("Null task object for entity %s", taskEntity.GetId()))
			continue
		}
		if task.State != "TASK_RUNNING" {
			tb.errorCollector.Collect(fmt.Errorf("Task object is not running %s", taskEntity.GetId()))
			continue
		}

		// Commodities sold
		commoditiesSoldApp := tb.appCommsSold(task)
		// Application Entity for the task
		entityDTOBuilder := tb.appEntityDTO(task, commoditiesSoldApp)
		// Commodities bought
		entityDTOBuilder = tb.appCommoditiesBought(entityDTOBuilder, taskEntity)
		// Entity DTO
		entityDTO, err := entityDTOBuilder.Create()
		tb.errorCollector.Collect(err)

		result = append(result, entityDTO)
	}
	glog.V(4).Infof("[BuildEntities] App DTOs :", result)

	var collectedErrors error
	if tb.errorCollector.Count() > 0 {
		collectedErrors = fmt.Errorf("Application entity builder errors: %+v", tb.errorCollector)
	}
	return result, collectedErrors
}

// Build Application DTO
func (tb *AppEntityBuilder) appEntityDTO(task *data.Task, commoditiesSold []*proto.CommodityDTO) *builder.EntityDTOBuilder {
	appEntityType := proto.EntityDTO_APPLICATION
	id := strings.Join([]string{APP_ENTITY_PREFIX, task.Name, "-", task.Id}, "")
	dispName := strings.Join([]string{APP_ENTITY_PREFIX, task.Name}, "")
	entityDTOBuilder := builder.NewEntityDTOBuilder(appEntityType, id).
		DisplayName(dispName).
		SellsCommodities(commoditiesSold)

	return entityDTOBuilder
}

// Build commodityDTOs for commodity sold by the app
func (tb *AppEntityBuilder) appCommsSold(task *data.Task) []*proto.CommodityDTO {

	var commoditiesSold []*proto.CommodityDTO
	transactionComm, err := builder.NewCommodityDTOBuilder(proto.CommodityDTO_TRANSACTION).
		Key(task.Name).
		Create()
	tb.errorCollector.Collect(err)
	commoditiesSold = append(commoditiesSold, transactionComm)
	return commoditiesSold
}

// Build commodityDTOs for commodity bought by the app
func (tb *AppEntityBuilder) appCommoditiesBought(appDto *builder.EntityDTOBuilder, taskEntity *TaskEntity) *builder.EntityDTOBuilder {
	if taskEntity == nil || taskEntity.task == nil {
		tb.errorCollector.Collect(fmt.Errorf("Null task while creating sold commodities"))
		return nil
	}

	task := taskEntity.task

	var commoditiesBought []*proto.CommodityDTO
	vMemCommBuilder := builder.NewCommodityDTOBuilder(proto.CommodityDTO_VMEM)
	vCpuCommBuilder := builder.NewCommodityDTOBuilder(proto.CommodityDTO_VCPU)
	// VMem
	memUsed := getEntityMetricValue(taskEntity, data.MEM, data.USED, tb.errorCollector)
	vMemCommBuilder.Used(*memUsed)

	// VCpu
	cpuUsed := getEntityMetricValue(taskEntity, data.CPU, data.USED, tb.errorCollector)
	vCpuCommBuilder.Used(*cpuUsed)

	vMemComm, err := vMemCommBuilder.Create()
	tb.errorCollector.Collect(err)
	commoditiesBought = append(commoditiesBought, vMemComm)

	vCpuComm, err := vCpuCommBuilder.Create()
	tb.errorCollector.Collect(err)
	commoditiesBought = append(commoditiesBought, vCpuComm)

	// Application commodity
	applicationComm, err := builder.NewCommodityDTOBuilder(proto.CommodityDTO_APPLICATION).
		Key(task.Id).
		Create()
	tb.errorCollector.Collect(err)

	commoditiesBought = append(commoditiesBought, applicationComm)

	// From Container
	containerName := task.Id
	containerProvider := builder.CreateProvider(proto.EntityDTO_CONTAINER, containerName)
	appDto.Provider(containerProvider)
	appDto.BuysCommodities(commoditiesBought)

	return appDto
}
