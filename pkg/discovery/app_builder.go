package discovery

import (

	"github.com/turbonomic/mesosturbo/pkg/data"
	"github.com/turbonomic/turbo-go-sdk/pkg/proto"
	"github.com/turbonomic/turbo-go-sdk/pkg/builder"
	"github.com/golang/glog"
	"fmt"
	"strings"
)


var (
	APP_ENTITY_PREFIX string = "APP-"
)

// Builder for creating Application Entities to represent the Mesos Tasks in Turbo server
type AppEntityBuilder struct {
	mesosMaster    *data.MesosMaster
	taskMetricsMap map[string]*data.EntityResourceMetrics
	builderError   []error
}



// Build Application EntityDTO using the tasks listed in the 'state' json returned from the Mesos Master
func (tb *AppEntityBuilder) BuildEntities() ([]*proto.EntityDTO, error) {
	glog.V(2).Infof("[BuildEntities] ...... ")
	result := []*proto.EntityDTO{}
	tb.builderError = []error{}

	taskMap:= tb.mesosMaster.TaskMap
	for _, task := range taskMap {
		if task.State != "TASK_RUNNING" {
			continue
		}
		appMetrics := tb.getTaskMetrics(task)

		// Commodities sold
		commoditiesSoldApp := tb.appCommsSold(task)
		// Application Entity for the task
		entityDTOBuilder := tb.appEntityDTO(task, commoditiesSoldApp)
		// Commodities bought
		entityDTOBuilder = tb.appCommoditiesBought(entityDTOBuilder, task, appMetrics)
		// Entity DTO
		entityDTO, err := entityDTOBuilder.Create()
		if err != nil {
			tb.builderError = append(tb.builderError, err)
		}
		result = append(result, entityDTO)
	}
	glog.V(4).Infof("[BuildEntities] App DTOs :", result)
	return result, fmt.Errorf("VM entity builder errors: %+v", tb.builderError)
}

func (tb *AppEntityBuilder) getTaskMetrics(task *data.Task) *data.EntityResourceMetrics {
	appMetrics, ok := tb.taskMetricsMap[task.Id]
	if !ok {
		return nil
	}
	return appMetrics
}
// Build Application DTO
func (tb *AppEntityBuilder) appEntityDTO(task *data.Task, commoditiesSold []*proto.CommodityDTO) *builder.EntityDTOBuilder {
	appEntityType := proto.EntityDTO_APPLICATION
	id := strings.Join([]string{APP_ENTITY_PREFIX, task.Name, "-", task.Id}, "")
	dispName := strings.Join([]string{APP_ENTITY_PREFIX}, task.Name)
	entityDTOBuilder := builder.NewEntityDTOBuilder(appEntityType, id).
					DisplayName(dispName).
					SellsCommodities(commoditiesSold)

	return entityDTOBuilder
}


// Build commodityDTOs for commodity sold by the app
func  (tb *AppEntityBuilder) appCommsSold(task *data.Task) []*proto.CommodityDTO {

	var commoditiesSold []*proto.CommodityDTO
	transactionComm, err := builder.NewCommodityDTOBuilder(proto.CommodityDTO_TRANSACTION).
					Key(task.Name).
					Create()
	if err != nil {
		tb.builderError = append(tb.builderError, err)
	}
	commoditiesSold = append(commoditiesSold, transactionComm)
	return commoditiesSold
}

// Build commodityDTOs for commodity bought by the app
func (tb *AppEntityBuilder) appCommoditiesBought(appDto *builder.EntityDTOBuilder, task *data.Task, appMetrics *data.EntityResourceMetrics) *builder.EntityDTOBuilder {
	if task == nil {
		tb.builderError = append(tb.builderError, fmt.Errorf("Null task while creating sold commodities"))
		return nil
	}

	var commoditiesBought []*proto.CommodityDTO
	vMemCommBuilder := builder.NewCommodityDTOBuilder(proto.CommodityDTO_VMEM)
	vCpuCommBuilder := builder.NewCommodityDTOBuilder(proto.CommodityDTO_VCPU)
	if appMetrics != nil {
		// VMem
		memUsed := getValue(appMetrics, data.MEM, data.USED)
		vMemCommBuilder.
			Used(*memUsed)

		// VCpu
		cpuUsed := getValue(appMetrics, data.CPU, data.USED)
		vCpuCommBuilder.
			Used(*cpuUsed)
	} else {
		tb.builderError = append(tb.builderError, fmt.Errorf("Null metrics for task %s", task.Id))
	}

	vMemComm, err := vMemCommBuilder.Create()
	commoditiesBought = append(commoditiesBought, vMemComm)
	if err != nil {
		tb.builderError = append(tb.builderError, err)
	}

	vCpuComm, err := vCpuCommBuilder.Create()
	commoditiesBought = append(commoditiesBought, vCpuComm)
	if err != nil {
		tb.builderError = append(tb.builderError, err)
	}

	// Application commodity
	applicationComm, err := builder.NewCommodityDTOBuilder(proto.CommodityDTO_APPLICATION).
				Key(task.Id).
				Create()
	if err != nil {
		tb.builderError = append(tb.builderError, err)
	}
	commoditiesBought = append(commoditiesBought, applicationComm)

	// From Container
	containerName := task.Id
	containerProvider := builder.CreateProvider(proto.EntityDTO_CONTAINER, containerName)
	appDto.Provider(containerProvider)
	appDto.BuysCommodities(commoditiesBought)

	return appDto
}
