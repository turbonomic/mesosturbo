package discovery

import (
	"fmt"
	"github.com/golang/glog"
	"github.com/turbonomic/mesosturbo/pkg/data"
	"github.com/turbonomic/turbo-go-sdk/pkg/proto"
)

// =======================================================================
type MONITOR_NAME string

const (
	DEFAULT_MESOS    MONITOR_NAME = "DEFAULT_MESOS"
	PROMETHEUS_MESOS MONITOR_NAME = "PROMETHEUS_MESOS"
)

type ENTITY_ID string

// MonitoringProperty list for a MesosEntity
type EntityMonitoringProps struct {
	entityId string
	propMap  map[PropKey]*MonitoringProperty
}

// Key struct using resource and metric type to organize the monitoring property for an entity instance
type PropKey string

var (
	CPU_CAP       PropKey = NewPropKey(data.CPU, data.CAP)
	CPU_USED      PropKey = NewPropKey(data.CPU, data.USED)
	MEM_CAP       PropKey = NewPropKey(data.MEM, data.CAP)
	MEM_USED      PropKey = NewPropKey(data.MEM, data.USED)
	CPU_PROV_CAP  PropKey = NewPropKey(data.CPU_PROV, data.CAP)
	CPU_PROV_USED PropKey = NewPropKey(data.CPU_PROV, data.USED)
	MEM_PROV_CAP  PropKey = NewPropKey(data.MEM_PROV, data.CAP)
	MEM_PROV_USED PropKey = NewPropKey(data.MEM_PROV, data.USED)
)

func NewPropKey(resourceType data.ResourceType, metricType data.MetricPropType) PropKey {
	propKey := PropKey(fmt.Sprintf(string(resourceType) + "-" + string(metricType)))
	return propKey
}

// ====================================================================================================================
// Metadata for the metric to monitored
type MetricDef struct {
	entityType   data.EntityType
	resourceType data.ResourceType
	metricType   data.MetricPropType
	metricSetter MetricSetter // Setter for the property
	// TODO: monitorSpec - spec used to poll for the property
}

//
type MonitoringProperty struct {
	metricDef *MetricDef
	id        string
}

// Object responsible for setting the value for a metric property
type MetricSetter interface {
	SetName(name string)
	SetMetricValue(entity MesosEntity, value *float64)
}

// Object that will fetch values for the given monitoring properties for all the entities in the repository
// by connecting to the target
type Monitor interface {
	GetSourceName() MONITOR_NAME
	Monitor(target *MonitorTarget) error
}

type MonitorTarget struct {
	targetId        string
	config          interface{}
	repository      Repository
	rawStatsCache   *RawStatsCache
	monitoringProps map[ENTITY_ID]*EntityMonitoringProps
}

// MetricStore is responsible for collecting values for different metrics for various resources belonging
// to the entities in the given probe repository.
// It is configured with a set of Monitors responsible for collecting the data values for the metrics.
// Applications invoke the GetMetrics() to trigger the data collection.
type MetricsMetadataStore interface {
	GetMetricDefs() map[data.EntityType]map[data.ResourceType]map[data.MetricPropType]*MetricDef
}

// =========================== MetricSetter Implementation ============================================

type DefaultMetricSetter struct {
	entityType   data.EntityType
	resourceType data.ResourceType
	metricType   data.MetricPropType
	name         string
}

func (setter *DefaultMetricSetter) SetMetricValue(entity MesosEntity, value *float64) {
	//fmt.Printf("Setter : %s %+v\n", &setter, setter)
	if convertEntityType(setter.entityType) != entity.GetType() {
		glog.Errorf("Invalid entity type %s, required %s", entity.GetType(), setter.entityType)
	}
	var entityMetrics MetricMap
	entityMetrics = entity.GetResourceMetrics()
	if entityMetrics == nil {
		glog.Errorf("Nil entity metrics for %s::%s", entity.GetType(), entity.GetId())
	}
	entityMetrics.SetResourceMetric(setter.resourceType, setter.metricType, value)
}

func (setter *DefaultMetricSetter) SetName(name string) {
	setter.name = name
}

// ============================== MetricStore Implementation =========================================

type MetricDefMap map[data.EntityType]map[data.ResourceType]map[data.MetricPropType]*MetricDef

func NewMetricDefMap() MetricDefMap {
	return make(map[data.EntityType]map[data.ResourceType]map[data.MetricPropType]*MetricDef)
}
func (mdm MetricDefMap) Put(et data.EntityType, rt data.ResourceType, mt data.MetricPropType, md *MetricDef) {
	resourceMap, ok := mdm[et]
	if !ok {
		mdm[et] = make(map[data.ResourceType]map[data.MetricPropType]*MetricDef)
	}
	resourceMap = mdm[et]

	metricMap, ok := resourceMap[rt]
	if !ok {
		resourceMap[rt] = make(map[data.MetricPropType]*MetricDef)
	}
	metricMap = resourceMap[rt]

	metricMap[mt] = md
}

func (mdm MetricDefMap) Get(et data.EntityType, rt data.ResourceType, mt data.MetricPropType) *MetricDef {
	resourceMap, ok := mdm[et]
	if !ok {
		return nil
	}
	resourceMap = mdm[et]

	metricMap, ok := resourceMap[rt]
	if !ok {
		return nil
	}
	return metricMap[mt]
}

// Implementation for the MetricsStore for Mesos target
type MesosMetricsMetadataStore struct {
	metricDefMap map[data.EntityType]map[data.ResourceType]map[data.MetricPropType]*MetricDef
}

func NewMesosMetricsMetadataStore() *MesosMetricsMetadataStore {
	mc := &MesosMetricsMetadataStore{}

	// TODO: parse from a file
	// read from a config file a table of entity type, resource type, metric type to be discovered
	var mdMap map[data.EntityType]map[data.ResourceType]map[data.MetricPropType]*MetricDef
	mdMap = make(map[data.EntityType]map[data.ResourceType]map[data.MetricPropType]*MetricDef)

	mdMap[data.NODE] = make(map[data.ResourceType]map[data.MetricPropType]*MetricDef)
	resourceMap := mdMap[data.NODE]
	addDefaultMetricDef(data.NODE, data.CPU, data.CAP, resourceMap)
	addDefaultMetricDef(data.NODE, data.MEM, data.CAP, resourceMap)
	addDefaultMetricDef(data.NODE, data.CPU, data.USED, resourceMap)
	addDefaultMetricDef(data.NODE, data.MEM, data.USED, resourceMap)
	addDefaultMetricDef(data.NODE, data.CPU_PROV, data.CAP, resourceMap)
	addDefaultMetricDef(data.NODE, data.CPU_PROV, data.USED, resourceMap)
	addDefaultMetricDef(data.NODE, data.MEM_PROV, data.CAP, resourceMap)
	addDefaultMetricDef(data.NODE, data.MEM_PROV, data.USED, resourceMap)

	mdMap[data.CONTAINER] = make(map[data.ResourceType]map[data.MetricPropType]*MetricDef)
	resourceMap = mdMap[data.CONTAINER]
	addDefaultMetricDef(data.CONTAINER, data.CPU, data.CAP, resourceMap)
	addDefaultMetricDef(data.CONTAINER, data.CPU, data.USED, resourceMap)
	addDefaultMetricDef(data.CONTAINER, data.MEM, data.CAP, resourceMap)
	addDefaultMetricDef(data.CONTAINER, data.MEM, data.USED, resourceMap)
	addDefaultMetricDef(data.CONTAINER, data.CPU_PROV, data.CAP, resourceMap)
	addDefaultMetricDef(data.CONTAINER, data.CPU_PROV, data.USED, resourceMap)
	addDefaultMetricDef(data.CONTAINER, data.MEM_PROV, data.CAP, resourceMap)
	addDefaultMetricDef(data.CONTAINER, data.MEM_PROV, data.USED, resourceMap)

	mdMap[data.APP] = make(map[data.ResourceType]map[data.MetricPropType]*MetricDef)
	resourceMap = mdMap[data.APP]
	addDefaultMetricDef(data.APP, data.CPU, data.CAP, resourceMap)
	addDefaultMetricDef(data.APP, data.CPU, data.USED, resourceMap)
	addDefaultMetricDef(data.APP, data.MEM, data.CAP, resourceMap)
	addDefaultMetricDef(data.APP, data.MEM, data.USED, resourceMap)
	addDefaultMetricDef(data.APP, data.CPU_PROV, data.CAP, resourceMap)
	addDefaultMetricDef(data.APP, data.CPU_PROV, data.USED, resourceMap)
	addDefaultMetricDef(data.APP, data.MEM_PROV, data.CAP, resourceMap)
	addDefaultMetricDef(data.APP, data.MEM_PROV, data.USED, resourceMap)

	mc.metricDefMap = mdMap

	return mc
}

func addDefaultMetricDef(entityType data.EntityType, resourceType data.ResourceType, metricType data.MetricPropType,
	resourceMap map[data.ResourceType]map[data.MetricPropType]*MetricDef) {
	metricMap, ok := resourceMap[resourceType]
	if !ok {
		resourceMap[resourceType] = make(map[data.MetricPropType]*MetricDef)
	}
	metricMap = resourceMap[resourceType]

	metricSetter := &DefaultMetricSetter{
		entityType:   entityType,
		resourceType: resourceType,
		metricType:   metricType}
	metricSetter.SetName(fmt.Sprintf("%s:%s:%s:%s", &metricSetter, entityType, resourceType, metricType))

	metricDef := &MetricDef{
		entityType:   entityType,
		resourceType: resourceType,
		metricType:   metricType,
		metricSetter: metricSetter,
	}
	metricMap[metricType] = metricDef
}

func (metricStore *MesosMetricsMetadataStore) GetMetricDefs() map[data.EntityType]map[data.ResourceType]map[data.MetricPropType]*MetricDef {
	return metricStore.metricDefMap
}

func createMonitoringProps(repository Repository, mdMap map[data.EntityType]map[data.ResourceType]map[data.MetricPropType]*MetricDef) map[ENTITY_ID]*EntityMonitoringProps {
	// Create monitoring property for the repository entities using the MetricDef configured for each entity type
	var entityPropsMap map[ENTITY_ID]*EntityMonitoringProps
	entityPropsMap = make(map[ENTITY_ID]*EntityMonitoringProps)

	for entityType, resourceMap := range mdMap {
		// entity instances
		entityList := repository.GetEntityInstances(convertEntityType(entityType))
		for idx, _ := range entityList {
			entity := entityList[idx]
			entityId := entity.GetId()
			// monitoring properties of an entity instance
			entityProps := &EntityMonitoringProps{
				entityId: entityId,
				propMap:  make(map[PropKey]*MonitoringProperty),
			}
			entityPropsMap[ENTITY_ID(entityId)] = entityProps
			tempMap := entityProps.propMap
			for _, metricMap := range resourceMap {
				for _, metricDef := range metricMap {
					glog.V(4).Infof("MetricDef %s --->  %s::%s::%s\n", entityId, metricDef.entityType, metricDef.resourceType, metricDef.metricType)
					prop := &MonitoringProperty{
						metricDef: metricDef,
						id:        entityId,
					}
					tempMap[NewPropKey(metricDef.resourceType, metricDef.metricType)] = prop
				}
			}
		}
	}
	return entityPropsMap
}

func convertEntityType(entityType data.EntityType) proto.EntityDTO_EntityType {
	if entityType == data.NODE {
		return proto.EntityDTO_VIRTUAL_MACHINE
	} else if entityType == data.APP {
		return proto.EntityDTO_APPLICATION
	} else if entityType == data.CONTAINER {
		return proto.EntityDTO_CONTAINER
	}
	return proto.EntityDTO_UNKNOWN
}
