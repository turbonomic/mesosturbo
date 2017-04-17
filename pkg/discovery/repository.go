package discovery

import (
	"github.com/golang/glog"
	"github.com/turbonomic/mesosturbo/pkg/data"
	"github.com/turbonomic/turbo-go-sdk/pkg/proto"
	"strings"
	"fmt"
)

//// Object which holds the values for different metrics for all the resources of an entity
type MetricMap map[data.ResourceType]map[data.MetricPropType]*Metric

// Interface for Mesos Entity
type MesosEntity interface {
	GetId() string
	GetType() proto.EntityDTO_EntityType
	GetResourceMetrics() MetricMap
	GetResourceMetric(resourceType data.ResourceType, metricType data.MetricPropType) (*Metric, error)
}

// Interface for Mesos Repository
type Repository interface {
	GetEntity(entityType proto.EntityDTO_EntityType, id string) MesosEntity
	GetEntityInstances(entityType proto.EntityDTO_EntityType) []MesosEntity
}

type Metric struct {
	value *float64
}
//
//// Object which holds the values for different metrics for all the resources of an entity
//type MetricMap struct {
//	entityId	string
//	metricMap	map[data.ResourceType]map[data.MetricPropType]*Metric
//}

// =========================== Repository ========================================
// Object representing the local repository of a Mesos Node or Agent.
// It consists of an Agent entity that represents the Node. Tasks running on the node are represented using TaskEntity.
// There is also a corresponding ContainerEntity for each task.
type NodeRepository struct {
	agentEntity *AgentEntity
	taskEntities map[string]*TaskEntity
	containerEntities map[string]*ContainerEntity
}

// Create a new NodeRepository for the given Agent Id
func NewNodeRepository(agentId string) *NodeRepository {
	et := proto.EntityDTO_VIRTUAL_MACHINE
	agentEntity := &AgentEntity{
		entityType: et,
		id: agentId,
		metrics: make(map[data.ResourceType]map[data.MetricPropType]*Metric), //NewMetricMap(agentId),
	}
	return &NodeRepository{
		agentEntity: agentEntity,
		taskEntities: make(map[string]*TaskEntity),
		containerEntities: make(map[string]*ContainerEntity),
	}
}

const (
	APP_PREFIX string = "APP-"
	CONTAINER_PREFIX string = "POD-"
)

func (nodeRepos *NodeRepository) CreateTaskEntity(id string) *TaskEntity {
	et := proto.EntityDTO_APPLICATION
	taskId := GetRepositoryId(et, id)
	taskEntity := &TaskEntity{
		entityType: et,
		id: taskId,
		metrics: make(map[data.ResourceType]map[data.MetricPropType]*Metric), //NewMetricMap(id),
	}
	nodeRepos.taskEntities[taskId] = taskEntity
	return taskEntity
}

func (nodeRepos *NodeRepository) CreateContainerEntity(id string) *ContainerEntity {
	et := proto.EntityDTO_CONTAINER
	containerId := GetRepositoryId(et, id)
	containerEntity := &ContainerEntity{
		entityType: et,
		id: containerId,
		metrics: make(map[data.ResourceType]map[data.MetricPropType]*Metric), //NewMetricMap(id),
	}
	nodeRepos.containerEntities[containerId] = containerEntity
	return containerEntity
}

func (nodeRepos *NodeRepository) GetAgentEntity() *AgentEntity {
	return nodeRepos.agentEntity
}

func (nodeRepos *NodeRepository) GetTaskEntities() map[string]*TaskEntity {
	return nodeRepos.taskEntities
}

func (nodeRepos *NodeRepository) GetContainerEntities() map[string]*ContainerEntity {
	return nodeRepos.containerEntities
}

func (nodeRepos *NodeRepository) GetEntity(entityType proto.EntityDTO_EntityType, id string) MesosEntity {
	if (entityType == proto.EntityDTO_VIRTUAL_MACHINE && nodeRepos.agentEntity.GetId() == id) {
		return nodeRepos.agentEntity
	}
	if (entityType == proto.EntityDTO_APPLICATION) {
		taskId := GetRepositoryId(proto.EntityDTO_APPLICATION, id)
		return nodeRepos.taskEntities[taskId]
	}
	if (entityType == proto.EntityDTO_CONTAINER) {
		containerId := GetRepositoryId(proto.EntityDTO_CONTAINER, id)
		return nodeRepos.containerEntities[containerId]
	}
	glog.Errorf("Entity type not found %s::%s", entityType, id)
	return nil
}

func (nodeRepos *NodeRepository) GetEntityInstances(entityType proto.EntityDTO_EntityType) []MesosEntity {
	var entityList []MesosEntity
	if (entityType == proto.EntityDTO_VIRTUAL_MACHINE) {
		entityList = append(entityList, nodeRepos.agentEntity)
	}
	if (entityType == proto.EntityDTO_APPLICATION) {
		for _, val := range nodeRepos.taskEntities {
			entityList = append(entityList, val)
		}
	}
	if (entityType == proto.EntityDTO_CONTAINER) {
		for _, val := range nodeRepos.containerEntities {
			entityList = append(entityList, val)
		}
	}
	return entityList
}


// Convenience function to generate unique id's for a repository entity
func GetRepositoryId(entityType proto.EntityDTO_EntityType, id string) string {
	if entityType == proto.EntityDTO_APPLICATION {
		return strings.Join([]string{APP_PREFIX,  id}, "")
	} else if entityType == proto.EntityDTO_CONTAINER {
		return strings.Join([]string{CONTAINER_PREFIX, id}, "")
	}
	return id
}

// ===================================== Repository Entities ============================================
// Object representing the Agent in the Mesos environment
type AgentEntity struct {
	entityType proto.EntityDTO_EntityType
	id string
	metrics MetricMap
	node *data.Agent
}

func (agent *AgentEntity) GetId() string {
	return agent.id
}

func (agent *AgentEntity) GetType() proto.EntityDTO_EntityType {
	return agent.entityType
}

func (agent *AgentEntity) GetResourceMetrics() MetricMap {
	return agent.metrics
}

func (agent *AgentEntity) GetResourceMetric(resourceType data.ResourceType, metricType data.MetricPropType) (*Metric, error) {
	metric, err := agent.metrics.GetResourceMetric(resourceType, metricType)
	if err != nil {
		err = fmt.Errorf("%s : %s", agent.id, err)
	}
	return metric, err
}

// Object representing an Application running on an Agent in the Mesos environment
type TaskEntity struct {
	entityType proto.EntityDTO_EntityType
	id string
	metrics  MetricMap
	task *data.Task
}

func (task *TaskEntity) GetId() string {
	return task.id
}

func (task *TaskEntity) GetType() proto.EntityDTO_EntityType {
	return task.entityType
}

func (task *TaskEntity) GetResourceMetrics() MetricMap {
	return task.metrics
}


func (task *TaskEntity) GetResourceMetric(resourceType data.ResourceType, metricType data.MetricPropType) (*Metric, error) {
	metric, err := task.metrics.GetResourceMetric(resourceType, metricType)
	if err != nil {
		err = fmt.Errorf("%s : %s", task.id, err)
	}
	return metric, err
}

// Object representing a container running on an Agent in the Mesos environment
type ContainerEntity struct {
	entityType proto.EntityDTO_EntityType
	id string
	metrics  MetricMap
	task *data.Task
}

func (container *ContainerEntity) GetId() string {
	return container.id
}

func (container *ContainerEntity) GetType() proto.EntityDTO_EntityType {
	return container.entityType
}

func (container *ContainerEntity) GetResourceMetrics() MetricMap {
	return container.metrics
}

func (container *ContainerEntity) GetResourceMetric(resourceType data.ResourceType, metricType data.MetricPropType) (*Metric, error) {
	metric, err := container.metrics.GetResourceMetric(resourceType, metricType)
	if err != nil {
		err = fmt.Errorf("%s : %s", container.id, err)
	}
	return metric, err
}

// =============================================== Entity Metrics ======================================

func (resourceMetrics MetricMap) SetResourceMetric(resourceType data.ResourceType, metricType data.MetricPropType, value *float64) {
	//resourceMap, exists := resourceMetrics.metricMap[resourceType]
	resourceMap, exists := resourceMetrics[resourceType]
	if !exists {
		resourceMap = make(map[data.MetricPropType]*Metric)
		resourceMetrics[resourceType] = resourceMap
		//resourceMetrics.metricMap[resourceType] = resourceMap
	}
	metric, ok := resourceMap[metricType]
	if !ok {
		metric = &Metric{}
	}
	metric.value = value
	resourceMap[metricType] = metric
}

func (resourceMetrics MetricMap) GetResourceMetric(resourceType data.ResourceType, metricType data.MetricPropType) (*Metric, error) {
	//resourceMap, exists := resourceMetrics.metricMap[resourceType]
	resourceMap, exists := resourceMetrics[resourceType]
	if !exists {
		glog.V(4).Infof("Cannot find metrics for resource %s\n", resourceType)
		return nil, fmt.Errorf("missing metrics for resource %s", resourceType)
	}
	metric, exists := resourceMap[metricType]
	if !exists {
		glog.V(4).Infof("Cannot find metrics for type %s\n", metricType)
		return nil, fmt.Errorf("missing metrics for type %s:%s", resourceType, metricType)
	}
	return metric, nil
}

func (resourceMetrics MetricMap) printMetrics() {
	//glog.Infof("Entity %s\n", resourceMetrics.entityId)
	//fmt.Printf("Entity %s\n", resourceMetrics.entityId)
	for rt, resourceMap := range resourceMetrics { //.metricMap {
		//fmt.Printf("Resource Type %s\n", rt)
		for mkey, metric := range resourceMap {
			if (metric != nil) {
				glog.Infof("\t\t%s::%s : %f\n", rt, mkey, *metric.value)
				fmt.Printf("\t\t%s::%s : %f\n", rt, mkey, *metric.value)
			} else {
				glog.Infof("\t\t%s::%s : %f\n", rt, mkey, metric.value)
				fmt.Printf("\t\t%s::%s : %f\n", rt, mkey, metric.value)

			}
		}
	}
}

func PrintEntity(entity MesosEntity) {
	glog.Infof("Entity %s::%s\n", entity.GetType(), entity.GetId())
	fmt.Printf("Entity %s::%s\n", entity.GetType(), entity.GetId())
	resourceMetrics := entity.GetResourceMetrics()
	resourceMetrics.printMetrics()
}

func PrintRepository(repository *NodeRepository) {
	PrintEntity(repository.GetAgentEntity())
	taskEntities := repository.GetTaskEntities()
	for _, taskEntity := range taskEntities {
		PrintEntity(taskEntity)
	}
	containerEntities := repository.GetContainerEntities()
	for _, containerEntity := range containerEntities {
		PrintEntity(containerEntity)
	}
}
