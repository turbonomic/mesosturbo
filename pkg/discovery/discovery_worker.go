package discovery

import (
	"github.com/turbonomic/mesosturbo/pkg/data"
	"github.com/golang/glog"
	"github.com/turbonomic/mesosturbo/pkg/conf"
	"sync"
	"fmt"
	"time"
	"github.com/turbonomic/turbo-go-sdk/pkg/proto"
)


// =============================================== Discovery and local Monitoring Tasks ======================================
// Worker Task to run discovery and monitoring on an agent node
type AgentTask interface {
	ProcessAgent() *AgentTaskResponse
}

type AgentTaskResponse struct {
	agent *data.Agent
	nodeRepository *NodeRepository
	entityDTOs []*proto.EntityDTO
	errors	*ErrorCollector
}

// Implementation for a Mesos Agent
type MesosAgentTask struct {
	node *data.Agent
	masterConf     *conf.MasterConf
	metricsStore 	*MesosMetricsMetadataStore
	rawStatsCache 	*RawStatsCache
}


func(agentTask MesosAgentTask) ProcessAgent() *AgentTaskResponse {
	ec := new(ErrorCollector)
	// Discovery related
	// Create Repository with entity objects for node, tasks and containers based on the Mesos data structures
	nodeRepository := NewNodeRepository(agentTask.node.Id)
	node := agentTask.node
	nodeRepository.agentEntity.node = node
	for _, task := range node.TaskMap {
		taskEntity := nodeRepository.CreateTaskEntity(task.Id)
		taskEntity.task = task	//save in the entity

		containerEntity := nodeRepository.CreateContainerEntity(task.Id)
		containerEntity.task = task // save in the entity
	}

	// Monitoring related
	// Get metrics for each entity gathered locally from the agent
	metricsStore := agentTask.metricsStore
	var monitoringPropsMap map[ENTITY_ID]*EntityMonitoringProps
	monitoringPropsMap = createMonitoringProps(nodeRepository, metricsStore.metricDefMap)

	monitorTarget := &MonitorTarget{
		targetId: agentTask.node.IP,
		config: agentTask.masterConf,
		repository: nodeRepository,
		rawStatsCache: agentTask.rawStatsCache,
		monitoringProps: monitoringPropsMap,
	}

	defaultMonitor := &DefaultMesosMonitor{}
	errors := defaultMonitor.Monitor(monitorTarget)
	ec.Collect(errors)
	if ec.Count() > 1 {
		glog.Errorf("%s : Monitor errors %s\n", node.IP, errors)
	}

	//PrintRepository(nodeRepository)

	// Create DTOs
	entityDTOs, errList := agentTask.createDiscoveryDTOs(nodeRepository)
	ec.CollectAll(errList)
	glog.V(2).Infof("%s : DONE create entities\n", node.IP)
	//for _, dto := range entityDTOs {
	//	glog.V(3).Infof("%s --> %s:%s\n %+s", node.IP, dto.GetEntityType(), dto.GetId(), dto)
	//}

	response := &AgentTaskResponse{
			nodeRepository: nodeRepository,
			errors: ec,
			agent: agentTask.node,
			}

	if entityDTOs != nil {
		response.entityDTOs = entityDTOs
	}
	return response
}

func (agentTask MesosAgentTask) createDiscoveryDTOs(nodeRepository *NodeRepository) ([]*proto.EntityDTO, []error) {
	var entityDtos []*proto.EntityDTO
	var errList []error
	// 3. Build Entities
	var nodeBuilder EntityBuilder
	nodeBuilder = &VMEntityBuilder{
		nodeRepository: nodeRepository,
	}
	nodeEntityDtos, err := nodeBuilder.BuildEntities()
	errList = append(errList, fmt.Errorf("Error parsing nodes: %s", err))
	entityDtos = append(entityDtos, nodeEntityDtos...)

	var containerBuilder EntityBuilder
	containerBuilder = &ContainerEntityBuilder{
		nodeRepository: nodeRepository,
	}
	containerEntityDtos, err := containerBuilder.BuildEntities()
	errList = append(errList, fmt.Errorf("Error parsing containers: %s", err))
	entityDtos = append(entityDtos, containerEntityDtos...)

	var appBuilder EntityBuilder
	appBuilder = &AppEntityBuilder{
		nodeRepository: nodeRepository,
	}
	appEntityDtos, err := appBuilder.BuildEntities()
	errList = append(errList, fmt.Errorf("Error parsing tasks: %s", err))
	entityDtos = append(entityDtos, appEntityDtos...)

	return entityDtos, errList
}

// --------------------------------------------------------------------------
//
type DiscoveryWorkerResponse []*AgentTaskResponse
type DiscoveryWorker struct {
	name string
	// complete mesos master state and config
	masterConf     *conf.MasterConf
	// subset of a agent map from the mesos master state
	nodeList 	[]*data.Agent
	nodeResponseQueue	chan *AgentTaskResponse //TODO: change to dto queue
	// metrics collection related
	metricsStore 	*MesosMetricsMetadataStore
	rawStatsCache 	*RawStatsCache
}

// Discovery worker for set of nodes grouped by certain criterion to distribute discovery
func NewDiscoveryWorker (masterConf *conf.MasterConf, nodeList []*data.Agent, rawStatsCache *RawStatsCache) *DiscoveryWorker {
	if nodeList == nil || len(nodeList) == 0 {
		glog.Errorf("No agents specified for discovery worker")
		return nil
	}
	worker := &DiscoveryWorker{
		masterConf: masterConf,
		nodeList: nodeList,
		nodeResponseQueue: make(chan *AgentTaskResponse, 1),
		rawStatsCache: rawStatsCache,
	}

	// Create metrics collector for this worker here and pass it to the different agent tasks
	worker.metricsStore = NewMesosMetricsMetadataStore()

	return worker
}
func (worker *DiscoveryWorker) SetName(name string) {
	worker.name = name
}

func (worker *DiscoveryWorker) DoWork() DiscoveryWorkerResponse {
	// Get discovered Entities
	// Get Metrics
	// Put in repository
	var agentTaskResponseList []*AgentTaskResponse
	wg := new(sync.WaitGroup)

	//workerRepos := make([]*NodeRepository, len(worker.nodeList))

	for idx, _ := range worker.nodeList {
		wg.Add(1)
		go func(idx int) {
			node := worker.nodeList[idx]
			//fmt.Printf("%s : Begin Process Agent %d::%s\n", worker.name, idx, node.IP)
			glog.V(2).Infof("%s: Begin Process Agent %d::%s", worker.name, idx, node.Id)
			agentTask := &MesosAgentTask{
				node: node,
				masterConf: worker.masterConf,
				metricsStore: worker.metricsStore,
				rawStatsCache: worker.rawStatsCache,
			}
			nodeResponse := agentTask.ProcessAgent() //TODO: <-- returns node repository

			//workerRepos[idx] = nodeResponse
			worker.nodeResponseQueue <- nodeResponse //send it on the channel/queue
			//fmt.Printf("%s : End Process Agent %d::%s, num of tasks: %d\n",  worker.name, idx, node.IP, len(nodeResponse.nodeRepository.taskEntities))
			glog.V(2).Infof("%s : End Process Agent %d::%s, num of tasks: %d",  worker.name, idx, node.Id, len(nodeResponse.nodeRepository.taskEntities))
		}(idx)
	}
	go func() {
		for nodeRepos := range worker.nodeResponseQueue {
			agentTaskResponseList = append(agentTaskResponseList, nodeRepos)
			wg.Done()   // ** move the `Done()` call here
		}
	}()

	wg.Wait()
	//glog.V(3).Infof("%s : Done all agent tasks\n", worker.name)
	return DiscoveryWorkerResponse(agentTaskResponseList)
}


// ==============================================
type StopWatch struct {
	name string
	startTime time.Time
	elapsed time.Duration
}

func NewStopWatch(name string) *StopWatch {
	return &StopWatch{name:name,}
}

func (watch *StopWatch) start() {
	watch.startTime = time.Now()
}

func (watch *StopWatch) stop() {
	watch.elapsed = time.Since(watch.startTime)
}

func (watch *StopWatch) printDuration() {
	glog.Infof("Execution time for %s is %v millis", watch.name, watch.elapsed)
}

// ===========================================
type AgentSelector interface {
	GetAgents(agentList []*data.Agent) [][]*data.Agent
}

type SimpleAgentSelector struct {}

func (selector *SimpleAgentSelector) GetAgents(agentList []*data.Agent) [][]*data.Agent {
	var agentGroup [][]*data.Agent
	for i, _ := range agentList {
		agent := agentList[i]
		var newAgentList []*data.Agent
		newAgentList = append(newAgentList, agent)
		agentGroup = append(agentGroup, newAgentList)
	}
	return agentGroup
}


type FixedAgentSizeSelector struct {
	groupSize int
}

func NewFixedGroupAgentSelector(groupSize int) *FixedAgentSizeSelector {
	return &FixedAgentSizeSelector{groupSize:groupSize,}
}
func (selector *FixedAgentSizeSelector) GetAgents(agentList []*data.Agent) [][]*data.Agent {
	var agentGroup [][]*data.Agent
	var newAgentList []*data.Agent
	newAgentList = []*data.Agent{}
	for i, _ := range agentList {
		agent := agentList[i]
		newAgentList = append(newAgentList, agent)
		if ((i+1) % selector.groupSize) == 0 {
			agentGroup = append(agentGroup, newAgentList)
			newAgentList = []*data.Agent{}
		}
	}
	if len(newAgentList) > 0 {
		agentGroup = append(agentGroup, newAgentList)
	}
	return agentGroup
}



type FixedWorkerSizeSelector struct {
	groupSize int
}

func NewFixedWorkerSizeSelector(groupSize int) *FixedWorkerSizeSelector {
	return &FixedWorkerSizeSelector{groupSize:groupSize,}
}
func (selector *FixedWorkerSizeSelector) GetAgents(agentList []*data.Agent) [][]*data.Agent {

	numOfAgentsPerGroup := len(agentList)
	if (numOfAgentsPerGroup > selector.groupSize) {
		numOfAgentsPerGroup = len(agentList) / selector.groupSize
	}
	var agentGroup [][]*data.Agent
	var newAgentList []*data.Agent
	newAgentList = []*data.Agent{}
	agentCounter := 0
	for i, _ := range agentList {
		agent := agentList[i]
		agentCounter++
		newAgentList = append(newAgentList, agent)
		if (agentCounter == numOfAgentsPerGroup) {
			agentGroup = append(agentGroup, newAgentList)
			newAgentList = []*data.Agent{}
			agentCounter = 0
		}
	}
	if len(newAgentList) > 0 {
		agentList := agentGroup[0]
		agentList = append(agentList, newAgentList...)
		glog.Info("[FixedWorkerSizeSelector] nlast AgentList: %s", agentList)
	}
	return agentGroup
}