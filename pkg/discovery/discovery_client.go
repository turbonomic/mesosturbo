package discovery

import (
	"github.com/golang/glog"

	"github.com/turbonomic/turbo-go-sdk/pkg/probe"
	"github.com/turbonomic/turbo-go-sdk/pkg/proto"

	"fmt"
	"github.com/turbonomic/mesosturbo/pkg/conf"
	"github.com/turbonomic/mesosturbo/pkg/data"
	"strings"
	"sync"
)

const (
	PROBE_CATEGORY string = "CloudNative"
)

// Discovery Client for the Mesos Probe
// Implements the TurboDiscoveryClient interface
type MesosDiscoveryClient struct {
	targetConf          *conf.MesosTargetConf //  target configuration
	MesosLeader         *MesosLeader          // discovered leader and its configuration

						  // Map of targetId and Mesos Master
	metricsStore        *MesosMetricsMetadataStore
	mesosMaster         *data.MesosMaster
	prevCycleStatsCache *RawStatsCache
	agentList           []*data.Agent
}

type SelectionStrategy string
const (

	FIXED_AGENT_SIZE SelectionStrategy = "Fixed_Agent_Size"
	FIXED_WORKER_SIZE  SelectionStrategy = "Fixed_Worker_Size"
	ONE_WORKER_PER_AGENT SelectionStrategy = "One_Worker_Per_Agent"
)

type DiscoveryWorkerStrategy interface {
	GetDiscoveryWorkerGroup(agentList []*data.Agent) []*DiscoveryWorker
}

func (discoveryClient *MesosDiscoveryClient) CreateDiscoveryWorker(st SelectionStrategy, count int)  []*DiscoveryWorker {
	var workerGroup []*DiscoveryWorker
	agentList := discoveryClient.agentList
	var agentSelector AgentSelector
	if st == FIXED_AGENT_SIZE {
		agentSelector = NewFixedGroupAgentSelector(count)
	} else if st == FIXED_WORKER_SIZE {
		agentSelector = NewFixedWorkerSizeSelector(count)
	} else {
		agentSelector = &SimpleAgentSelector{}
	}
	agentGroups := agentSelector.GetAgents(agentList)
	glog.Infof("*********** Number of agent groups %d", len(agentGroups))
	for i, _ := range agentGroups {
		agentList := agentGroups[i]
		glog.Infof("Number of agents %d", len(agentList))
		var agentNames []string
		for j, _ := range agentList {
			agentNames = append(agentNames, agentList[j].IP)
		}
		glog.Infof("DW-%d : Agents %+v", i, agentNames)
	}

	workerGroup = make([]*DiscoveryWorker, 0, len(agentGroups))
	for i, _ := range agentGroups {
		agentList := agentGroups[i]
		discoveryWorker := NewDiscoveryWorker(discoveryClient.MesosLeader.leaderConf, agentList, discoveryClient.prevCycleStatsCache)
		name := fmt.Sprintf("DW-%d", i)
		discoveryWorker.SetName(name)
		workerGroup = append(workerGroup, discoveryWorker)
	}
	return workerGroup
}


func NewDiscoveryClient(mesosMasterType conf.MesosMasterType, targetConf *conf.MesosTargetConf) (probe.TurboDiscoveryClient, error) {
	if targetConf == nil {
		return nil, fmt.Errorf("[MesosDiscoveryClient] Null target config")
	}
	glog.V(2).Infof("[MesosDiscoveryClient] Target Conf ", targetConf)

	// Create the MesosLeader to detect the leader IP to make Rest API calls during discovery and validation
	mesosLeader, err := NewMesosLeader(targetConf)
	if err != nil {
		return nil, fmt.Errorf("Error while creating new MesosDiscoveryClient: %s", err)
	}

	client := &MesosDiscoveryClient{
		targetConf:      targetConf,
		prevCycleStatsCache: &RawStatsCache{},
		MesosLeader: mesosLeader,
	}

	// Monitoring metadata
	client.metricsStore = NewMesosMetricsMetadataStore()
	return client, nil
}

// ===================== Target Info ===========================================
// Get the Account Values to create VMTTarget in the turbo server corresponding to this client
func (discoveryClient *MesosDiscoveryClient) GetAccountValues() *probe.TurboTargetInfo { //[]*proto.AccountValue {
	var accountValues []*proto.AccountValue
	clientConf := discoveryClient.targetConf
	accountValues = clientConf.GetAccountValues()

	glog.Infof("[MesosDiscoveryClient] account values %s\n", accountValues)
	targetInfo := probe.NewTurboTargetInfoBuilder(PROBE_CATEGORY, string(clientConf.Master), string(conf.MasterIPPort), accountValues).Create()
	// TODO: change this to return the account values so that the turbo probe can create the target data
	return targetInfo //accountValues
}

// ===================== Validation ===========================================
// Validate the Target
func (discoveryClient *MesosDiscoveryClient) Validate(accountValues []*proto.AccountValue) (*proto.ValidationResponse, error) {
	// refresh login using current leader or select the new one and login
	err := discoveryClient.MesosLeader.RefreshMesosLeaderLogin()

	if err != nil {
		nerr := fmt.Errorf("[MesosDiscoveryClient] Error %s logging to mesos master using "+
			"account values %s ", err, accountValues)
		glog.Errorf("%s", nerr.Error())
		return nil, nerr
	}
	validationResponse := &proto.ValidationResponse{}

	glog.Infof("%s : End validation using leader %++v", accountValues, discoveryClient.MesosLeader.leaderConf)
	return validationResponse, nil
}

// ===================== Discovery ===========================================
// Discover the Target Topology
func (discoveryClient *MesosDiscoveryClient) Discover(accountValues []*proto.AccountValue) (*proto.DiscoveryResponse, error) {
	// Use account values to verify the target conf
	var masterIPPort string
	for _, accVal := range accountValues {
		if *accVal.Key == string(conf.MasterIPPort) {
			masterIPPort = *accVal.StringValue
		}
	}
	if masterIPPort != discoveryClient.targetConf.MasterIPPort {
		return nil, fmt.Errorf("Invalid target : %s", accountValues)
	}

	// Refresh the state using current leader or select the new one and get state
	mesosLeader := discoveryClient.MesosLeader
	err := mesosLeader.RefreshMesosLeaderState()
	if err != nil {
		nerr := fmt.Errorf("%++v : Error getting state from leader %s", mesosLeader.leaderConf, err)
		glog.Errorf("%s", nerr.Error())
		return nil, nerr
	}
	glog.V(3).Infof("Mesos get succeeded: %v\n", mesosLeader.MasterState)

	// to create convenience maps for slaves, tasks, convert units
	mesosMaster, err := discoveryClient.parseMesosState(mesosLeader.MasterState)
	if mesosMaster == nil {
		return nil, fmt.Errorf("Error parsing mesos master response : %s", err)
	}
	logMesosSummary(mesosMaster)
	discoveryClient.mesosMaster = mesosMaster

	// Start discovery worker routines per group of agents
	workerResponseQueue := make(chan DiscoveryWorkerResponse, 1)
	var slice []DiscoveryWorkerResponse
	wg := new(sync.WaitGroup)

	var discoveryWorkerGroup []*DiscoveryWorker
	discoveryWorkerGroup = discoveryClient.CreateDiscoveryWorker(FIXED_WORKER_SIZE, 10)
	for idx, _ := range discoveryWorkerGroup {
		wg.Add(1)
		go func(idx int) {
			discoveryWorker := discoveryWorkerGroup[idx]
			nodeResponse := discoveryWorker.DoWork()
			workerResponseQueue <- nodeResponse //send it on the channel/queue
		}(idx)
	}
	go func() {
		for nodeRepos := range workerResponseQueue {
			slice = append(slice, nodeRepos)
			wg.Done()   // ** move the `Done()` call here
		}
	}()
	wg.Wait()

	// Build discovery response
	discoveryResponse, err := discoveryClient.createDiscoveryResponse(slice)
	// Save discovery stats
	discoveryClient.prevCycleStatsCache.RefreshCache(mesosMaster)
	glog.Infof("%s : End discovery using leader %++v", accountValues, discoveryClient.MesosLeader.leaderConf)
	return discoveryResponse, nil
}

// Parses the mesos state into agent and task objects and maps
func (handler *MesosDiscoveryClient) parseMesosState(stateResp *data.MesosAPIResponse) (*data.MesosMaster, error) {
	if stateResp.Agents == nil {
		nerr := fmt.Errorf("Error getting agents data from Mesos Master")
		glog.Errorf("%s", nerr.Error())
		return nil, nerr
	}

	mesosMaster := &data.MesosMaster{
		Id:     stateResp.Id,
		Leader: stateResp.Leader,
		Pid: stateResp.Pid,
	}
	// Agent Map
	mesosMaster.AgentMap = make(map[string]*data.Agent)
	handler.agentList = []*data.Agent{}
	for idx, _ := range stateResp.Agents {
		agent := stateResp.Agents[idx]
		agent.ClusterName = handler.targetConf.MasterIPPort	//Using target scope as cluster scope to handle
									// deployments where the Cluster is not named
		glog.V(3).Infof("Agent : %s Id: %s", agent.Name+"::"+agent.Pid, agent.Id)
		agent.IP, agent.PortNum = getSlaveIP(agent)
		mesosMaster.AgentMap[agent.Id] = &agent
		handler.agentList = append(handler.agentList, &agent)
	}

	// Cluster
	mesosMaster.Cluster.MasterIP = stateResp.Leader
	// Note - Using target scope as cluster scope to handle  deployments where the Cluster is not named
	mesosMaster.Cluster.ClusterName = handler.targetConf.MasterIPPort    //stateResp.ClusterName

	if stateResp.Frameworks == nil {
		nerr := fmt.Errorf("Error getting frameworks response, only agents will be visible")
		glog.Errorf("%s", nerr.Error())
		return mesosMaster, nerr
	}

	// Tasks Map
	glog.V(3).Infof("[MesosDiscoveryClient] number of frameworks is %d\n", len(stateResp.Frameworks))
	// Parse the Frameworks to get the list of all Tasks across all agents
	mesosMaster.FrameworkMap = make(map[string]*data.Framework)
	mesosMaster.TaskMap = make(map[string]*data.Task)
	for i := range stateResp.Frameworks {
		framework := stateResp.Frameworks[i]
		glog.V(3).Infof("Framework : ", framework.Name+"::"+framework.Hostname)
		mesosMaster.FrameworkMap[framework.Id] = &framework

		if framework.Tasks == nil {
			glog.V(3).Infof("	No tasks defined for framework : %s", framework.Name)
			continue
		}
		ftasks := framework.Tasks
		for idx := range ftasks {
			task := ftasks[idx]
			glog.V(3).Infof("	Task : %s", task.Name)
			mesosMaster.TaskMap[task.Id] = &task
			taskAgent, ok := mesosMaster.AgentMap[task.SlaveId] //save in the Agent
			if ok {
				var taskMap map[string]*data.Task
				if taskAgent.TaskMap == nil {
					taskMap = make(map[string]*data.Task)
					taskAgent.TaskMap = taskMap
				}
				taskMap = taskAgent.TaskMap
				taskMap[task.Id] = &task
			} else {
				glog.Warningf("Cannot find Agent: %s for task %s", taskAgent.Id+"::"+taskAgent.Pid, task.Name)
			}
		}
		glog.V(3).Infof("[MesosDiscoveryClient] Number of tasks in framework %s is %d", framework.Name, len(framework.Tasks))

	}
	return mesosMaster, nil
}

func logMesosSummary(mesosMaster *data.MesosMaster) {
	glog.Infof("Master Id:%s, Pid:%s, Leader:%s, Cluster:%+v", mesosMaster.Id, mesosMaster.Pid, mesosMaster.Leader, mesosMaster.Cluster)

	for _, agent := range mesosMaster.AgentMap {
		glog.V(2).Infof("Agent Id: %s, Name: %s, IP: %s, Number of tasks is %d", agent.Id, agent.Name, agent.IP, len(agent.TaskMap))
		for _, task := range agent.TaskMap {
			glog.V(4).Infof("	Task Id: %s, Name: %s", task.Id, task.Name)
		}
	}

	for _, framework := range mesosMaster.FrameworkMap {
		glog.V(2).Infof("Framework Id: %s, Name: %s, Number of tasks is %d", framework.Id, framework.Name, len(framework.Tasks))
		for _, task := range framework.Tasks {
			glog.V(4).Infof("	Task Id: %s, Name: %s", task.Id, task.Name)
		}
	}

	glog.V(4).Infof("Total number of tasks is %d", len(mesosMaster.TaskMap))
	for _, task := range mesosMaster.TaskMap {
		glog.V(4).Infof("	Task Id: %s, Name: %s", task.Id, task.Name)
	}
}


func (client *MesosDiscoveryClient) createDiscoveryResponse(workerResponseList []DiscoveryWorkerResponse) (*proto.DiscoveryResponse, error) {
	var entityDtos []*proto.EntityDTO
	ec := new(ErrorCollector)
	for i, _ := range workerResponseList {
		nodeResponseList := workerResponseList[i]
		for j, _ := range nodeResponseList {
			nodeResponse := nodeResponseList[j]
			if nodeResponse != nil && len(nodeResponse.entityDTOs) > 0 {
				entityDtos = append(entityDtos, nodeResponse.entityDTOs...)
			}
			if nodeResponse != nil && len(nodeResponse.entityDTOs) == 0 {
				ec.Collect(fmt.Errorf("Null DTOs for agent %s::%s", nodeResponse.agent.Id, nodeResponse.agent.IP))
			}

			ec.Collect(nodeResponse.errors)
		}
	}

	// 4. Discovery Response
	discoveryResponse := &proto.DiscoveryResponse{
		EntityDTO: entityDtos,
	}
	return discoveryResponse, ec
}

func getSlaveIP(s data.Agent) (string, string) {
	//"slave(1)@10.10.174.92:5051"
	var ipportArray []string
	slaveIP := ""
	slavePort := ""
	ipLong := s.Pid
	arr := strings.Split(ipLong, "@")
	if len(arr) > 1 {
		ipport := arr[1]
		ipportArray = strings.Split(ipport, ":")
	}
	if len(ipportArray) > 0 {
		slaveIP = ipportArray[0]
		slavePort = ipportArray[1]
	}
	return slaveIP, slavePort
}