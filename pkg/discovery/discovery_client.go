package discovery

import (
	"errors"
	"github.com/golang/glog"

	"github.com/turbonomic/turbo-go-sdk/pkg/probe"
	"github.com/turbonomic/turbo-go-sdk/pkg/proto"

	"fmt"
	"github.com/turbonomic/mesosturbo/pkg/conf"
	"github.com/turbonomic/mesosturbo/pkg/masterapi"
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
	mesosMasterType     conf.MesosMasterType
	clientConf          *conf.MesosTargetConf
	masterRestClient    master.MasterRestClient
	urlList		    []*MesosMasterUrl

	// Map of targetId and Mesos Master
	metricsStore 	*MesosMetricsMetadataStore
	mesosMaster	*data.MesosMaster
	prevCycleStatsCache *RawStatsCache
	agentList []*data.Agent
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
		discoveryWorker := NewDiscoveryWorker(discoveryClient.clientConf, agentList, discoveryClient.prevCycleStatsCache)
		name := fmt.Sprintf("DW-%d", i)
		fmt.Println("name=", name)
		discoveryWorker.SetName(name)
		workerGroup = append(workerGroup, discoveryWorker)
	}
	return workerGroup
}


func NewDiscoveryClient(mesosMasterType conf.MesosMasterType, clientConf *conf.MesosTargetConf) (probe.TurboDiscoveryClient, error) {
	if clientConf == nil {
		return nil, fmt.Errorf("[MesosDiscoveryClient] Null config")
	}

	glog.V(2).Infof("[MesosDiscoveryClient] Target Conf ", clientConf)

	client := &MesosDiscoveryClient{
		mesosMasterType: mesosMasterType,
		clientConf:      clientConf,
		prevCycleStatsCache: &RawStatsCache{},
	}

	// Init the discovery client by logging to the target
	err := client.initDiscoveryClient()
	if err != nil {
		return nil, fmt.Errorf("Error while creating new MesosDiscoveryClient: %s", err)
	}
	// Monitoring metadata
	client.metricsStore = NewMesosMetricsMetadataStore()
	return client, nil
}

func (discoveryClient *MesosDiscoveryClient) initDiscoveryClient() error {
	clientConf := discoveryClient.clientConf
	// Based on the Mesos vendor, instantiate the MesosRestClient
	masterRestClient := master.GetMasterRestClient(clientConf.Master, clientConf)
	if masterRestClient == nil {
		return fmt.Errorf("Cannot find RestClient for Mesos : " + string(clientConf.Master))
	}

	// Login to the Mesos Master and save the login token
	token, err := masterRestClient.Login()
	if err != nil {
		return fmt.Errorf("Error logging to Mesos Master at "+
			clientConf.LeaderIP +"::"+clientConf.LeaderPort +" : ", err)
	}

	clientConf.Token = token

	discoveryClient.masterRestClient = masterRestClient

	return nil
}

// ===================== Target Info ===========================================
// Get the Account Values to create VMTTarget in the turbo server corresponding to this client
func (discoveryClient *MesosDiscoveryClient) GetAccountValues() *probe.TurboTargetInfo { //[]*proto.AccountValue {
	var accountValues []*proto.AccountValue
	clientConf := discoveryClient.clientConf
	accountValues = clientConf.GetAccountValues()

	glog.Infof("[MesosDiscoveryClient] account values %s\n", accountValues)
	targetInfo := probe.NewTurboTargetInfoBuilder(PROBE_CATEGORY, string(clientConf.Master), string(conf.MasterIPPort), accountValues).Create()
	// TODO: change this to return the account values so that the turbo probe can create the target data
	return targetInfo //accountValues
}

// ===================== Validation ===========================================
// Validate the Target
func (discoveryClient *MesosDiscoveryClient) Validate(accountValues []*proto.AccountValue) (*proto.ValidationResponse, error) {
	// Login to the Mesos Master and save the login token
	token, err := discoveryClient.masterRestClient.Login()
	if err != nil {
		glog.Errorf("[MesosDiscoveryClient] Error logging to Mesos Master at %s", accountValues)
		return nil, fmt.Errorf("[MesosDiscoveryClient] Error %s logging to Mesos Master at using "+
			"account value %s ", err, accountValues)
	}
	discoveryClient.clientConf.Token = token
	// TODO: login here and save the login token
	validationResponse := &proto.ValidationResponse{}

	glog.Infof("validation response %s\n", validationResponse)
	return validationResponse, nil
}

// ===================== Discovery ===========================================

// Discover the Target Topology
func (discoveryClient *MesosDiscoveryClient) Discover(accountValues []*proto.AccountValue) (*proto.DiscoveryResponse, error) {
	// TODO: use account values to create target conf ?
	//target, err := conf.CreateMesosTargetConf(discoveryClient.mesosMasterType, accountValues)
	//mesosConf, ok := target.(*conf.MesosTargetConf)
	//if (!ok) {
	//	return nil, fmt.Errorf("Invalid target : %s", accountValues)
	//}

	mesosConf := discoveryClient.clientConf
	// Select the leader to connect
	if discoveryClient.urlList == nil {
		discoveryClient.urlList = ParseMasterIPPorts(mesosConf.MasterIPPort)
	}
	//urlList := ParseMasterIPPorts(mesosConf.MasterIPPort)
	mesosLeader, err := NewMesosLeader(mesosConf.Master, discoveryClient.urlList , mesosConf.MasterUsername, mesosConf.MasterPassword)

	// Connect to mesos master client
	//mesosState, err := discoveryClient.masterRestClient.GetState()
	if err != nil {
		glog.Errorf("Error getting state from master : %s \n", err)
		return nil, fmt.Errorf("Error getting state from master : %s", err)
	}
	mesosState := mesosLeader.MasterState
	discoveryClient.clientConf.LeaderIP = mesosLeader.leaderConf.LeaderIP
	discoveryClient.clientConf.LeaderPort = mesosLeader.leaderConf.LeaderPort
	glog.V(3).Infof("Mesos Get Succeeded: %v\n", mesosState)

	// to create convenience maps for slaves, tasks, convert units
	mesosMaster, err := discoveryClient.parseMesosState(mesosState)
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

	glog.Infof("End Discovery for MesosDiscoveryClient %s", accountValues)
	return discoveryResponse, nil
}

// Parses the mesos state into agent and task objects and maps
func (handler *MesosDiscoveryClient) parseMesosState(stateResp *data.MesosAPIResponse) (*data.MesosMaster, error) {

	glog.V(4).Infof("=========== parseMesosState =========")
	if stateResp.Agents == nil {
		glog.Errorf("Error getting Agents data from Mesos Master")
		return nil, errors.New("Error getting Agents data from Mesos Master")
	}

	mesosMaster := &data.MesosMaster{
		Id:     stateResp.Id,
		Leader: stateResp.Leader,
	}
	// Agent Map
	mesosMaster.AgentMap = make(map[string]*data.Agent)
	handler.agentList = []*data.Agent{}
	for idx, _ := range stateResp.Agents {
		agent := stateResp.Agents[idx]
		agent.ClusterName = stateResp.ClusterName
		glog.V(2).Infof("Agent : %s Id: %s", agent.Name+"::"+agent.Pid, agent.Id)
		agent.IP, agent.PortNum = getSlaveIP(agent)
		mesosMaster.AgentMap[agent.Id] = &agent
		handler.agentList = append(handler.agentList, &agent)
	}

	// Cluster
	mesosMaster.Cluster.MasterIP = handler.clientConf.LeaderIP
	mesosMaster.Cluster.ClusterName = stateResp.ClusterName

	if stateResp.Frameworks == nil {
		glog.Errorf("Error getting Frameworks response, only agents will be visible")
		return mesosMaster, errors.New("Error getting Frameworks response: %s")
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
			glog.Infof("	No tasks defined for framework : %s", framework.Name)
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
	glog.Infof("Master Id: %s Leader: %s Cluster: %+v", mesosMaster.Id, mesosMaster.Leader, mesosMaster.Cluster)

	for _, agent := range mesosMaster.AgentMap {
		glog.Infof("Agent Id: %s Name: %s IP: %s", agent.Id, agent.Name, agent.IP)
		glog.Infof("Total number of tasks is %d", len(agent.TaskMap))
		for _, task := range agent.TaskMap {
			glog.Infof("	Task Id: %s Name: %s", task.Id, task.Name)
		}
	}

	for _, framework := range mesosMaster.FrameworkMap {
		glog.V(3).Infof("Id: %s Name: %s", framework.Id, framework.Name)
		glog.V(3).Infof("Total number of tasks is %d", len(framework.Tasks))
		for _, task := range framework.Tasks {
			glog.V(3).Infof("	Task Id: %s Name: %s", task.Id, task.Name)
		}
	}

	glog.V(3).Infof("Total number of tasks is %d", len(mesosMaster.TaskMap))
	for _, task := range mesosMaster.TaskMap {
		glog.V(3).Infof("	Task Id: %s Name: %s", task.Id, task.Name)
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