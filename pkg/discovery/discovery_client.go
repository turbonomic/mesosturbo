package discovery

import (
	"errors"
	"github.com/golang/glog"
	"time"

	"github.com/turbonomic/turbo-go-sdk/pkg/probe"
	"github.com/turbonomic/turbo-go-sdk/pkg/proto"

	"fmt"
	"github.com/turbonomic/mesosturbo/pkg/conf"
	"github.com/turbonomic/mesosturbo/pkg/masterapi"
	"github.com/turbonomic/mesosturbo/pkg/data"
	"strings"
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

	// Map of targetId and Mesos Master
	monitoringClient MonitoringClient
	mesosMaster	*data.MesosMaster
	lastCycleStats *DiscoveryCycleStats
}

type DiscoveryCycleStats struct {
	lastDiscoveryTime *time.Time
	//raw statistics from the rest api for each task
	TaskStats map[string]*TaskStatistics
}

type TaskStatistics struct {
	taskId   string
	rawStats data.Statistics
}

func NewDiscoveryClient(mesosMasterType conf.MesosMasterType, clientConf *conf.MesosTargetConf) (probe.TurboDiscoveryClient, error) {
	if clientConf == nil {
		return nil, fmt.Errorf("[MesosDiscoveryClient] Null config")
	}

	glog.V(2).Infof("[MesosDiscoveryClient] Target Conf ", clientConf)

	client := &MesosDiscoveryClient{
		mesosMasterType: mesosMasterType,
		clientConf:      clientConf,
		lastCycleStats:  &DiscoveryCycleStats{},
	}

	// Init the discovery client by logging to the target
	err := client.initDiscoveryClient()
	if err != nil {
		return nil, fmt.Errorf("Error while creating new MesosDiscoveryClient: %s", err)
	}

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
			clientConf.MasterIP+"::"+clientConf.MasterPort+" : ", err)
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
	targetInfo := probe.NewTurboTargetInfoBuilder(PROBE_CATEGORY, string(clientConf.Master), string(conf.MasterIP), accountValues).Create()
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
	// TODO: use account values ?
	// 1. Discovery data
	mesosState, err := discoveryClient.masterRestClient.GetState()
	if err != nil {
		glog.Errorf("Error getting state from master : %s \n", err)
		return nil, fmt.Errorf("Error getting state from master : %s", err)
	}
	glog.V(3).Infof("Mesos Get Succeeded: %v\n", mesosState)

	// TODO: update leader and reissue request
	// to create convenience maps for slaves, tasks, convert units
	mesosMaster, err := discoveryClient.parseMesosState(mesosState)
	if mesosMaster == nil {
		return nil, fmt.Errorf("Error parsing mesos master response : %s", err)
	}
	logMesosSummary(mesosMaster)
	discoveryClient.mesosMaster = mesosMaster

	// 2. Monitoring data
	monitoringClient := &DefaultMonitoringClient{
		masterConf:     discoveryClient.clientConf,
		mesosMaster:    mesosMaster,
		lastCycleStats: discoveryClient.lastCycleStats,
	}
	discoveryClient.monitoringClient = monitoringClient
	monitoringClient.InitMonitoringClient()

	// 3. Build Entities
	discoveryResponse, err := discoveryClient.createDiscoveryResponse(mesosMaster, monitoringClient)

	// Save discovery stats
	currtime := time.Now()
	discoveryClient.lastCycleStats.lastDiscoveryTime = &currtime
	discoveryClient.lastCycleStats.TaskStats = make(map[string]*TaskStatistics)
	for _, agent := range mesosMaster.AgentMap {
		taskMap := agent.TaskMap
		for _, task := range taskMap {
			discoveryClient.lastCycleStats.TaskStats[task.Id] = &TaskStatistics{
				taskId:   task.Id,
				rawStats: task.RawStatistics,
			}
		}
	}
	glog.Infof("END Discovery for MesosDiscoveryClient %s", accountValues)
	return discoveryResponse, nil
}

// Parses the mesos state into agent and task objects and maps
func (handler *MesosDiscoveryClient) parseMesosState(stateResp *data.MesosAPIResponse) (*data.MesosMaster, error) {
	glog.Info("=========== parseMesosState =========")
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
	for idx, _ := range stateResp.Agents {
		agent := stateResp.Agents[idx]
		glog.Infof("Agent : %s Id: %s", agent.Name+"::"+agent.Pid, agent.Id)
		agent.IP, agent.Port = getSlaveIP(agent)
		mesosMaster.AgentMap[agent.Id] = &agent
	}

	// Cluster
	mesosMaster.Cluster.MasterIP = handler.clientConf.MasterIP
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
			glog.Infof("	Task : %s", task.Name)
			mesosMaster.TaskMap[task.Id] = &task
			taskagent, ok := mesosMaster.AgentMap[task.SlaveId] //save in the Agent
			if ok {
				var taskMap map[string]*data.Task
				if taskagent.TaskMap == nil {
					taskMap = make(map[string]*data.Task)
					taskagent.TaskMap = taskMap
				}
				taskMap = taskagent.TaskMap
				taskMap[task.Id] = &task
			} else {
				glog.Warningf("Cannot find Agent: %s for task %s", taskagent.Id+"::"+taskagent.Pid, task.Name)
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
		glog.Infof("Id: %s Name: %s", framework.Id, framework.Name)
		glog.Infof("Total number of tasks is %d", len(framework.Tasks))
		for _, task := range framework.Tasks {
			glog.Infof("	Task Id: %s Name: %s", task.Id, task.Name)
		}
	}

	glog.Infof("Total number of tasks is %d", len(mesosMaster.TaskMap))
	for _, task := range mesosMaster.TaskMap {
		glog.Infof("	Task Id: %s Name: %s", task.Id, task.Name)
	}
}

func (client *MesosDiscoveryClient) createDiscoveryResponse(mesosMaster *data.MesosMaster, monitoringClient MonitoringClient) (*proto.DiscoveryResponse, error) {
	var entityDtos []*proto.EntityDTO

	// 3. Build Entities
	var nodeBuilder EntityBuilder
	nodeBuilder = &VMEntityBuilder{
		mesosMaster:    mesosMaster,
		nodeMetricsMap: monitoringClient.GetNodeMetrics(),
	}
	nodeEntityDtos, err := nodeBuilder.BuildEntities()
	if err != nil {
		glog.Errorf("Error parsing nodes: %s. Will return.", err)
		return nil, fmt.Errorf("Error parsing nodes: %s. Will return.", err)
	}

	entityDtos = append(entityDtos, nodeEntityDtos...)

	var containerBuilder EntityBuilder
	containerBuilder = &ContainerEntityBuilder{
		mesosMaster:         mesosMaster,
		containerMetricsMap: monitoringClient.GetContainerMetrics(),
	}
	containerEntityDtos, err := containerBuilder.BuildEntities()
	if err != nil {
		glog.Errorf("Error parsing containers: %s. Will return.", err)
	}
	entityDtos = append(entityDtos, containerEntityDtos...)

	var appBuilder EntityBuilder
	appBuilder = &AppEntityBuilder{
		mesosMaster:    mesosMaster,
		taskMetricsMap: monitoringClient.GetTaskMetrics(),
	}
	appEntityDtos, err := appBuilder.BuildEntities()
	if err != nil {
		glog.Errorf("Error parsing containers: %s. Will return.", err)
	}
	entityDtos = append(entityDtos, appEntityDtos...)

	// 4. Discovery Response
	discoveryResponse := &proto.DiscoveryResponse{
		EntityDTO: entityDtos,
	}
	return discoveryResponse, nil
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