package discovery

import (
	"fmt"
	"github.com/golang/glog"
	"github.com/turbonomic/mesosturbo/pkg/conf"
	"github.com/turbonomic/mesosturbo/pkg/masterapi"
	"time"
	"github.com/turbonomic/mesosturbo/pkg/data"
)

// Interface used by the DiscoveryClient to obtain the metrics for different resources for all entities discovered
type MonitoringClient interface {
	PollAgentForMetrics(agent *data.Agent)
	GetNodeMetrics() map[string]*data.EntityResourceMetrics
	GetTaskMetrics() map[string]*data.EntityResourceMetrics
	GetContainerMetrics() map[string]*data.EntityResourceMetrics
	InitMonitoringClient() error
}

const (
	CPU_MULTIPLIER float64 = 2000
	KB_MULTIPLIER float64 = 1024
	DEFAULT_VAL float64 = 0.0
)

// Implementation of MonitoringClient interface to provide metrics obtained from the mesos master state and
// agent statistics rest api calls that are available by default for each Mesos installation
type DefaultMonitoringClient struct {
	mesosMaster    *data.MesosMaster
	masterConf     *conf.MesosTargetConf
	lastCycleStats *DiscoveryCycleStats

	// Debug mode parameters
	DebugMode  bool
	DebugProps map[string]string //agent id and file
}

// Initialize the momitoring client.
// Get the stats from each agent and parse and save in the Agent and Task objects
func (monClient *DefaultMonitoringClient) InitMonitoringClient() error {
	if monClient.mesosMaster == nil || monClient.masterConf == nil {
		return fmt.Errorf("Monitoring client is not initalized")
	}
	// For each agent
	agentMap := monClient.mesosMaster.AgentMap
	for _, agent := range agentMap {
		// Get the stats from each agent and parse and save in the Agent and Task objects
		arrOfExec, err := monClient.getAgentStats(agent)
		glog.V(3).Infof("Parsed executors %s\n", arrOfExec)
		if err != nil {
			glog.Errorf("Error obtaining metrics from the agent %s::%s", agent.IP, agent.Port)
		} else {
			err = monClient.parseAgentUsedStats(agent, arrOfExec)
			if err != nil {
				glog.Errorf("Error parsing metrics from the agent %s::%s", agent.IP, agent.Port)
			}
		}
	}
	return nil
}

func (monClient *DefaultMonitoringClient) PollAgentForMetrics(agent *data.Agent) {
	// Get the stats from each agent and parse and save in the Agent and Task objects
	arrOfExec, err := monClient.getAgentStats(agent)
	glog.V(3).Infof("Parsed executors %s\n", arrOfExec)
	if err != nil {
		glog.Errorf("Error obtaining metrics from the agent %s::%s", agent.IP, agent.Port)
	} else {
		err = monClient.parseAgentUsedStats(agent, arrOfExec)
		if err != nil {
			glog.Errorf("Error parsing metrics from the agent %s::%s", agent.IP, agent.Port)
		}
	}
}

// Get the data containing the monitoring statistics for the specified agent.
// Create the rest api client for querying the agent and execute the stats query.
//
func (monClient *DefaultMonitoringClient) getAgentStats(agent *data.Agent) ([]data.Executor, error) {
	// Create the client for making rest api queries to the agent
	agentConf := &conf.AgentConf{
		AgentIP:   agent.IP,
		AgentPort: agent.Port,
	}
	agentClient := master.GetAgentRestClient(monClient.masterConf.Master, agentConf, monClient.masterConf)

	if monClient.DebugMode {
		filepath, exists := monClient.DebugProps[agent.Id]
		if !exists {
			return nil, fmt.Errorf("Missing agent stats file for agent [%s] in debug props: %s ", agent.Id, monClient.DebugProps)
		}
		genericAgent, ok := agentClient.(*master.GenericAgentAPIClient)
		if !ok {
			return nil, fmt.Errorf("Cannot execute request for agent in debug mode")
		}
		genericAgent.DebugMode = true
		debugProps := make(map[string]string)
		debugProps["file"] = filepath
		genericAgent.DebugProps = debugProps
	}

	// Query response with the Task statistics
	var arrOfExec []data.Executor
	arrOfExec, err := agentClient.GetStats()
	if err != nil {
		return nil, err
	}
	return arrOfExec, nil
}

// Get the node cpu and mem usage metrics using the response of executor objects
func (monClient *DefaultMonitoringClient) parseAgentUsedStats(agent *data.Agent, arrOfExec []data.Executor) error {
	if arrOfExec == nil || len(arrOfExec) == 0 {
		return fmt.Errorf("Null or empty stats response for agent %s", agent.Id)
	}

	lastCycleStats := monClient.lastCycleStats
	var lastTime *time.Time
	var taskPrevStats map[string]*TaskStatistics
	if lastCycleStats != nil {
		if lastCycleStats.lastDiscoveryTime != nil {
			lastTime = lastCycleStats.lastDiscoveryTime
		}
		taskPrevStats = lastCycleStats.TaskStats
	}

	// Create new ResourceUseStats for the agent
	agent.ResourceUseStats = &data.CalculatedUse{}

	// Iterate over the list and compute the task vcpu and vmem used values
	// The used values for the agent is the sum of the used for each task
	glog.Infof("--------------------------------> Agent %s : %s\n", agent.Id, agent.IP)
	glog.Infof("Before agent resource stats: [capacity %+v] [usage %+v]\n", agent.Resources, agent.ResourceUseStats)
	for idx, _ := range arrOfExec {
		executor := arrOfExec[idx]
		glog.Infof("---------> Executor %+v\n", executor)
		task := findTask(executor.Source, executor.Id, agent.TaskMap)
		//
		if task == nil { // check for the task object corresponding to this taskId
			glog.Warningf("unknown task id %s", executor.Source+"::"+executor.Id)
			continue
		}
		glog.Infof("Task %s::%s\n", task.Name, task.Id)
		var currStats, prevStats data.Statistics
		currStats = executor.Statistics
		task.RawStatistics = currStats //save for next cycle
		glog.Infof("Initial Task: [capacity %+v] [usage %+v]\n", task.Resources, task.RawStatistics)
		if taskPrevStats != nil {
			_, ok := taskPrevStats[task.Id]
			if ok {
				prevStats = taskPrevStats[task.Id].rawStats
			} else {
				glog.Infof("previous cycle stats not available for " + agent.Id + "::" + task.Id)
			}
		}
		usedCPUFraction := calculateCPU(task.Id, agent.Id, &prevStats, &currStats, lastTime)

		usedCPU := usedCPUFraction * agent.Resources.CPUUnits * float64(1000) //CPU_MULTIPLIER
		agent.ResourceUseStats.CPUMHz += usedCPU // save the accumulated value in the agent
		glog.Infof("%s usedCPU=%f agent=%f", agent.IP, usedCPU, agent.ResourceUseStats.CPUMHz)
		usedMemBytes := currStats.MemRSSBytes
		usedMemKB := usedMemBytes / KB_MULTIPLIER
		agent.ResourceUseStats.MemKB += usedMemKB // save in the agent
		// Task capacities - create new ResourceUseStats for the task
		task.ResourceUseStats = &data.CalculatedUse{}
		task.ResourceUseStats.CPUMHz = usedCPU // save in the task
		task.ResourceUseStats.MemKB = usedMemKB
		task.Resources.MemMB = currStats.MemLimitBytes
		task.Resources.CPUUnits = currStats.CPUsLimit
		//task.Resources.Disk = currStats.DiskLimitBytes / float64(1024.00*1024.00)

		glog.Infof("Task resource stats: [capacity %+v] [usage %+v]\n", task.Resources, task.ResourceUseStats)

	} // task loop
	//fmt.Printf("Agent resource stats: [capacity %+v] [usage %+v]\n", agent.Resources, agent.ResourceUseStats)
	glog.Infof("Agent resource stats: [capacity %+v] [usage %+v]\n", agent.Resources, agent.ResourceUseStats)
	glog.Infof("--------------------------------------------------")
	return nil
}

// Find the task using the sourceId or executorId from the agents stats query response
func findTask(sourceId, executorId string, taskMap map[string]*data.Task) *data.Task {
	var task *data.Task
	task, ok := taskMap[sourceId]
	if ok {
		return task
	}
	for _, task := range taskMap {
		if task.ExecutorId == executorId {
			return task
		}
	}
	return nil
}

func calculateCPU(taskId, agentId string, prevStats, currStats *data.Statistics, lastTime *time.Time) float64 {
	var prevSecs, curSecs float64
	glog.Infof("Previous RawStatistics %+v\n", prevStats)
	if prevStats == nil || lastTime == nil {
		glog.Infof("previous cycle stats not available for " + agentId + "::" + taskId)
		return DEFAULT_VAL
	}

	prevSecs = prevStats.CPUsystemTimeSecs + prevStats.CPUuserTimeSecs
	curSecs = currStats.CPUsystemTimeSecs + currStats.CPUuserTimeSecs
	diffSecs := curSecs - prevSecs
	if diffSecs < 0 {
		diffSecs = DEFAULT_VAL
	}

	diffTime := time.Since(*lastTime)
	diffT := diffTime.Seconds()
	usedCPUFraction := diffSecs / diffT
	glog.Infof("%s: diffSecs=%f diffTime=%t usedCPUFraction=%s", agentId, diffSecs, diffT, usedCPUFraction)
	return usedCPUFraction
}

// ==================================== Metrics ================================================
// VCPU, VMem, CPU Provisioned, Mem Provisioned metrics for container objects for each of the tasks in the mesos master.
func (monClient *DefaultMonitoringClient) GetContainerMetrics() map[string]*data.EntityResourceMetrics {
	taskMap := monClient.mesosMaster.TaskMap
	metricsMap := make(map[string]*data.EntityResourceMetrics)
	// For each agent
	for _, task := range taskMap {
		glog.V(3).Infof("========== GetContainerMetrics: %s %s ===========", task.Id, task.Name)
		appMetrics, _ := defaultContainerMetrics(task)
		metricsMap[task.Id] = appMetrics
	}
	return metricsMap
}

func defaultContainerMetrics(task *data.Task) (*data.EntityResourceMetrics, error) {
	containerMetrics := data.NewEntityResourceMetrics(task.Id)

	// VCPU Capacity

	var cpuCap float64
	cpuCap = task.Resources.CPUUnits * CPU_MULTIPLIER
	containerMetrics.SetResourceMetricValue(data.CPU, data.CAP, &cpuCap)

	// VMem Capacity
	var memCapKB float64
	memCapKB = task.Resources.MemMB * KB_MULTIPLIER
	containerMetrics.SetResourceMetricValue(data.MEM, data.CAP, &memCapKB)

	if task.ResourceUseStats != nil { // from the Agent Rest api
		// VCPU Used
		containerMetrics.SetResourceMetricValue(data.CPU, data.USED, &task.ResourceUseStats.CPUMHz)

		// VMem Used
		containerMetrics.SetResourceMetricValue(data.MEM, data.USED, &task.ResourceUseStats.MemKB)
	} else {
		fmt.Errorf("missing stats for container %s", task.Id)
	}

	var cpuProvUsedMHZ float64
	cpuProvUsedMHZ = task.Resources.CPUUnits * CPU_MULTIPLIER
	containerMetrics.SetResourceMetricValue(data.CPU_PROV, data.USED, &cpuProvUsedMHZ)

	var memProvUsedKB float64
	memProvUsedKB = task.Resources.MemMB * KB_MULTIPLIER
	containerMetrics.SetResourceMetricValue(data.MEM_PROV, data.USED, &memProvUsedKB)
	return containerMetrics, nil
}

// VCPU, VMem, CPU Provisioned, Mem Provisioned metrics for each of the tasks in the mesos master.
func (monClient *DefaultMonitoringClient) GetTaskMetrics() map[string]*data.EntityResourceMetrics {
	taskMap := monClient.mesosMaster.TaskMap
	metricsMap := make(map[string]*data.EntityResourceMetrics)
	// For each agent
	for _, task := range taskMap {
		glog.V(3).Infof("========== Task: %s %s ===========", task.Id, task.Name)
		appMetrics, _ := defaultTaskMetrics(task) // TODO: make this a method in Task and save the metrics there
		metricsMap[task.Id] = appMetrics
	}
	return metricsMap
}

func defaultTaskMetrics(task *data.Task) (*data.EntityResourceMetrics, error) {
	appMetrics := data.NewEntityResourceMetrics(task.Id)
	if task.ResourceUseStats != nil { // from the Agent Rest api
		// VCPU Use
		appMetrics.SetResourceMetricValue(data.CPU, data.USED, &task.ResourceUseStats.CPUMHz)
		// VMem Used
		appMetrics.SetResourceMetricValue(data.MEM, data.USED, &task.ResourceUseStats.MemKB)
	} else {
		return appMetrics, fmt.Errorf("Missing stats for task %s", task.Id)
	}

	return appMetrics, nil
}

// VCPU, VMem, CPU Provisioned, Mem Provisioned metrics for each of the agents in the mesos master.
func (monClient *DefaultMonitoringClient) GetNodeMetrics() map[string]*data.EntityResourceMetrics {
	agentMap := monClient.mesosMaster.AgentMap

	metricsMap := make(map[string]*data.EntityResourceMetrics)
	// For each agent
	for _, agent := range agentMap {
		glog.V(3).Infof("========== Agent: %s %s ===========", agent.Id, agent.IP)
		// Default
		nodeMetrics, _ := defaultNodeMetrics(agent) // TODO: make this a method in Agent and save the metrics there
		metricsMap[agent.Id] = nodeMetrics
	}
	return metricsMap
}

func defaultNodeMetrics(agent *data.Agent) (*data.EntityResourceMetrics, error) {
	nodeMetrics := data.NewEntityResourceMetrics(agent.Id)

	var cpuCapMHZ float64
	cpuCapMHZ = agent.Resources.CPUUnits * CPU_MULTIPLIER
	nodeMetrics.SetResourceMetricValue(data.CPU, data.CAP, &cpuCapMHZ)

	var memCapKB float64
	memCapKB = agent.Resources.MemMB * KB_MULTIPLIER
	nodeMetrics.SetResourceMetricValue(data.MEM, data.CAP, &memCapKB)

	var cpuProvCapMHZ float64
	cpuProvCapMHZ = agent.Resources.CPUUnits * CPU_MULTIPLIER
	nodeMetrics.SetResourceMetricValue(data.CPU_PROV, data.CAP, &cpuProvCapMHZ)

	var cpuProvUsedMHZ float64
	cpuProvUsedMHZ = agent.UsedResources.CPUUnits * CPU_MULTIPLIER
	nodeMetrics.SetResourceMetricValue(data.CPU_PROV, data.USED, &cpuProvUsedMHZ)

	var memProvCapKB float64
	memProvCapKB = agent.Resources.MemMB * KB_MULTIPLIER
	nodeMetrics.SetResourceMetricValue(data.MEM_PROV, data.CAP, &memProvCapKB)

	var memProvUsedKB float64
	memProvUsedKB = agent.UsedResources.MemMB * KB_MULTIPLIER
	nodeMetrics.SetResourceMetricValue(data.MEM_PROV, data.USED, &memProvUsedKB)

	if agent.ResourceUseStats != nil { // from the Agent Rest api
		nodeMetrics.SetResourceMetricValue(data.CPU, data.USED, &agent.ResourceUseStats.CPUMHz)
		nodeMetrics.SetResourceMetricValue(data.MEM, data.USED, &agent.ResourceUseStats.MemKB)
	} else {
		fmt.Errorf("Missing stats for agent %s", agent.Id)
		glog.Errorf("Missing stats for agent %s", agent.Id)
	}
	glog.Infof("======= AGENT %s::%s\n",agent.Id, agent.IP )
	for mkey, resourceMetric := range nodeMetrics.GetAllResourceMetrics() {
		if resourceMetric.GetValue() != nil {
			glog.Infof("%s %f\n", mkey, *resourceMetric.GetValue())
		}
	}
	glog.Infof("================================================")
	return nodeMetrics, nil
}
