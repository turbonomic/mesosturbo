package discovery

import (
	"fmt"
	"github.com/golang/glog"
	"github.com/turbonomic/mesosturbo/pkg/conf"
	"github.com/turbonomic/mesosturbo/pkg/data"
	"github.com/turbonomic/mesosturbo/pkg/masterapi"
	"time"
	"strings"
)

type DefaultMesosMonitor struct {
	DebugMode  bool
	DebugProps map[string]string
}

func (monitor *DefaultMesosMonitor) GetSourceName() MONITOR_NAME {
	return DEFAULT_MESOS
}

// Implementation method for metric collection using Mesos Agent Rest API
func (monitor *DefaultMesosMonitor) Monitor(target *MonitorTarget) error {
	// Monitoring related
	if target == nil || target.config == nil {
		glog.Errorf("%s: Invalid target for monitor %s", monitor.GetSourceName(), target)
		return fmt.Errorf("%s: Invalid target for monitor %s", monitor.GetSourceName(), target)
	}
	var nodeRepository *NodeRepository
	nodeRepository, ok := target.repository.(*NodeRepository)
	if !ok {
		glog.Errorf("%s: Invalid repository for monitor %s", monitor.GetSourceName(), target.targetId)
		return fmt.Errorf("%s: Invalid repository for monitor %s", monitor.GetSourceName(), target.targetId)
	}

	var masterConf *conf.MesosTargetConf
	masterConf, ok = target.config.(*conf.MesosTargetConf)
	if !ok {
		glog.Errorf("Invalid mesos target conf %s", target.config)
		return fmt.Errorf("Invalid mesos target conf %s", target.config)
	}

	if target.monitoringProps == nil {
		glog.Errorf("Monitoring properties not specified for the target %s", target.targetId)
		return fmt.Errorf("Monitoring properties not specified for the target %s", target.targetId)
	}

	// Get the stats from each agent and parse and save in the Agent and Task objects
	agentEntity := nodeRepository.agentEntity
	agent := agentEntity.node
	arrOfExec, err := monitor.getAgentStats(agent, masterConf)
	glog.V(3).Infof("Parsed executors %s\n", arrOfExec)
	if err != nil {
		glog.Errorf("Error obtaining metrics from the agent %s::%s : %s", agent.IP, agent.PortNum, err)
		return fmt.Errorf("Error obtaining metrics from the agent %s::%s", agent.IP, agent.PortNum)
	}

	err = monitor.parseAgentUsedStats(agent, arrOfExec, target.rawStatsCache)
	if err != nil {
		glog.Errorf("Error parsing metrics from the agent %s::%s : %s", agent.IP, agent.PortNum, err)
		return fmt.Errorf("Error parsing metrics from the agent %s::%s", agent.IP, agent.PortNum)
	}

	// And then compute the metric values for the specified metrics and invoke the metric setter to set it in the entity
	errorCollector := new(ErrorCollector)
	monitor.setNodeMetrics(nodeRepository.agentEntity, target.monitoringProps, errorCollector)
	monitor.setTaskMetrics(nodeRepository.taskEntities, target.monitoringProps, errorCollector)
	monitor.setContainerMetrics(nodeRepository.containerEntities, target.monitoringProps, errorCollector)

	return errorCollector
}

// From - http://stackoverflow.com/questions/33470649/combine-multiple-error-strings
type ErrorCollector []error

func (ec *ErrorCollector) Count() int {
	return len(*ec)
}

func (ec *ErrorCollector) Collect(err error) {
	if err != nil {
		*ec = append(*ec, err)
	}
}

func (ec *ErrorCollector) CollectAll(errList []error) {
	for i, _ := range errList{
		err := errList[i]
		if err != nil {
			*ec = append(*ec, err)
		}
	}
}

func (ec *ErrorCollector) Error() (string){
	var errorStr []string
	errorStr = append(errorStr, "Collected errors:")
	for i, err := range *ec {
		errorStr = append(errorStr, fmt.Sprintf("Error %d: %s", i, err.Error()))
	}
	return  strings.Join(errorStr, "\n")
}

func (monitor *DefaultMesosMonitor) setNodeMetrics(nodeEntity *AgentEntity, monitoringProps map[ENTITY_ID]*EntityMonitoringProps, ec *ErrorCollector) {
	agent := nodeEntity.node
	var props *EntityMonitoringProps
	props, exists := monitoringProps[ENTITY_ID(nodeEntity.GetId())]
	if !exists {
		ec.Collect(fmt.Errorf("%s::%s : Missing monitoring properties", nodeEntity.GetType(), nodeEntity.GetId()))
		return
	}

	cpuCapMHZ := agent.Resources.CPUUnits * data.CPU_MULTIPLIER
	setValue(nodeEntity, &cpuCapMHZ, CPU_CAP, props, ec)

	memCapKB := agent.Resources.MemMB * data.KB_MULTIPLIER
	setValue(nodeEntity, &memCapKB, MEM_CAP, props, ec)

	cpuProvCapMHZ := agent.Resources.CPUUnits * data.CPU_MULTIPLIER
	setValue(nodeEntity, &cpuProvCapMHZ, CPU_PROV_CAP, props, ec)

	cpuProvUsedMHZ := agent.UsedResources.CPUUnits * data.CPU_MULTIPLIER
	setValue(nodeEntity, &cpuProvUsedMHZ, CPU_PROV_USED, props, ec)

	memProvCapKB := agent.Resources.MemMB * data.KB_MULTIPLIER
	setValue(nodeEntity, &memProvCapKB, MEM_PROV_CAP, props, ec)

	memProvUsedKB := agent.UsedResources.MemMB * data.KB_MULTIPLIER
	setValue(nodeEntity, &memProvUsedKB, MEM_PROV_USED, props, ec)

	if agent.ResourceUseStats != nil { // from the Agent Rest api
		setValue(nodeEntity, &agent.ResourceUseStats.CPUMHz, CPU_USED, props, ec)
		setValue(nodeEntity, &agent.ResourceUseStats.MemKB, MEM_USED, props, ec)
	} else {
		glog.Errorf("Missing stats for agent %s", agent.Id)
	}
	return
}

func (monitor *DefaultMesosMonitor) setTaskMetrics(taskEntities map[string]*TaskEntity, monitoringProps map[ENTITY_ID]*EntityMonitoringProps, ec *ErrorCollector) {
	// For each task
	for _, taskEntity := range taskEntities {
		var props *EntityMonitoringProps
		props, exists := monitoringProps[ENTITY_ID(taskEntity.GetId())]
		if !exists {
			ec.Collect(fmt.Errorf("%s::%s : Missing monitoring properties", taskEntity.GetType(), taskEntity.GetId()))
			continue
		}

		task := taskEntity.task
		if task.ResourceUseStats != nil { // from the Agent Rest api
			// VCPU Use
			setValue(taskEntity, &task.ResourceUseStats.CPUMHz, CPU_USED, props, ec)
			// VMem Used
			setValue(taskEntity, &task.ResourceUseStats.MemKB, MEM_USED, props, ec)
		}
	}
	return
}

func (monitor *DefaultMesosMonitor) setContainerMetrics(containerEntities map[string]*ContainerEntity, monitoringProps map[ENTITY_ID]*EntityMonitoringProps, ec *ErrorCollector) {
	glog.Info("============== setContainerMetrics =============")
	// For each container
	for _, containerEntity := range containerEntities {
		var props *EntityMonitoringProps
		props, exists := monitoringProps[ENTITY_ID(containerEntity.GetId())]
		if !exists {
			ec.Collect(fmt.Errorf("%s::%s::%s : Missing monitoring properties",
				containerEntity.GetType(), containerEntity.GetId(), containerEntity.task.Name))
			continue
		}

		task := containerEntity.task
		// VCPU Capacity

		var cpuCap float64
		cpuCap = task.Resources.CPUUnits * data.CPU_MULTIPLIER
		setValue(containerEntity, &cpuCap, CPU_CAP, props, ec)

		// VMem Capacity
		var memCapKB float64
		memCapKB = task.Resources.MemMB * data.KB_MULTIPLIER
		setValue(containerEntity, &memCapKB, MEM_CAP, props, ec)

		if task.ResourceUseStats != nil { // from the Agent Rest api
			// VCPU Used
			setValue(containerEntity, &task.ResourceUseStats.CPUMHz, CPU_USED, props, ec)
			// VMem Used
			setValue(containerEntity, &task.ResourceUseStats.MemKB, MEM_USED, props, ec)
		} else {
			glog.Errorf("missing stats for container %s", task.Id)
		}

		var cpuProvUsedMHZ float64
		cpuProvUsedMHZ = task.Resources.CPUUnits * data.CPU_MULTIPLIER
		setValue(containerEntity, &cpuProvUsedMHZ, CPU_PROV_USED, props, ec)

		var memProvUsedKB float64
		memProvUsedKB = task.Resources.MemMB * data.KB_MULTIPLIER
		setValue(containerEntity, &memProvUsedKB, MEM_PROV_USED, props, ec)
	}
	glog.Info("============== End setContainerMetrics =============")
	return
}

// Helper method to set value for a metric
func setValue(entity MesosEntity, value *float64, propKey PropKey, props *EntityMonitoringProps, ec *ErrorCollector)  {
	propMap := props.propMap

	prop, ok := propMap[propKey]
	if ok {
		metricDef := prop.metricDef
		if metricDef != nil {
			metricSetter := metricDef.metricSetter
			if metricSetter != nil {
				//fmt.Printf("%s::%s : %s : %s\n", entity.GetType(),entity.GetId(), propKey, &metricSetter)
				metricSetter.SetMetricValue(entity, value)
			} else {
				ec.Collect(fmt.Errorf("%s::%s : Missing metric setter for key %s", entity.GetType(),entity.GetId(), propKey))
				return
			}
		} else {
			ec.Collect(fmt.Errorf("%s::%s : Missing metric def for key %s", entity.GetType(),entity.GetId(), propKey))
			return
		}
	} else {
		ec.Collect(fmt.Errorf("%s::%s : Missing monitoring property for key %s", entity.GetType(),entity.GetId(), propKey))
		return
	}
}

// ======================= Polling Agent ==========================

// Get the data containing the monitoring statistics for the specified agent.
// Create the rest api client for querying the agent and execute the stats query.
//
func (monitor *DefaultMesosMonitor) getAgentStats(agent *data.Agent, masterConf *conf.MesosTargetConf) ([]data.Executor, error) {
	// Create the client for making rest api queries to the agent
	agentConf := &conf.AgentConf{
		AgentIP:   agent.IP,
		AgentPort: agent.PortNum,
	}
	agentClient := master.GetAgentRestClient(masterConf.Master, agentConf, masterConf)

	if monitor.DebugMode {
		filepath, exists := monitor.DebugProps[agent.Id]
		if !exists {
			return nil, fmt.Errorf("Missing agent stats file for agent [%s] in debug props: %s ", agent.Id, monitor.DebugProps)
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
func (monitor *DefaultMesosMonitor) parseAgentUsedStats(agent *data.Agent, arrOfExec []data.Executor, rawStatsCache *RawStatsCache) error {
	if arrOfExec == nil || len(arrOfExec) == 0 {
		return fmt.Errorf("Null or empty stats response for agent %s", agent.Id)
	}

	//rawStatsCache.lastDiscoveryTime
	var lastTime *time.Time
	var taskPrevStats map[string]*TaskStatistics
	if rawStatsCache != nil {
		if rawStatsCache.lastDiscoveryTime != nil {
			lastTime = rawStatsCache.lastDiscoveryTime
		}
		agentStats, exists := rawStatsCache.nodeStats[agent.Id]
		if exists {
			taskPrevStats = agentStats.taskStats //lastCycleStats.TaskStats
		}
	}

	// Create new ResourceUseStats for the agent
	agent.ResourceUseStats = &data.CalculatedUse{}

	// Iterate over the list and compute the task vcpu and vmem used values
	// The used values for the agent is the sum of the used for each task
	glog.V(3).Infof("--------------------------------> Agent %s : %s\n", agent.Id, agent.IP)
	glog.V(3).Infof("Before agent resource stats: [capacity %+v] [usage %+v]\n", agent.Resources, agent.ResourceUseStats)
	for idx, _ := range arrOfExec {
		executor := arrOfExec[idx]
		glog.V(3).Infof("---------> Executor %+v\n", executor)
		task := findTask(executor.Source, executor.Id, agent.TaskMap)
		//
		if task == nil { // check for the task object corresponding to this taskId
			glog.Warningf("unknown task id %s", executor.Source+"::"+executor.Id)
			continue
		}
		glog.V(3).Infof("Task %s::%s\n", task.Name, task.Id)
		var currStats, prevStats data.Statistics
		currStats = executor.Statistics
		task.RawStatistics = currStats //save for next cycle
		glog.V(3).Infof("Initial Task: [capacity %+v] [usage %+v]\n", task.Resources, task.RawStatistics)
		if taskPrevStats != nil {
			_, ok := taskPrevStats[task.Id]
			if ok {
				prevStats = taskPrevStats[task.Id].rawStats
			} else {
				glog.V(3).Infof("Previous cycle stats not available for " + agent.Id + "::" + task.Id)
			}
		}
		usedCPUFraction := calculateCPU(task.Id, agent.Id, &prevStats, &currStats, lastTime)

		usedCPU := usedCPUFraction * agent.Resources.CPUUnits * float64(1000) //CPU_MULTIPLIER
		agent.ResourceUseStats.CPUMHz += usedCPU                              // save the accumulated value in the agent
		glog.V(3).Infof("%s usedCPU=%f agent=%f", agent.IP, usedCPU, agent.ResourceUseStats.CPUMHz)
		usedMemBytes := currStats.MemRSSBytes
		usedMemKB := usedMemBytes / data.KB_MULTIPLIER
		agent.ResourceUseStats.MemKB += usedMemKB // save in the agent
		glog.V(3).Infof("%s usedMemBytes=%f agent=%f", agent.IP, usedMemBytes, agent.ResourceUseStats.MemKB)
		// Task capacities - create new ResourceUseStats for the task
		task.ResourceUseStats = &data.CalculatedUse{}
		task.ResourceUseStats.CPUMHz = usedCPU // save in the task
		task.ResourceUseStats.MemKB = usedMemKB
		task.Resources.MemMB = currStats.MemLimitBytes / (data.KB_MULTIPLIER*data.KB_MULTIPLIER)
		task.Resources.CPUUnits = currStats.CPUsLimit
		//task.Resources.Disk = currStats.DiskLimitBytes / float64(1024.00*1024.00)

		glog.Infof("%s::%s : Task resource stats: [capacity %+v] [usage %+v]\n", agent.IP, task.Name, task.Resources, task.ResourceUseStats)

	} // task loop
	//fmt.Printf("Agent resource stats: [capacity %+v] [usage %+v]\n", agent.Resources, agent.ResourceUseStats)
	glog.Infof("%s : Agent resource stats: [capacity %+v] [usage %+v]\n", agent.IP, agent.Resources, agent.ResourceUseStats)
	glog.V(3).Infof("--------------------------------------------------")
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
	glog.V(3).Infof("Previous RawStatistics %+v\n", prevStats)
	if prevStats == nil || lastTime == nil {
		glog.V(3).Infof("previous cycle stats not available for " + agentId + "::" + taskId)
		return data.DEFAULT_VAL
	}

	prevSecs = prevStats.CPUsystemTimeSecs + prevStats.CPUuserTimeSecs
	curSecs = currStats.CPUsystemTimeSecs + currStats.CPUuserTimeSecs
	diffSecs := curSecs - prevSecs
	if diffSecs < 0 {
		diffSecs = data.DEFAULT_VAL
	}

	diffTime := time.Since(*lastTime)
	diffT := diffTime.Seconds()
	usedCPUFraction := diffSecs / diffT
	glog.V(3).Infof("%s: diffSecs=%f diffTime=%t usedCPUFraction=%s", agentId, diffSecs, diffT, usedCPUFraction)
	return usedCPUFraction
}
