package discovery

import (
	"github.com/golang/glog"
	"github.com/turbonomic/mesosturbo/pkg/data"
	"time"
	"fmt"
)

type RawStatsCache struct {
	lastDiscoveryTime *time.Time
	nodeStats         map[string]*NodeStatistics
}

type NodeStatistics struct {
	nodeId string
	//raw statistics from the rest api for each task
	taskStats map[string]*TaskStatistics
}

type TaskStatistics struct {
	taskId   string
	rawStats data.Statistics
}

// Return the raw statistics collected for the given task
func (rawStatsCache *RawStatsCache) GetTaskStats(nodeId, taskId string) *TaskStatistics {
	var taskStats *TaskStatistics
	nodeStatsMap, exists := rawStatsCache.nodeStats[nodeId]
	if !exists {
		glog.Infof("previous cycle stats not available for node %s", nodeId)
		return nil
	}
	taskStatsMap := nodeStatsMap.taskStats
	taskStats, ok := taskStatsMap[taskId]
	if !ok {
		glog.Infof("previous cycle stats not available for " + nodeId + "::" + taskId)
		return nil
	}

	return taskStats
}

func (rawStatsCache *RawStatsCache) log() {
	fmt.Printf("RawStatsCache last discovery time : %s\n", rawStatsCache.lastDiscoveryTime)
	for node, nodeStats := range rawStatsCache.nodeStats {
		fmt.Printf("Node %s\n", node)
		for _, taskStats := range nodeStats.taskStats {
			fmt.Printf("   TaskStats %+v\n", taskStats)
		}
	}
}

func CreateCopy(origCache *RawStatsCache, nodeIdList []string) *RawStatsCache {
	copyCache := &RawStatsCache{
		lastDiscoveryTime: origCache.lastDiscoveryTime,
	}
	copyCache.nodeStats = make(map[string]*NodeStatistics)

	origNodeStatsMap := origCache.nodeStats
	for _, nodeId := range nodeIdList {
		nodeStats, exists := origNodeStatsMap[nodeId]
		if exists {
			copyCache.nodeStats[nodeId] = nodeStats
		}
	}

	return copyCache
}

func (rawStatsCache *RawStatsCache) RefreshCache(mesosMaster *data.MesosMaster) {
	// Save discovery stats
	currentTime := time.Now()
	rawStatsCache.lastDiscoveryTime = &currentTime
	// make a new map for all nodes to clear previous entries
	rawStatsCache.nodeStats = make(map[string]*NodeStatistics)
	for _, agent := range mesosMaster.AgentMap {
		rawStatsCache.nodeStats[agent.Id] = &NodeStatistics{
			nodeId:    agent.Id,
			taskStats: make(map[string]*TaskStatistics),
		}
		taskMap := agent.TaskMap
		nodeStats := rawStatsCache.nodeStats[agent.Id]
		for _, task := range taskMap {
			nodeStats.taskStats[task.Id] = &TaskStatistics{
				taskId:   task.Id,
				rawStats: task.RawStatistics,
			}
		}
	}
}
