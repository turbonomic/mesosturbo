package data

import (
	"time"
)

type MesosMaster struct {
	Id     string
	Leader string
	Pid    string
	//cluster
	Cluster      ClusterInfo
	AgentMap     map[string]*Agent
	FrameworkMap map[string]*Framework
	TaskMap      map[string]*Task
	TimeSinceLastDisc *time.Time
	AgentList 	[]*Agent
}
