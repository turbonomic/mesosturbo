package data

type MesosAPIResponse struct {
	Leader      string `json:"leader"`
	LeaderInfo struct {
			   Id string `json:"id"`
			   Pid string `json:"pid"`
			   Port  int `json:"port"`
			   Hostname string `json:"hostname"`
	       } `json:"leader_info"`
	Version     string `json:"version"`
	Id          string `json:"id"`
	ClusterName string `json:"cluster"`

	//ActivatedSlaves   float64     `json:"activated_slaves"`
	//DeActivatedSlaves float64     `json:"deactivated_slaves"`
	Agents            []Agent     `json:"slaves"`
	Frameworks        []Framework `json:"frameworks"`
}
type Agent struct {
	Id               string    `json:"id"`
	Pid              string    `json:"pid"`
	Hostname         string    `json:"hostname"`
	Resources        Resources `json:"resources"`
	UsedResources    Resources `json:"used_resources"`
	OfferedResources Resources `json:"offered_resources"`
	Name             string    `json:"hostname"`
	//Calculated       CalculatedUse
	//Attributes       Attributes `json:"attributes"`
	Active           bool   `json:"active"`
	Version          string `json:"version"`
	// -------- Computed parameters
	ClusterName      string
	IP               string // parsed ip for the Slave
	PortNum          string
	ResourceUseStats *CalculatedUse
	TaskMap          map[string]*Task
}

// assumed to be framework from slave , not from master state
type Framework struct {
	Id        string    `json:"id"`
	Name      string    `json:"name"`
	Pid       string    `json:"pid"`
	Hostname  string    `json:"hostname"`
	Active    bool      `json:"active"`
	Role      string    `json:"role"`
	Resources Resources `json:"resources"`
	Tasks     []Task    `json:"tasks"`
}

type Task struct {
	FrameworkId string    `json:"framework_id"`
	SlaveId     string    `json:"slave_id"`
	Container   Container `json:"container"`
	Discovery        Discovery `json:"discovery"`
	ExecutorId string `json:"executor_id"`
	Id         string `json:"id"`
	//Labels           []Label   `json:"labels"`
	Name      string    `json:"name"`
	Resources Resources `json:"resources"`
	State     string    `json:"state"`
	//Statuses         []Status  `json:"statuses"`
	//--------- Computed Stats
	RawStatistics    Statistics //read by querying the agent
	ResourceUseStats *CalculatedUse
}

type Discovery struct {
	Name       string    `json:"name"`
	Ports      DiscPorts `json:"ports"`
	Visibility string    `json:"visibility"`
}
type DiscPorts struct {
	Ports []PortInfo `json:"ports"`
}
type PortInfo struct {
	Number   int64  `json:"number"`
	Protocol string `json:"protocol"`
}
//
//type NetworkInfos struct {
//	Infos []NetworkInfo `json:"network_infos"`
//}
//
//type NetworkInfo struct {
//	IPaddress string `json:"ip_address"`
//}
//
//type Status struct {
//	Container_Status NetworkInfos `json:"container_status"`
//	State            string       `json:"state"`
//	//	Timestamp `json:"timestamp"`
//}
//
//type Label struct {
//	//	Key   string `json:"key"`
//	//	Value string `json:"value"`
//	State string `json:"key"`
//}
//
//type Attributes struct {
//	Rack string `json:"rack"`
//}

type Resources struct {
	Disk     float64 `json:"disk"`
	MemMB    float64 `json:"mem"`
	CPUUnits float64 `json:"cpus"`
	Ports    string  `json:"ports"`
}

// ======================= Agent Rest API Response =========================
type Statistics struct {
	CPUsLimit         float64 `json:"cpus_limit"`
	MemLimitBytes     float64 `json:"mem_limit_bytes"`
	MemRSSBytes       float64 `json:"mem_rss_bytes"`
	CPUsystemTimeSecs float64 `json:"cpus_system_time_secs"`
	CPUuserTimeSecs   float64 `json:"cpus_user_time_secs"`
	DiskLimitBytes    float64 `json:"disk_limit_bytes"`
	DiskUsedBytes     float64 `json:"disk_used_bytes`
}

type Executor struct {
	Id          string     `json:"executor_id"`
	FrameworkId string     `json:"framework_id"`
	Source      string     `json:"source"`
	Statistics  Statistics `json:"statistics"`
}

//// ================= Frameworks Rest API Response ====================
type App struct {
	Name         string     `json:"id"`
	Constraints  [][]string `json:"constraints"`
	RequirePorts bool       `json:"requirePorts"`
	Container    Container  `json:"container"`
}

//// ==================== Container =================
type Container struct {
	Docker ContDocker `json:"docker"`
	Type   string     `json"type"`
}

type ContDocker struct {
	ForcePullImage bool          `json:"force_pull_image"`
	Image          string        `json:"image"`
	Network        string        `json:"network"`
	Privileged     bool          `json:"privileged"`
	PortMappings   []PortMapping `json:"portMappings"`
}

type PortMapping struct {
	ContainerPort int `json:"containerPort"`
	HostPort      int `json:"hostPort"`
	ServicePort   int `json:"servicePort"`
}

// ==============================================

type TokenResponse struct {
	Token string `json:"token"`
}

type ClusterInfo struct {
	ClusterName string
	MasterIP    string
	MasterId    string
}
type PortUtil struct {
	Number   float64
	Capacity float64
	Used     float64
}

type CalculatedUse struct {
	Disk   float64
	MemKB  float64
	CPUMHz float64
	//UsedPorts map[string]PortUtil
}
