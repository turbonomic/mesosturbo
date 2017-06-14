package master

import (
	"github.com/golang/glog"
	"github.com/turbonomic/mesosturbo/pkg/conf"
	"github.com/turbonomic/mesosturbo/pkg/data"
)


// Interface for the client to handle Rest API communication with the Mesos Master
type MasterRestClient interface {
	Login() (string, error)
	GetState() (*data.MesosAPIResponse, error)
}

// Interface for the client to handle Rest API communication with the Agent
type AgentRestClient interface {
	GetStats() ([]data.Executor, error)
}

// Get the Rest API client to handle communication with the Mesos Master
// Returns the MasterRestClient for the supported specific Mesos vendor type, else nil
func GetMasterRestClient(mesosType conf.MesosMasterType, masterConf *conf.MasterConf) MasterRestClient {
	var endpointStore *MasterEndpointStore
	if mesosType == conf.Apache {
		glog.V(2).Infof("[GetMasterRestClient] Creating Apache Mesos Master Client")
		endpointStore = NewApacheMesosEndpointStore()
	} else if mesosType == conf.DCOS {
		glog.V(2).Infof("[GetMasterRestClient] Creating DCOS Mesos Master Client")
		endpointStore = NewDCOSMesosEndpointStore()
	}

	if endpointStore == nil {
		glog.Errorf("[GetMasterRestClient] Unsupported Mesos Master ", mesosType)
		return nil
	}

	return NewGenericMasterAPIClient(masterConf, endpointStore)
}

// Get the Rest API client to handle communication with the Agent
// Returns the AgentRestClient for the supported specific Mesos vendor type, else nil
func GetAgentRestClient(mesosType conf.MesosMasterType, agentConf *conf.AgentConf, masterConf *conf.MasterConf) AgentRestClient {
	var endpointStore *AgentEndpointStore
	if mesosType == conf.Apache {
		glog.V(2).Infof("[GetAgentRestClient] Creating Apache Agent Client")
		endpointStore = NewApacheAgentEndpointStore()
	} else if mesosType == conf.DCOS {
		glog.V(2).Infof("[GetAgentRestClient] Creating DCOS Agent Client")
		endpointStore = NewDCOSAgentEndpointStore()
	}

	if endpointStore == nil {
		glog.Errorf("[GetAgentRestClient] Unsupported Mesos Master ", mesosType)
		return nil
	}

	return NewGenericAgentAPIClient(agentConf, masterConf, endpointStore)
}


