package master

import (
	"github.com/golang/glog"
	"fmt"
	"encoding/json"
	"github.com/turbonomic/mesosturbo/pkg/data"
)

// Endpoint paths for Apache Mesos Master
type DCOSEndpointPath string

const (
	DCOS_StatePath      DCOSEndpointPath = "/mesos/state"
	DCOS_FrameworksPath DCOSEndpointPath = "/mesos/frameworks"
	DCOS_TasksPath      DCOSEndpointPath = "/mesos/tasks"
	DCOS_LoginPath      DCOSEndpointPath = "/acs/api/v1/auth/login"
)


// Endpoint paths for Apache Agent
type DCOSAgentEndpointPath string
const (
	DCOS_StatsPath      DCOSAgentEndpointPath = "/monitor/statistics.json"
)

// Endpoint store containing endpoint and parsers for DCOS Mesos Master
func NewDCOSMesosEndpointStore() *MasterEndpointStore {
	store := &MasterEndpointStore{
		EndpointMap: make(map[MasterEndpointName]*MasterEndpoint),
	}

	epMap := store.EndpointMap

	epMap[Login] = &MasterEndpoint{
		EndpointName: string(Login),
		EndpointPath: string(DCOS_LoginPath),
		Parser:       &DCOSLoginParser{},
	}

	epMap[State] = &MasterEndpoint{
		EndpointName: string(State),
		EndpointPath: string(DCOS_StatePath),
		Parser:       &GenericMasterStateParser{},
	}
	epMap[Frameworks] = &MasterEndpoint{
		EndpointName: string(Frameworks),
		EndpointPath: string(DCOS_FrameworksPath),
	}
	epMap[Tasks] = &MasterEndpoint{
		EndpointName: string(Tasks),
		EndpointPath: string(DCOS_TasksPath),
	}

	return store
}


// Endpoint store containing endpoint and parsers for Apache Mesos Master
func NewDCOSAgentEndpointStore() *AgentEndpointStore {
	store := &AgentEndpointStore{
		EndpointMap: make(map[AgentEndpointName]*AgentEndpoint),
	}

	epMap := store.EndpointMap

	epMap[Stats] = &AgentEndpoint{
		EndpointName: string(Stats),
		EndpointPath: string(DCOS_StatsPath),
		Parser:       &GenericAgentStatsParser{},
	}
	return store
}


// ============================================ DCOS Login Parser ======================================================

type DCOSLoginParser struct {
	Message string
}

const DCOSLoginParserClass = "[DCOSLoginParser]"

func (parser *DCOSLoginParser) parseResponse(resp []byte) error {
	glog.V(3).Infof("[DCOSLoginParser] in parseAPIStateResponse")
	var tokenResp = new(data.TokenResponse)
	err := json.Unmarshal(resp, &tokenResp)
	if err != nil {
		return fmt.Errorf("[DCOSLoginParser] error in json unmarshalling login response :%s", err)
	}
	parser.Message = tokenResp.Token
	return nil
	//return errors.New("[DCOSLoginParser] DCOS authorization credentials are not correct : " + string(content)))
}

func (parser *DCOSLoginParser) GetMessage() interface{} {
	glog.V(3).Infof("[DCOSLoginParser] DCOS Login Token %s\n", parser.Message)
	return parser.Message
}
