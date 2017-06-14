package master

import (
	"github.com/turbonomic/mesosturbo/pkg/conf"
	"github.com/golang/glog"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"github.com/turbonomic/mesosturbo/pkg/data"
)

type AgentEndpointName string
const (
	Stats  AgentEndpointName = "stats"
)

// The endpoints used for making RestAPI calls to the Agent
type AgentEndpoint struct {
	EndpointName string
	EndpointPath string
	Parser       EndpointParser
}

// Store containing the Rest API endpoints for communicating with the Agent
type AgentEndpointStore struct {
	EndpointMap map[AgentEndpointName]*AgentEndpoint
}

// ==========================================================================
// Represents the generic client used to connect to a Agent
type GenericAgentAPIClient struct {
	MasterConf *conf.MesosTargetConf
	// Mesos target configuration
	AgentConf     *conf.AgentConf
	// Endpoint store with the endpoint paths for different rest api calls
	EndpointStore *AgentEndpointStore

	DebugMode bool
	DebugProps     map[string]string
}

// Create a new instance of the GenericMasterAPIClient
// @param AgentConf the conf.AgentConf that contains the configuration information for the Agent
// @param epStore    the Endpoint store containing the Rest API endpoints for the Agent
func NewGenericAgentAPIClient(agentConf *conf.AgentConf, mesosConf *conf.MesosTargetConf, epStore *AgentEndpointStore) *GenericAgentAPIClient{
	return &GenericAgentAPIClient{
		MasterConf: mesosConf,
		AgentConf:     agentConf,
		EndpointStore: epStore,
	}
}

const AgentAPIClientClass = "[AgentAPIClient] "

// Make a RestAPI call to get the Mesos State using the path specified for the MasterEndpointName.Login endpoint
func (agentRestClient *GenericAgentAPIClient) GetStats() ([]data.Executor, error) {
	glog.V(3).Infof(AgentAPIClientClass + "Get Stats ...")
	// Execute request
	endpoint, _ := agentRestClient.EndpointStore.EndpointMap[Stats]
	request, err := createRequest(endpoint.EndpointPath,
					agentRestClient.AgentConf.AgentIP, string(agentRestClient.AgentConf.AgentPort),
					agentRestClient.MasterConf.Token)
	if err != nil {
		return nil, ErrorCreateRequest(AgentAPIClientClass, err)
	}
	glog.V(3).Infof(AgentAPIClientClass + ": send GetStats() request %s ", request)

	var byteContent []byte
	if agentRestClient.DebugMode { // Debug mode
		byteContent, err = agentRestClient.getDebugModeStats()
	} else { // Execute Request
		byteContent, err = executeAndValidateResponse(request, AgentAPIClientClass)
	}

	if err != nil {
		return nil, fmt.Errorf(AgentAPIClientClass + " : GetStats() error :  %s", err)
	}

	// Parse response
	parser := endpoint.Parser
	err = parser.parseResponse(byteContent)
	if err != nil {
		return nil, ErrorParseRequest(AgentAPIClientClass, err)
	}

	msg := parser.GetMessage()
	executorList, ok := msg.([]data.Executor)
	if ok {
		return executorList, nil
	}
	return nil, ErrorConvertResponse(AgentAPIClientClass, err)
}


func (agentRestClient *GenericAgentAPIClient) getDebugModeStats() ([]byte, error) {
	fmt.Println("========= getDebugModeStats() : DEBUG MODE =============")
	filePath, exists := agentRestClient.DebugProps["file"]
	if !exists {
		return nil, fmt.Errorf("Missing 'file' parameter in debug props:  %s", agentRestClient.DebugProps)
	}
	byteContent, e := ioutil.ReadFile(filePath)
	if e != nil {
		return nil, fmt.Errorf("File error:  %s", e)
	}
	return byteContent, nil
}
// =============================================================================
type GenericAgentStatsParser struct {
	Message []data.Executor
}

const GenericAgentStatsParserClass = "[GenericAgentStatsParser] "

func (parser *GenericAgentStatsParser) parseResponse(resp []byte) error {
	glog.V(3).Infof(GenericAgentStatsParserClass + "in parse Agent Stats")
	if resp == nil {
		return ErrorEmptyResponse(GenericAgentStatsParserClass)
	}
	var usedRes = new([]data.Executor)
	err := json.Unmarshal(resp, &usedRes)
	if err != nil {
		glog.Errorf("JSON error %s", err)
		return fmt.Errorf(GenericAgentStatsParserClass + " Error in json unmarshal for stats response : %s ", err)
	}
	parser.Message = *usedRes
	return nil
}

func (parser *GenericAgentStatsParser) GetMessage() interface{} {
	glog.V(3).Infof(GenericAgentStatsParserClass + "Agent Stats %s\n", parser.Message)
	return parser.Message
}


