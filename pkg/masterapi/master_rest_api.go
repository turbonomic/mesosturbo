package master

import (
	"bytes"
	"github.com/golang/glog"
	"net/http"

	"github.com/turbonomic/mesosturbo/pkg/conf"
	"errors"
	"io/ioutil"
	"encoding/json"
	"fmt"
	"github.com/turbonomic/mesosturbo/pkg/data"
)


type MasterEndpointName string

const (
	Login      MasterEndpointName = "login"
	State      MasterEndpointName = "state"
	Frameworks MasterEndpointName = "frameworks"
	Tasks      MasterEndpointName = "tasks"
)

// The endpoints used for making RestAPI calls to the Mesos Master
type MasterEndpoint struct {
	EndpointName string
	EndpointPath string
	Parser       EndpointParser
}

// Store containing the Rest API endpoints for communicating with the Mesos Master
type MasterEndpointStore struct {
	EndpointMap map[MasterEndpointName]*MasterEndpoint
}


// Parser interface for different server messages
type EndpointParser interface {
	//parse(resp *http.Response) error
	parseResponse(resp []byte) error
	GetMessage() interface{}
}


// -----------------------------------------------------------------------------
// Represents the generic client used to connect to a Mesos Master. Implements the MasterRestClient interface
type GenericMasterAPIClient struct {
	// Mesos target configuration
	MesosConf     *conf.MesosTargetConf
	// Endpoint store with the endpoint paths for different rest api calls
	EndpointStore *MasterEndpointStore

	DebugMode 	bool
	DebugProps     map[string]string
}

// Create a new instance of the GenericMasterAPIClient
// @param mesosConf the conf.MesosTargetConf that contains the configuration information for the Mesos Target
// @param epStore    the Endpoint store containing the Rest API endpoints for the Mesos Master
func NewGenericMasterAPIClient(mesosConf *conf.MesosTargetConf, epStore *MasterEndpointStore) MasterRestClient {
	return &GenericMasterAPIClient{
		MesosConf:     mesosConf,
		EndpointStore: epStore,
	}
}

const MesosMasterAPIClientClass = "MesosMasterAPIClient"

// Handle login to the Mesos Client using the path specified for the MasterEndpointName.Login endpoint
func (mesosRestClient *GenericMasterAPIClient) Login() (string, error) {
	glog.V(3).Infof("[GenericMasterAPIClient] Login ...")

	// Execute request
	endpoint, _ := mesosRestClient.EndpointStore.EndpointMap[Login]

	if endpoint == nil {
		return "", nil
	}
	request, err := mesosRestClient.createLoginRequest(endpoint.EndpointPath)

	if err != nil {
		return "", ErrorCreateRequest(MesosMasterAPIClientClass, err)
	}
	glog.V(3).Infof(MesosMasterAPIClientClass +  " : send Login() request : ", request)
	var byteContent []byte
	byteContent, err = executeAndValidateResponse(request, MesosMasterAPIClientClass)
	if err != nil {
		return "", fmt.Errorf(MesosMasterAPIClientClass + " : Login() error :  %s", err)
	}

	// Parse response
	parser := endpoint.Parser
	err = parser.parseResponse(byteContent)
	if err != nil {
		return "", ErrorParseRequest(MesosMasterAPIClientClass, err)
	}

	msg := parser.GetMessage()
	st, ok := msg.(string)
	if ok {
		return st, nil
	}
	return "", ErrorConvertResponse(MesosMasterAPIClientClass, err)
}

// Make a RestAPI call to get the Mesos State using the path specified for the MasterEndpointName.Login endpoint
func (mesosRestClient *GenericMasterAPIClient) GetState() (*data.MesosAPIResponse, error) {
	glog.V(3).Infof("[GenericMasterAPIClient] Get State ...")
	// Execute request
	endpoint, _ := mesosRestClient.EndpointStore.EndpointMap[State]
	request, err := createRequest(endpoint.EndpointPath,
				mesosRestClient.MesosConf.LeaderIP, mesosRestClient.MesosConf.LeaderPort,
				mesosRestClient.MesosConf.Token)
	if err != nil {
		return nil, ErrorCreateRequest(MesosMasterAPIClientClass, err)
	}
	glog.V(3).Infof(MesosMasterAPIClientClass +  " : send GetState() request %s ", request)

	var byteContent []byte
	if mesosRestClient.DebugMode { // Debug mode
		byteContent, err = mesosRestClient.getDebugModeState()
	} else { // Execute Request
		byteContent, err = executeAndValidateResponse(request, MesosMasterAPIClientClass)
	}
	if err != nil {
		return nil, fmt.Errorf(MesosMasterAPIClientClass + " : GetState() error :  %s", err)
	}

	// Parse response
	parser := endpoint.Parser
	err = parser.parseResponse(byteContent)
	if err != nil {
		return nil, ErrorParseRequest(MesosMasterAPIClientClass, err)
	}

	msg := parser.GetMessage()
	st, ok := msg.(*data.MesosAPIResponse)
	if ok {
		return st, nil
	}
	return nil, ErrorConvertResponse(MesosMasterAPIClientClass, err)
}

func createRequest(endpoint, ip, port, token string) (*http.Request, error) {
	fullUrl := "http://" +ip + ":" + port + endpoint	//TODO: handle https requests
	req, err := http.NewRequest("GET", fullUrl, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Add("Content-type", "application/json")
	if token != "" {
		req.Header.Add("Authorization", "token="+token)
	}
	return req, nil
}

func executeAndValidateResponse(request *http.Request, logPrefix string) ([]byte, error) {
	var byteContent []byte
	var resp *http.Response

	client := &http.Client{}
	resp, err := client.Do(request)

	if err != nil {
		return nil, ErrorExecuteRequest(logPrefix, err)
	}

	defer resp.Body.Close()

	if resp == nil {
		return nil, ErrorEmptyResponse(logPrefix)
	}

	// Check response status code
	if resp.Status == "" {
		return nil, errors.New(logPrefix + " Empty response status\n")
	}

	byteContent, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf(logPrefix + " Error in ioutil.ReadAll: %s", err)
	}
	return byteContent, nil
}

func (mesosRestClient *GenericMasterAPIClient) createLoginRequest(endpoint string) (*http.Request, error) {
	var jsonStr []byte
	url := "http://" + mesosRestClient.MesosConf.LeaderIP + endpoint

	// Send user and password
	jsonStr = []byte(`{"uid":"` + mesosRestClient.MesosConf.MasterUsername + `","password":"` + mesosRestClient.MesosConf.MasterPassword + `"}`)

	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonStr))
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "application/json")
	return req, nil
}

func (mesosRestClient *GenericMasterAPIClient) getDebugModeState() ([]byte, error) {
	fmt.Println("========= getDebugModeState() : DEBUG MODE =============")
	filePath, exists := mesosRestClient.DebugProps["file"]
	if !exists {
		return nil, fmt.Errorf("Missing 'file' parameter in debug props:  %s", mesosRestClient.DebugProps)
	}
	byteContent, e := ioutil.ReadFile(filePath)
	if e != nil {
		return nil, fmt.Errorf("File error:  %s", e)
	}
	return byteContent, nil
}

// ========================================= State Request Parser ===================================================

type GenericMasterStateParser struct {
	Message *data.MesosAPIResponse
}

const GenericMasterStateParserClass = "[GenericMasterStateParser]"

func (parser *GenericMasterStateParser) parseResponse(resp []byte) error {
	glog.V(3).Infof("%s in parseAPIStateResponse : %s", GenericMasterStateParserClass, resp)
	if resp == nil {
		return ErrorEmptyResponse(GenericMasterStateParserClass)
	}

	var jsonMesosMaster data.MesosAPIResponse
	err := json.Unmarshal(resp, &jsonMesosMaster)
	if err != nil {
		return fmt.Errorf(GenericMasterStateParserClass + " Error in json unmarshal for state response : %s %+v", err, err)
	}
	parser.Message = &jsonMesosMaster
	return nil
}

func (parser *GenericMasterStateParser) GetMessage() interface{} {
	glog.V(3).Infof(GenericMasterStateParserClass + " Mesos State %s\n", parser.Message)
	return parser.Message
}