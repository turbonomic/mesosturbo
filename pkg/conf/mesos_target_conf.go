package conf

import (
	"io/ioutil"
	"encoding/json"
	"github.com/golang/glog"
	"fmt"
	"github.com/turbonomic/turbo-go-sdk/pkg/proto"
	"github.com/turbonomic/turbo-go-sdk/pkg/probe"
)

const (
	DEFAULT_MASTER_PORT string = "5050"
)

// Configuration Parameters to connect to a Mesos Target
type MesosTargetConf struct {
	// Master related - Apache or DCOS Mesos
	Master         MesosMasterType  `json:"master"`
	MasterIPPort   string 		`json:"master-ipport"`
	MasterUsername string		`json:"master-user"`
	MasterPassword string		`json:"master-pwd"`
	LeaderIP       string
	LeaderPort     string

	FrameworkConf	`json:"framework,omitempty"`

	// Login Token obtained from the Mesos Master
	Token          string
}

type FrameworkConf struct {
	// Scheduler or Framework related
	Framework      	   MesosFrameworkType   `json:"framework"`
	FrameworkIP        string		`json:"framework-ip"`
	FrameworkPort      string		`json:"framework-port"`
	FrameworkUser      string		`json:"framework-user"`
	FrameworkPassword  string	        `json:"framework-pwd"`
}

type ActionFrameworkConf struct {
	// Action Executor related to using Layer-X
	ActionIP       string
	ActionPort     string
	ActionAPI      string
}

type AgentConf struct {
	AgentIP       string
	AgentPort     string
}

// Create a new MesosTargetConf from a json file.
// Return null config if the there are errors loading or parsing the file
func NewMesosTargetConf(targetConfigFilePath string) (*MesosTargetConf, error) {
	glog.Infof("[MesosClientConf] Target configuration from %s", targetConfigFilePath)
	config, err := readConfig(targetConfigFilePath)
	if err != nil {
		return nil, fmt.Errorf("[[MesosTargetConf] Error reading config "+ targetConfigFilePath + " : %s" , err)
	}

	// Provide framework ip for dcos mesos
	dcosMesosTargetConf(config)

	// Validate
	ok, err := config.validate()
	if !ok {
		return nil, fmt.Errorf("[MesosTargetConf] Invalid config : %s",  err)
	}
	glog.Infof("[MesosTargetConf] Mesos Target Config: %+v\n", config)
	return config, nil
}

// Provide framework ip for dcos mesos
func dcosMesosTargetConf(config *MesosTargetConf) {
	// For DCOS Mesos, framework IP is not specified
	if (config.FrameworkIP == "") {
		config.FrameworkIP = config.LeaderIP
		config.FrameworkPort = config.LeaderPort
		config.Framework = DCOS_Marathon
	}
}

// Create a new MesosTargetConf from a given list of AccountValues
// Return null config if the there are errors validating the config
func CreateMesosTargetConf(targetType string, accountValues []*proto.AccountValue) (probe.TurboTargetConf, error) {
	// TODO: revisit if the targetType parameter should be string or MesosMasterType
	var mesosMasterType MesosMasterType
	if targetType == string(Apache) {
		mesosMasterType = Apache
	} else if targetType == string(DCOS) {
		mesosMasterType = DCOS
	} else {
		glog.Errorf("Unknown Mesos Master Type " , targetType)
		return nil, fmt.Errorf("Unknown Mesos Master Type %s" , targetType)
	}
	config := &MesosTargetConf{
		Master: mesosMasterType,
	}
	for _, accVal := range accountValues {
		if *accVal.Key ==  string(MasterIPPort) {
			config.MasterIPPort = *accVal.StringValue
		}
		if *accVal.Key ==  string(MasterUsername) {
			config.MasterUsername = *accVal.StringValue
		}
		if *accVal.Key ==  string(MasterPassword) {
			config.MasterPassword = *accVal.StringValue
		}
		if *accVal.Key ==  string(FrameworkIP) {
			config.FrameworkIP = *accVal.StringValue
		}
		if *accVal.Key ==  string(FrameworkPort) {
			config.FrameworkPort = *accVal.StringValue
		}
		if *accVal.Key ==  string(FrameworkUsername) {
			config.FrameworkUser = *accVal.StringValue
		}
		if *accVal.Key ==  string(FrameworkPassword) {
			config.FrameworkPassword = *accVal.StringValue
		}
	}

	// Provide framework ip for dcos mesos
	dcosMesosTargetConf(config)
	// Validate
	ok, err := config.validate()
	if !ok {
		glog.Errorf("[MesosTargetConf] Invalid config : %s", err)
		return nil, fmt.Errorf("[MesosTargetConf] Invalid config : %s", err)
	}
	return config, nil
}


// Get the Account Values to create VMTTarget in the turbo server corresponding to this client
func (mesosConf *MesosTargetConf) GetAccountValues() []*proto.AccountValue {
	if _, err := mesosConf.validate(); err != nil {
		glog.Infof("[GetAccountValues] mesos target config is not valid %s", err)
		return nil
	}
	var accountValues []*proto.AccountValue
	// Convert all parameters in clientConf to AccountValue list
	ipportProp := string(MasterIPPort)
	accVal := &proto.AccountValue{
		Key: &ipportProp,
		StringValue: &mesosConf.MasterIPPort,
	}
	accountValues = append(accountValues, accVal)

	userProp := string(MasterUsername)
	accVal = &proto.AccountValue{
		Key: &userProp,
		StringValue: &mesosConf.MasterUsername,
	}
	accountValues = append(accountValues, accVal)

	pwdProp := string(MasterPassword)
	accVal = &proto.AccountValue{
		Key: &pwdProp,
		StringValue: &mesosConf.MasterPassword,
	}
	accountValues = append(accountValues, accVal)

	if mesosConf.Master == Apache {
		fmIpProp := string(FrameworkIP)
		accVal = &proto.AccountValue{
			Key: &fmIpProp,
			StringValue: &mesosConf.FrameworkIP,
		}
		accountValues = append(accountValues, accVal)

		fmPort := string(FrameworkPort)
		accVal = &proto.AccountValue{
			Key: &fmPort,
			StringValue: &mesosConf.FrameworkPort,
		}
		accountValues = append(accountValues, accVal)

		fmUserProp := string(FrameworkUsername)
		accVal = &proto.AccountValue{
			Key: &fmUserProp,
			StringValue: &mesosConf.FrameworkUser,
		}
		accountValues = append(accountValues, accVal)

		fmPwd := string(FrameworkPassword)
		accVal = &proto.AccountValue{
			Key: &fmPwd,
			StringValue: &mesosConf.FrameworkPassword,
		}
		accountValues = append(accountValues, accVal)
	}

	glog.V(2).Infof("[GetAccountValues] account values %s\n",  accountValues)

	return accountValues
}

func (conf *MesosTargetConf) validate() (bool, error) {
	if (conf.Master == "") {
		return false, fmt.Errorf("Mesos Master Type is required :  %+v" + fmt.Sprint(conf))
	}

	if (conf.MasterIPPort == "") {
		return false, fmt.Errorf("Mesos Master IP:Port list is required :  %+v" + fmt.Sprint(conf))
	}
	return true, nil
}

// Get the config from file.
func readConfig(path string) (*MesosTargetConf, error) {
	file, e := ioutil.ReadFile(path)
	if e != nil {
		return nil, fmt.Errorf("File error:  %s", e)
	}
	var config MesosTargetConf
	err := json.Unmarshal(file, &config)

	if err != nil {
		return nil, fmt.Errorf(string(file) + " \nUnmarshall error : %s" , err)
	}
	return &config, nil
}
