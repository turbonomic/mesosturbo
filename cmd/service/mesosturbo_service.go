package service

import (
	"github.com/golang/glog"
	"os"
	"github.com/turbonomic/turbo-go-sdk/pkg/service"
	"github.com/turbonomic/mesosturbo/pkg/conf"
	"github.com/turbonomic/mesosturbo/pkg/discovery"
	mesos "github.com/turbonomic/mesosturbo/pkg/probe"
	"github.com/turbonomic/turbo-go-sdk/pkg/probe"
	"github.com/spf13/pflag"
	"fmt"
	"github.com/turbonomic/turbo-go-sdk/pkg/mediationcontainer"
)


// VMTServer has all the context and params needed to run a Scheduler
type MesosTurboService struct {
	LogVersion         int
	MesosMasterConfig  string	//path to the mesos master config
	TurboCommConfig    string	// path to the turbo communication config file

	// config for mesos
	Master             string
	MasterIPPort       string
	MasterUsername     string
	MasterPassword     string

	// config for turbo server
	TurboServerUrl     string
	OpsManagerUsername string
	OpsManagerPassword string
}

// NewVMTServer creates a new VMTServer with default parameters
func NewMesosTurboService() *MesosTurboService {
	s := MesosTurboService{}
	return &s
}

// AddFlags adds flags for a specific VMTServer to the specified FlagSet
func (s *MesosTurboService) AddFlags(fs *pflag.FlagSet) {
	fs.IntVar(&s.LogVersion, "v", s.LogVersion, "Log level")
	fs.StringVar(&s.MesosMasterConfig, "mesosconfig", s.MesosMasterConfig, "Path to the mesos config file.")
	fs.StringVar(&s.TurboCommConfig, "turboconfig", s.TurboCommConfig, "Path to the turbo config flag.")

	fs.StringVar(&s.Master, "mesostype", s.Master, "Mesos Master Type 'Apache Mesos'|'Mesosphere DCOS'")
	fs.StringVar(&s.MasterIPPort, "masteripport", s.MasterIPPort, "Comma separated list of IP:port of each Mesos Master in the cluster")
	fs.StringVar(&s.MasterUsername, "masteruser", s.MasterUsername, "User for the Mesos Master")
	fs.StringVar(&s.MasterPassword, "masterpwd", s.MasterPassword, "Password for the Mesos Master")

	fs.StringVar(&s.TurboServerUrl, "turboserverurl", s.TurboServerUrl, "Url for Turbo Server")
	fs.StringVar(&s.OpsManagerUsername, "opsmanagerusername", s.OpsManagerUsername, "Username for Ops Manager")
	fs.StringVar(&s.OpsManagerPassword, "opsmanagerpassword", s.OpsManagerPassword, "Password for Ops Manager")
}

// Run runs the specified VMTServer.  This should never exit.
func (s *MesosTurboService) Run(_ []string) error {

	probeCategory := string(conf.CloudNative)

	targetConf :=  s.MesosMasterConfig
	turboCommConf := s.TurboCommConfig

	// ----------- Mesos Target Config
	var mesosTargetConf *conf.MesosTargetConf
	var mesosConfErr error

	if targetConf != "" {
		mesosTargetConf, mesosConfErr = conf.NewMesosTargetConf(targetConf)
	} else {
		var master conf.MesosMasterType
		if s.Master == string(conf.Apache) {
			master = conf.Apache
		} else if s.Master == string(conf.DCOS){
			master = conf.DCOS
		} else {
			mesosConfErr = fmt.Errorf("Invalid Mesos Master Type")
		}
		if (master == "") {
			mesosConfErr = fmt.Errorf("Mesos Master Type is required")
		}

		if (s.MasterIPPort == "") {
			mesosConfErr = fmt.Errorf("Mesos Master IP::Port list is required")
		}

		mesosTargetConf = &conf.MesosTargetConf{
			Master: master,
			MasterIPPort: s.MasterIPPort,
			MasterUsername: s.MasterUsername,
			MasterPassword: s.MasterPassword,
		}
	}
	if mesosTargetConf == nil || mesosConfErr != nil {
		glog.Errorf("Cannot start Mesos TAP service, invalid target config : %s\n", mesosConfErr.Error())
		os.Exit(1)
	}

	mesosMasterType := mesosTargetConf.Master

	// ---------- Turbo server config
	var turboCommConfigData *service.TurboCommunicationConfig
	var turboConfErr error
	if turboCommConf != "" {
		turboCommConfigData, turboConfErr = service.ParseTurboCommunicationConfig(turboCommConf)
	} else {
		wsConfig := mediationcontainer.WebSocketConfig {
		}

		servermeta := mediationcontainer.ServerMeta{
			TurboServer: s.TurboServerUrl,
		}
		restApiConfig := service.RestAPIConfig{
			OpsManagerUsername: s.OpsManagerUsername,
			OpsManagerPassword: s.OpsManagerPassword,
		}

		turboCommConfigData = &service.TurboCommunicationConfig {
			servermeta,
			wsConfig,
			restApiConfig,
		}
		turboConfErr = turboCommConfigData.ValidateTurboCommunicationConfig()
	}

	if turboCommConfigData == nil || turboConfErr != nil {
		glog.Errorf("Cannot start Mesos TAP service, invalid turbo server communication config : %s\n", turboConfErr.Error())
		os.Exit(1)
	}

	// ============================================
	// Mesos Probe Registration Client
	registrationClient := mesos.NewRegistrationClient(mesosMasterType)

	// Mesos Probe Discovery Client
	discoveryClient, err := discovery.NewDiscoveryClient(mesosMasterType, mesosTargetConf)

	if err != nil {
		glog.Errorf("Error creating discovery client for "+string(mesosMasterType)+"::"+mesosTargetConf.LeaderIP +"\n", err.Error())
		os.Exit(1)
	}

	mesosTarget := mesosTargetConf.MasterIPPort
	tapService, err :=
		service.NewTAPServiceBuilder().
			WithTurboCommunicator(turboCommConfigData).
			WithTurboProbe(probe.NewProbeBuilder(string(mesosMasterType), probeCategory).
			RegisteredBy(registrationClient).
			DiscoversTarget(mesosTarget, discoveryClient)).
			Create()

	if err != nil {
		glog.Errorf("Error creating TAP Service : ", err)
	}

	// Connect to the Turbo server
	go tapService.ConnectToTurbo()
	glog.Infof("Connected to Turbo")

	select {}
	glog.Fatal("this statement is unreachable")
	panic("unreachable")
}
