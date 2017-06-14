package discovery

import (
	"github.com/turbonomic/mesosturbo/pkg/conf"
	master "github.com/turbonomic/mesosturbo/pkg/masterapi"
	"fmt"
	"strconv"
	"strings"
	"github.com/golang/glog"
	"github.com/turbonomic/mesosturbo/pkg/data"
)
//
//func main() {
//	ipportlist := "10.10.174.92:5050,10.10.174.91:5050,10.10.174.100, 10.10.174.101:5050 "
//	urlList := ParseMasterIPPorts(ipportlist)
//	masteruser := "enlin.xu"
//	masterpwd := "sysdreamworks"
//	NewMesosCluster(conf.Apache, urlList, masteruser, masterpwd)
//}
//
type MesosMasterUrl struct {
	IP string
	Port string
}

type MesosLeader struct {
	leaderConf *conf.MesosTargetConf
	MasterState *data.MesosAPIResponse
}

func NewMesosLeader(mesosMasterType conf.MesosMasterType, urlList []*MesosMasterUrl, masteruser, masterpwd string) (*MesosLeader, error) {
	glog.Infof("Detecting mesos master leader from %s", urlList)
	for _, ipport := range urlList {
		glog.Infof("Check Master IPPort : %s\n", ipport)
		clientConf := &conf.MesosTargetConf{
			Master: mesosMasterType,
			LeaderIP: ipport.IP,
			LeaderPort: ipport.Port,
			MasterUsername: masteruser,
			MasterPassword: masterpwd,
		}

		masterRestClient := master.GetMasterRestClient(clientConf.Master, clientConf)
		if masterRestClient == nil {
			glog.Errorf("Cannot find RestClient for Mesos : " + string(clientConf.Master))
			continue
		}

		// Login to the Mesos Master and save the login token
		token, err := masterRestClient.Login()
		if err != nil {
			glog.Errorf("Error logging to Mesos Master at " +
					clientConf.LeaderIP + "::" + clientConf.LeaderPort + " : ", err)
			continue
		}
		clientConf.Token = token

		mesosState, err := masterRestClient.GetState()
		if err != nil {
			glog.Errorf("Error getting state from master : %s \n", err)
			continue
		}
		glog.V(3).Infof("Mesos API Succeeded for: %++v\n", mesosState.LeaderInfo)
		// Leader is detected
		clientConf.LeaderIP = mesosState.LeaderInfo.Hostname
		clientConf.LeaderPort = strconv.Itoa(mesosState.LeaderInfo.Port)

		mesosLeader := &MesosLeader{}
		mesosLeader.leaderConf = clientConf
		mesosLeader.MasterState = mesosState
		glog.Infof("Detected Mesos Leader: %s::%s\n", mesosLeader.leaderConf.LeaderIP, mesosLeader.leaderConf.LeaderPort)
		return mesosLeader, nil
	}
	return nil, fmt.Errorf("Cannot detect leader using %s", urlList)
}

func ParseMasterIPPorts(ipportlist string) []*MesosMasterUrl {
	// Split on comma.
	ipports := strings.Split(ipportlist, ",")

	var urlList []*MesosMasterUrl
	// Display all elements.
	for i := range ipports {
		fmt.Println("ipport=" + ipports[i])
		result := strings.Split(ipports[i], ":")
		var ipaddress, port string
		ipaddress = strings.TrimSpace(result[0])
		if len(result) < 2 {
			port = conf.DEFAULT_MASTER_PORT
		} else {
			port = strings.TrimSpace(result[1])
		}
		ipport := &MesosMasterUrl{
			IP:ipaddress,
			Port:port,
		}
		urlList = append(urlList, ipport)
	}
	return urlList
} //end


