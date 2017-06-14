package discovery

import (
	"github.com/turbonomic/mesosturbo/pkg/conf"
	master "github.com/turbonomic/mesosturbo/pkg/masterapi"
	"fmt"
	"strings"
	"github.com/golang/glog"
	"github.com/turbonomic/mesosturbo/pkg/data"
	"strconv"
)


type MASTER_IP_PORT string
// Structure representing the Master acts as the leader in the cluster
type MesosLeader struct {
	// Mesos Target configuration provided to the mesosturbo service
	targetConf       *conf.MesosTargetConf

	// Map of master IP:Port and the configuration for that master
	masterConfMap map[MASTER_IP_PORT]*conf.MasterConf

	// The mesos state response
	MasterState      *data.MesosAPIResponse

	// Configuration of the current leader in the cluster
	leaderConf	*conf.MasterConf
	// Rest API Client for the current leader in the cluster
	leaderRestClient master.MasterRestClient
}

// Create new instance of MesosLeader using the given target Conf that contains
// the list of IP:Port and credentials for login for each master
func NewMesosLeader(targetConf *conf.MesosTargetConf) (*MesosLeader, error) {
	glog.V(3).Infof("Creating mesos leader %++v", targetConf)

	// Create the Mesos leader
	mesosLeader := &MesosLeader{}
	mesosLeader.targetConf = targetConf
	mesosLeader.masterConfMap = make(map[MASTER_IP_PORT]*conf.MasterConf)

	// Parse the list of IP:Port into an array of MasterSerivceConf
	confList := parseMasterIPPorts(targetConf.Master, targetConf.MasterIPPort)
	for _, masterConf := range confList {
		mapkey := strings.Join([]string{masterConf.MasterIP, masterConf.MasterPort}, ":")
		mesosLeader.masterConfMap[MASTER_IP_PORT(mapkey)] = masterConf
	}

	// Detect the leader by iterating over the list of IP:Port
	err := mesosLeader.updateMesosLeader()
	if err != nil {
		return nil, err
	}
	return mesosLeader, err
}

// Routine to detect the leader by logging and issuing the get state rest api call
func (mesosLeader *MesosLeader) updateMesosLeader() (error){
	// The target info originally created by the probe
	targetConf := mesosLeader.targetConf
	glog.V(3).Infof("Detecting mesos master leader from %s", targetConf.MasterIPPort)

	// Create the Mesos leader by iterating over the list of IP:Port
	for _, masterConf := range mesosLeader.masterConfMap {
		glog.Infof("Checking master conf : %s\n", masterConf)

		// Get RestAPIClient for this master and login
		var masterRestClient master.MasterRestClient
		masterRestClient, err := mesosLeader.getRestAPIClient(targetConf, masterConf)
		if err != nil { // can't login, skip this one - dcos
			glog.Errorf("Error creating rest api client : %s", err)
			continue
		}
		// API request to get the Master State to get the leader
		mesosState, err := masterRestClient.GetState()
		if err != nil { // can't get state, skip this one - apache
			glog.Errorf("Error getting state from master %s::%s : %s \n",
					masterConf.MasterIP, masterConf.MasterPort, err)
			continue
		}
		glog.V(3).Infof("Mesos api succeeded for: %++v\n", mesosState.LeaderInfo)
		if  mesosState.LeaderInfo.Hostname == "" {
			glog.V(3).Infof("Missing leader info in state response from master  %s::%s : %s \n",
					masterConf.MasterIP, masterConf.MasterPort, err)
			//continue;
			mesosLeader.leaderConf = masterConf
			mesosLeader.leaderRestClient = masterRestClient
			mesosLeader.MasterState = mesosState	// state retrieved
			glog.V(3).Infof("Using mesos leader: %s::%s\n",
				mesosLeader.leaderConf.MasterIP, mesosLeader.leaderConf.MasterPort)
			return nil
		}
		// Compare leader IP to current master
		if mesosState.LeaderInfo.Hostname == masterConf.MasterIP {
			// Leader is same as the current master
			mesosLeader.leaderConf = masterConf
			mesosLeader.leaderRestClient = masterRestClient
			glog.V(3).Infof("Leader is same as the current master %s::%s",
					mesosLeader.leaderConf.MasterIP, mesosLeader.leaderConf.MasterPort)
		} else {
			// Leader is different from the current master
			portStr := strconv.Itoa(mesosState.LeaderInfo.Port)
			mapkey := strings.Join([]string{mesosState.LeaderInfo.Hostname, portStr}, ":")
			leaderConf, exists := mesosLeader.masterConfMap[MASTER_IP_PORT(mapkey)]
			if !exists { // edge case that the target conf is missing the leader service ip:port
				leaderConf = &conf.MasterConf {
					MasterIP:mesosState.LeaderInfo.Hostname,
					MasterPort:portStr,
				}
				glog.V(2).Infof("Missing conf for the leader %s, created new leaderconf %++v", mapkey, leaderConf)
				mesosLeader.masterConfMap[MASTER_IP_PORT(mapkey)] = leaderConf
			}
			mesosLeader.leaderConf = leaderConf
			glog.V(3).Infof("Leader is differnt than the current master %s::%s",
				mesosLeader.leaderConf.MasterIP, mesosLeader.leaderConf.MasterPort)
			// get the api client and save it
			leaderRestClient, _ := mesosLeader.getRestAPIClient(targetConf, leaderConf)
			mesosLeader.leaderRestClient = leaderRestClient
		}

		mesosLeader.MasterState = mesosState	// state retrieved
		glog.Infof("Detected mesos leader: %s::%s\n",
				mesosLeader.leaderConf.MasterIP, mesosLeader.leaderConf.MasterPort)
		return nil
	}
	return fmt.Errorf("Cannot detect leader using %s", targetConf.MasterIPPort)
}


// Refresh the Mesos leader state
// Use existing RestAPI client to execute the request or update the leader and execute the request
func (mesosLeader *MesosLeader) RefreshMesosLeaderState() (error) {
	glog.V(3).Infof("RefreshMesosLeaderState %++v", mesosLeader.leaderConf)
	// API request to get the Master State from the current leader
	mesosState, err := mesosLeader.leaderRestClient.GetState()
	if err == nil { // no error, succeeded but check the leader
		glog.V(3).Infof("Mesos get state api succeeded with existing leader : %++v\n",
					mesosLeader.leaderConf)
		// Leader is same as the current leader, then update the Master State and return
		if mesosState.LeaderInfo.Hostname == mesosLeader.leaderConf.MasterIP {
			mesosLeader.MasterState = mesosState
			glog.V(3).Infof("No change in mesos leader : %++v\n", mesosLeader.leaderConf)
			return nil
		}
	}

	// Create the new leader by iterating over the list of master service configs
	glog.Errorf("Existing leader at %s is not reachable, detecting new leader using : %s",
				mesosLeader.leaderConf.MasterIP, mesosLeader.targetConf.MasterIPPort)

	// Create the Mesos leader by iterating over the list of IP:Port
	err = mesosLeader.updateMesosLeader()
	return err
}

// Refresh the Mesos leader login
// Use existing leader RestAPI client to execute the request, if not successful update the leader and login again
func (mesosLeader *MesosLeader) RefreshMesosLeaderLogin() (error) {
	glog.V(3).Infof("RefreshMesosLeaderLogin %++v", mesosLeader.leaderConf)
	// API request to Login to the current Mesos Master leader and save the login token for subsequent discovery requests
	token, err := mesosLeader.leaderRestClient.Login()
	if err == nil {
		glog.V(3).Infof("Mesos login api succeeded with existing leader : %++v\n", mesosLeader.leaderConf)
		mesosLeader.leaderConf.Token = token
		return nil
	}

	glog.Errorf("Error logging to Mesos Leader at %s, detecting new leader using : %s",
			mesosLeader.leaderConf.MasterIP, mesosLeader.targetConf.MasterIPPort)
	// Refresh the leader
	err = mesosLeader.updateMesosLeader()
	return err
}

// Get the RestAPI Client using the given master config.
// Return MasterRestClient if login is successful, nil if the login fails
func (mesosLeader *MesosLeader) getRestAPIClient(targetConf *conf.MesosTargetConf, masterConf *conf.MasterConf) (master.MasterRestClient,  error) {
	// Bad master config
	if masterConf.MasterIP == "" {
		return nil, fmt.Errorf("Null master ip : " + string(targetConf.Master))
	}
	var masterRestClient master.MasterRestClient

	// supply credentials from the target conf before creating the login request
	masterConf.MasterUsername = targetConf.MasterUsername
	masterConf.MasterPassword = targetConf.MasterPassword
	masterConf.Master = targetConf.Master
	glog.V(3).Infof("Creating new master rest api client %++v", masterConf)
	// Create a new rest api client using the given master conf
	masterRestClient = master.GetMasterRestClient(targetConf.Master, masterConf)

	if masterRestClient == nil {	//does not exist and cannot create new instance
		nerr := fmt.Errorf("Cannot find rest api client for master %s:%s::%s ", string(targetConf.Master))
		glog.Errorf("%s", nerr.Error())
		return nil, nerr
	}

	// Login to the Mesos Master and save the login token
	token, err := masterRestClient.Login()
	if err != nil {
		nerr := fmt.Errorf("Error logging to mesos master at %s::%s : %s ",
			masterConf.MasterIP, masterConf.MasterPort, err)
		glog.Errorf("%s", nerr.Error())
		return nil, nerr
	}
	// Save the token
	masterConf.Token = token
	return masterRestClient, nil
}

func parseMasterIPPorts(masterType conf.MesosMasterType, ipportlist string) []*conf.MasterConf {
	// Split on comma.
	ipports := strings.Split(ipportlist, ",")

	var masterConfList []*conf.MasterConf
	for i := range ipports {
		result := strings.Split(ipports[i], ":")
		var ipaddress, port string
		ipaddress = strings.TrimSpace(result[0])
		if ipaddress == "" {
			continue
		}

		if len(result) < 2 {
			if masterType== conf.Apache {
				port = conf.DEFAULT_APACHE_MESOS_MASTER_PORT
			} else if masterType== conf.DCOS {
				port = conf.DEFAULT_DCOS_MESOS_MASTER_PORT
			} else {
				port = ""
			}
		} else {
			port = strings.TrimSpace(result[1])
		}
		glog.Infof("[MesosLeader] parsed ipport %s::%s",ipaddress, port)
		masterConf := &conf.MasterConf {
			MasterIP:ipaddress,
			MasterPort:port,
		}
		masterConfList = append(masterConfList, masterConf)
	}
	return masterConfList
} //end

