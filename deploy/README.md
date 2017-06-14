# Deploying Mesosturbo
Once deployed, the Mesosturbo service enables you to give Turbonomic visibility into a Mesos cluster. This cluster can be located in either a public or private datacenter. Mesosturbo will be deployed as a container on Agent nodes.

### Prerequisites
* Turbonomic 5.9+
* Running either Mesos Apache 0.28 or later or running Mesosphere DCOS 1.8

### Step One: Deploying the Mesosturbo Docker Container Image
> NOTE: Ensure that the Turbonomic Mesosturbo container image on DockerHub is accessible to the Marathon service in the Mesos Cluster.
##### Prerequisites 
* Know your Mesos Master IP and port
* Marathon service is available in the Mesos Cluster for deploying applications.
* Marathon service has internet access to the DockerHub registry where the Turbonomic Mesosturbo container image resides.
* Install Operations Manager 5.9+ and know its IP
* Know the username and password for the Rest API user for Operations Manager.

Containers are deployed by Marathon Service running in Mesos. 

##### Mesosturbo Container Definition for deploying in Mesosphere DC/OS

A copy of the deploy config can be downloaded from [here](deploy_dcos_mesosturbo_5.9_0.json)

```yaml
{
  "id": "mesosturbo",
  "container": {
    "docker": {
      "image": "vmturbo/mesosturbo:5.9.0"
    },
    "type": "DOCKER",
    "volumes": []
  },
  "args": [
    "--mesostype", "Mesosphere DCOS",
    "--masteripport", "<MESOS-MASTER-IPPORT>",
    "--masteruser", "<MESOS-MASTER-USER>",
    "--masterpwd", "<MESOS-MASTER-PASSWORD>",
    "--turboserverurl", "http://<TURBO-OPERATIONS-MANAGER-IP>:80",
    "--opsmanagerusername", "<TURBO-OPERATIONS-MANAGER-ADMIN-USERNAME>",
    "--opsmanagerpassword", "<TURBO-OPERATIONS-MANAGER-ADMIN-PASSWORD>" 
  ],
  "cpus": 0.5,
  "mem": 128.0,
  "instances": 1
}
```
> Replace 
> * \<MESOS-MASTER-IPPORT> with the Comma separated list of host and port of each Mesos Master in the cluster, 
 e.g. 1.1.1.100,1.1.1.101,1.1.1.102
> * \<MESOS-MASTER-USER> with the Username for the Mesos Master
> * \<MESOS-MASTER-PASSWORD> with the Password for the Mesos Master
> * \<TURBO-OPERATIONS-MANAGER-IP> with the IP address for the Turbo Operations Manager
> * \<TURBO-OPERATIONS-MANAGER-ADMIN-USERNAME> with the username for the administrator user in the Turbo Operations Manager
> * \<TURBO-OPERATIONS-MANAGER-ADMIN-PASSWORD> with the password for the administrator user in the Turbo Operations Manager

##### Mesosturbo Container Definition for deploying in Apache Mesos

A copy of the deploy config can be downloaded from [here](deploy_apache_mesosturbo_5.9_0.json)

```yaml
{
  "id": "mesosturbo",
  "container": {
    "docker": {
      "image": "vmturbo/mesosturbo:5.9.0"
    },
    "type": "DOCKER",
    "volumes": []
  },
  "args": [
    "--mesostype", "Apache Mesos",
    "--masteripport", "<MESOS-MASTER-IPPORT>",
    "--turboserverurl", "http://<TURBO-OPERATIONS-MANAGER-IP>:80",
    "--opsmanagerusername", "<TURBO-OPERATIONS-MANAGER-ADMIN-USERNAME>",
    "--opsmanagerpassword", "<TURBO-OPERATIONS-MANAGER-ADMIN-PASSWORD>" 
  ],
  "cpus": 0.5,
  "mem": 128.0,
  "instances": 1
}
```
> Replace 
> * \<MESOS-MASTER-IPPORT> with the Comma separated list of host and port of each Mesos Master in the cluster,
 e.g. 1.1.1.100:5050,1.1.1.101:5050,1.1.1.102:5050. *Omit port if using default port 5050*.
> * \<TURBO-OPERATIONS-MANAGER-IP> with the IP address for the Turbo Operations Manager
> * \<TURBO-OPERATIONS-MANAGER-ADMIN-USERNAME> with the username for the administrator user in the Turbo Operations Manager
> * \<TURBO-OPERATIONS-MANAGER-ADMIN-PASSWORD> with the password for the administrator user in the Turbo Operations Manager


The Mesosturbo container will be visible after several seconds. 

### Step Two: Verify that the Mesosturbo is running

* Visit the Marathon Service webpage and locate the mesosturbo application.
* Login to the Turbo Operations Manager UI and check in the Inventory Tab for the discovered Mesos Cluster Topology.

