# Deploying Mesosturbo
Once deployed, the Mesosturbo service enables you to give Turbonomic visibility into a Mesos cluster. This cluster can be located in either a public or private datacenter. Mesosturbo will be deployed as a container on Agent nodes.

### Prerequisites
* Turbonomic 5.9+
* Running Mesos Apache XXX or Mesosphere DCOS 1.8

### Step One: Deploying the Mesosturbo Docker Container Image
> NOTE: Ensure that the Turbonomic Mesosturbo container image on DockerHub is accessible to the Marathon service in the Mesos Cluster.
##### Prerequisites 
* Know your Mesos Master IP and port
* Marathon service is available in the Mesos Cluster for deploying applications.
* Marathon service has internet access to the DockerHub registry where the container image resides.
* Install Operations Manager 5.9+ and know its IP
* Know the username and password for the Rest API user for Operations Manager.

Containers are deployed by Marathon Service running in Mesos. 

#### Mesosturbo Container Definition

```yaml
{
  "id": "mesosturbo",
  "container": {
    "docker": {
      "image": "vmturbo/mesosturbo:5.9-latest"
    },
    "type": "DOCKER",
    "volumes": []
  },
  "args": [
    "--mesostype", "<MESOS MASTER TYPE>",
    "--masterip", "<MESOS-MASTER-IP>"
    "--masterport", "<MESOS-MASTER-PORT>",
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
> * \<MESOS MASTER TYPE> with 
>  * "Mesosphere DCOS" for Mesosphere DC/OS 
>  * "Apache Mesos" for Apache Mesos
> * \<MESOS-MASTER-IP> with IP address for the Mesos Master
> * \<MESOS-MASTER-PORT> with the port for the Mesos Master
> * \<MESOS-MASTER-USER> with the Username for the Mesos Master
> * \<MESOS-MASTER-PASSWORD> with the Password for the Mesos Master
> * \<TURBO-OPERATIONS-MANAGER-IP> with the IP address for the Turbo Operations Manager
> * \<TURBO-OPERATIONS-MANAGER-ADMIN-USERNAME> with the username for the administrator user in the Turbo Operations Manager
> * \<TURBO-OPERATIONS-MANAGER-ADMIN-PASSWORD> with the password for the administrator user in the Turbo Operations Manager

The Mesosturbo container will be visible after several seconds. 

### Step Two: Verify that the Mesosturbo is running

* Visit the Marathon Service webpage and locate the mesosturbo application.
* Login to the Turbo Operations Manager UI and check in the Inventory Tab for the discovered Mesos Cluster Topology.

