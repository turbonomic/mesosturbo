# Deploying Mesosturbo
Once deployed, the Mesosturbo service enables you to give Turbonomic visibility into a Mesos cluster. This cluster can be located in either a private datacenter, or locally. Mesosturbo will be deployed as a container on Agent nodes.

### Prerequisites
* Turbonomic 5.9+
* Running Mesos Apache XXX or Mesosphere DCOS 1.8
> NOTE: to check the current status of your Mesos Master, run the following command in the console:
> ```console
>$ ?????

### Step One: Build Mesosturbo Binary from source code
##### Prerequisites 
* Set up your Go Dev environment. 

Download Mesosturbo 
* Check out mesosturbo project from https://github.com/turbonomic/mesosturbo

Build MesosTurbo binary with the following command:
> ```` console
>$ cd <root of the mesosturbo project>

To build binary that is runnable in linux environment, use this command with the OS specification.     
> ```` console
>$ GOOS=linux ARCH=amd go build -o ./_output/mesosturbo ./cmd/
    
### Step Two: Creating and push the Mesosturbo Docker Container Image

> NOTE: Ensure that you have the Mesosturbo Binary
##### Prerequisites
* Install Docker on your machine. You can download a stable version for Mac from [here](https://docs.docker.com/docker-for-mac/install/#download-docker-for-mac).
* If you want to push image to [DockerHub](https://docs.docker.com/docker-hub/), the default registry for docker images, you will need to register and obtain 
a username and pwd.  If you don't have a Docker ID, head over to 
[DockerHub HomePage](https://hub.docker.com) to create one.
* Need [Dockerfile](https://docs.docker.com/engine/reference/builder/) which is found at the root dir of the mesosturbo project. 
  * Change the name of the maintainer in the file 

Build and push Mesosturbo Container Image -
* Build a Docker Image. 
At the root dir of your mesosturbo project where the Dockerfile is located, run 
> ```` console
> $ docker build -t <yourname>/mesosturbo:<your-tag> .
> <youname> identifier for the user creating the image
> <your-tag> any name for the the image tag

> _**Note, make sure you have the tailing "DOT" in the previous line.**_
   
* Check that the image is created on your local machine’s local Docker image registry:
> ```` console 
> $ docker images

* Login with your docker repo using your Docker ID to push and pull images from Docker Hub  
> ```` console
> $ docker login
> Login with your Docker ID to push and pull images from Docker Hub. If you don't have a Docker ID, 
> head over to https://hub.docker.com to create one.

> ```` console
> $ docker push yourname/mesosturbo:your-tag

* Visit the [DockerHub HomePage](https://hub.docker.com) and check that the image has been uploaded.

### Step Three: Deploying the Mesosturbo Docker Container Image
> NOTE: Ensure that the Mesosturbo container image is pushed to docker hub or an image registry 
> that is accessible to the Marathon service in the Mesos Cluster.
##### Prerequisites 
* Know your Mesos Master IP and port
* Marathon service is available in the Mesos Cluster for deploying applications.
* Marathon service has internet access to the DockerHub or the registry where the container image resides.
* Install Operations Manager 5.9+ and know its IP
* Know the username and password for the Rest API user for Operations Manager.

Containers are deployed by Marathon Service running in Mesos. 

#### Mesosturbo Container Definition

```yaml
{
  "id": "mesosturbo",
  "container": {
    "docker": {
      "image": "<yourname>/mesosturbo:<your-tag>"
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
> * \<yourname\>/mesosturbo:\<your-tag\> with Name of the docker image tag
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

### Step Four: Verify the Mesosturbo is running

* Visit the Marathon Service webpage and locate the mesosturbo application.
* Login to the Turbo Operations Manager UI and check in the Inventory Tab for the discovered Mesos Cluster Topology.

