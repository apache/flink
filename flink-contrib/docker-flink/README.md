#Apache Flink cluster deployment on Docker using Docker-Compose

##Installation
###Install Docker

https://docs.docker.com/installation/

if you have issues with Docker-Compose versions incompatible with your version of Docker try

`curl -sSL https://get.docker.com/ubuntu/ | sudo sh`

###Install Docker-Compose

```
curl -L https://github.com/docker/compose/releases/download/1.1.0/docker-compose-`uname -s`-`uname -m` > /usr/local/bin/docker-compose

chmod +x /usr/local/bin/docker-compose
```

###Get the repo

###Build the images

Images are based on Ubuntu Trusty 14.04 and run Supervisord to stay alive when running containers.

The base image installs Oracle Java JDK 1.7 and SSH client & server. You can change the SSH password there or add your own key and adjust SSH config.

- Run `./build.sh`

###Deploy

- Deploy cluster and see config/setup log output (best run in a screen session)

`docker-compose up`

- Deploy as a daemon (and return)

`docker-compose up -d`

- Scale the cluster up or down to *N* TaskManagers

`docker-compose scale taskmanager=<N>`

- Access the JobManager node with SSH (exposed on Port 220)

`ssh root@localhost -p 220`

or on Mac OS X with boot2docker

`ssh root@$(boot2docker ip) -p 220`

The password is 'secret'

- Kill the cluster

`docker-compose kill`

- Upload a jar to the cluster

`scp -P 220 <your_jar> root@localhost:/<your_path>`

- Run a topology

`ssh -p 220 root@localhost /usr/local/flink/bin/flink run -c <your_class> <your_jar> <your_params>`

or

ssh to the job manager and run the topology from there.

###Ports

- The Web Dashboard is on port `48080`
- The Web Client is on port `48081`
- JobManager RPC port `6123` (default, not exposed to host)
- TaskManagers RPC port `6121` (default, not exposed to host)
- TaskManagers Data port `6122` (default, not exposed to host)
- JobManager SSH `220`
- TaskManagers SSH: randomly assigned port, check wih `docker ps`

Edit the `docker-compose.yml` file to edit port settings.
