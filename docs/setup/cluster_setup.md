---
title:  "Cluster Setup"
---
<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

This documentation is intended to provide instructions on how to run
Flink in a fully distributed fashion on a static (but possibly
heterogeneous) cluster.

This involves two steps. First, installing and configuring Flink and
second installing and configuring the [Hadoop Distributed
Filesystem](http://hadoop.apache.org/) (HDFS).

* This will be replaced by the TOC
{:toc}

## Preparing the Cluster

### Software Requirements

Flink runs on all *UNIX-like environments*, e.g. **Linux**, **Mac OS X**,
and **Cygwin** (for Windows) and expects the cluster to consist of **one master
node** and **one or more worker nodes**. Before you start to setup the system,
make sure you have the following software installed **on each node**:

- **Java 1.7.x** or higher,
- **ssh** (sshd must be running to use the Flink scripts that manage
  remote components)

If your cluster does not fulfill these software requirements you will need to
install/upgrade it.

For example, on Ubuntu Linux, type in the following commands to install Java and
ssh:

~~~bash
sudo apt-get install ssh
sudo apt-get install openjdk-7-jre
~~~

You can check the correct installation of Java by issuing the following command:

~~~bash
java -version
~~~

The command should output something comparable to the following on every node of
your cluster (depending on your Java version, there may be small differences):

~~~bash
java version "1.7.0_55"
Java(TM) SE Runtime Environment (build 1.7.0_55-b13)
Java HotSpot(TM) 64-Bit Server VM (build 24.55-b03, mixed mode)
~~~

To make sure the ssh daemon is running properly, you can use the command

~~~bash
ps aux | grep sshd
~~~

Something comparable to the following line should appear in the output
of the command on every host of your cluster:

~~~bash
root       894  0.0  0.0  49260   320 ?        Ss   Jan09   0:13 /usr/sbin/sshd
~~~

### Configuring Remote Access with ssh

In order to start/stop the remote processes, the master node requires access via
ssh to the worker nodes. It is most convenient to use ssh's public key
authentication for this. To setup public key authentication, log on to the
master as the user who will later execute all the Flink components. **The
same user (i.e. a user with the same user name) must also exist on all worker
nodes**. For the remainder of this instruction we will refer to this user as
*flink*. Using the super user *root* is highly discouraged for security
reasons.

Once you logged in to the master node as the desired user, you must generate a
new public/private key pair. The following command will create a new
public/private key pair into the *.ssh* directory inside the home directory of
the user *flink*. See the ssh-keygen man page for more details. Note that
the private key is not protected by a passphrase.

~~~bash
ssh-keygen -b 2048 -P '' -f ~/.ssh/id_rsa
~~~

Next, copy/append the content of the file *.ssh/id_rsa.pub* to your
authorized_keys file. The content of the authorized_keys file defines which
public keys are considered trustworthy during the public key authentication
process. On most systems the appropriate command is

~~~bash
cat .ssh/id_rsa.pub >> .ssh/authorized_keys
~~~

On some Linux systems, the authorized keys file may also be expected by the ssh
daemon under *.ssh/authorized_keys2*. In either case, you should make sure the
file only contains those public keys which you consider trustworthy for each
node of cluster.

Finally, the authorized keys file must be copied to every worker node of your
cluster. You can do this by repeatedly typing in

~~~bash
scp .ssh/authorized_keys <worker>:~/.ssh/
~~~

and replacing *\<worker\>* with the host name of the respective worker node.
After having finished the copy process, you should be able to log on to each
worker node from your master node via ssh without a password.

### Setting JAVA_HOME on each Node

Flink requires the `JAVA_HOME` environment variable to be set on the
master and all worker nodes and point to the directory of your Java
installation.

You can set this variable in `conf/flink-conf.yaml` via the
`env.java.home` key.

Alternatively, add the following line to your shell profile. If you use the
*bash* shell (probably the most common shell), the shell profile is located in
*\~/.bashrc*:

~~~bash
export JAVA_HOME=/path/to/java_home/
~~~

If your ssh daemon supports user environments, you can also add `JAVA_HOME` to
*.\~/.ssh/environment*. As super user *root* you can enable ssh user
environments with the following commands:

~~~bash
echo "PermitUserEnvironment yes" >> /etc/ssh/sshd_config
/etc/init.d/ssh restart

# on some system you might need to replace the above line with
/etc/init.d/sshd restart
~~~


## Flink Setup

Go to the [downloads page]({{site.baseurl}}/downloads.html) and get the ready to run
package. Make sure to pick the Flink package **matching your Hadoop
version**.

After downloading the latest release, copy the archive to your master node and
extract it:

~~~bash
tar xzf flink-*.tgz
cd flink-*
~~~

### Configuring the Cluster

After having extracted the system files, you need to configure Flink for
the cluster by editing *conf/flink-conf.yaml*.

Set the `jobmanager.rpc.address` key to point to your master node. Furthermode
define the maximum amount of main memory the JVM is allowed to allocate on each
node by setting the `jobmanager.heap.mb` and `taskmanager.heap.mb` keys.

The value is given in MB. If some worker nodes have more main memory which you
want to allocate to the Flink system you can overwrite the default value
by setting an environment variable `FLINK_TM_HEAP` on the respective
node.

Finally you must provide a list of all nodes in your cluster which shall be used
as worker nodes. Therefore, similar to the HDFS configuration, edit the file
*conf/slaves* and enter the IP/host name of each worker node. Each worker node
will later run a TaskManager.

Each entry must be separated by a new line, as in the following example:

~~~
192.168.0.100
192.168.0.101
.
.
.
192.168.0.150
~~~

The Flink directory must be available on every worker under the same
path. Similarly as for HDFS, you can use a shared NSF directory, or copy the
entire Flink directory to every worker node.

Please see the [configuration page](config.html) for details and additional
configuration options.

In particular,

 * the amount of available memory per TaskManager (`taskmanager.heap.mb`),
 * the number of available CPUs per machine (`taskmanager.numberOfTaskSlots`),
 * the total number of CPUs in the cluster (`parallelism.default`) and
 * the temporary directories (`taskmanager.tmp.dirs`)

are very important configuration values.


### Starting Flink

The following script starts a JobManager on the local node and connects via
SSH to all worker nodes listed in the *slaves* file to start the
TaskManager on each node. Now your Flink system is up and
running. The JobManager running on the local node will now accept jobs
at the configured RPC port.

Assuming that you are on the master node and inside the Flink directory:

~~~bash
bin/start-cluster.sh
~~~

To stop Flink, there is also a `stop-cluster.sh` script.

### Optional: Adding JobManager/TaskManager instances to a cluster

You can add both TaskManager or JobManager instances to your running cluster with the `bin/taskmanager.sh` and `bin/jobmanager.sh` scripts.

#### Adding a TaskManager
<pre>
bin/taskmanager.sh start|stop|stop-all
</pre>

#### Adding a JobManager
<pre>
bin/jobmanager.sh (start (local|cluster))|stop|stop-all
</pre>

Make sure to call these scripts on the hosts, on which you want to start/stop the respective instance.


## Optional: Hadoop Distributed Filesystem (HDFS) Setup

**NOTE** Flink does not require HDFS to run; HDFS is simply a typical choice of a distributed data
store to read data from (in parallel) and write results to.
If HDFS is already available on the cluster, or Flink is used purely with different storage
techniques (e.g., Apache Kafka, JDBC, Rabbit MQ, or other storage or message queues), this
setup step is not needed.


The following instructions are a general overview of usual required settings. Please consult one of the
many installation guides available online for more detailed instructions.

__Note that the following instructions are based on Hadoop 1.2 and might differ
for Hadoop 2.__

### Downloading, Installing, and Configuring HDFS

Similar to the Flink system HDFS runs in a distributed fashion. HDFS
consists of a **NameNode** which manages the distributed file system's meta
data. The actual data is stored by one or more **DataNodes**. For the remainder
of this instruction we assume the HDFS's NameNode component runs on the master
node while all the worker nodes run an HDFS DataNode.

To start, log on to your master node and download Hadoop (which includes  HDFS)
from the Apache [Hadoop Releases](http://hadoop.apache.org/releases.html) page.

Next, extract the Hadoop archive.

After having extracted the Hadoop archive, change into the Hadoop directory and
edit the Hadoop environment configuration file:

~~~bash
cd hadoop-*
vi conf/hadoop-env.sh
~~~

Uncomment and modify the following line in the file according to the path of
your Java installation.

~~~
export JAVA_HOME=/path/to/java_home/
~~~

Save the changes and open the HDFS configuration file *conf/hdfs-site.xml*. HDFS
offers multiple configuration parameters which affect the behavior of the
distributed file system in various ways. The following excerpt shows a minimal
configuration which is required to make HDFS work. More information on how to
configure HDFS can be found in the [HDFS User
Guide](http://hadoop.apache.org/docs/r1.2.1/hdfs_user_guide.html) guide.

~~~xml
<configuration>
  <property>
    <name>fs.default.name</name>
    <value>hdfs://MASTER:50040/</value>
  </property>
  <property>
    <name>dfs.data.dir</name>
    <value>DATAPATH</value>
  </property>
</configuration>
~~~

Replace *MASTER* with the IP/host name of your master node which runs the
*NameNode*. *DATAPATH* must be replaced with path to the directory in which the
actual HDFS data shall be stored on each worker node. Make sure that the
*flink* user has sufficient permissions to read and write in that
directory.

After having saved the HDFS configuration file, open the file *conf/slaves* and
enter the IP/host name of those worker nodes which shall act as *DataNode*s.
Each entry must be separated by a line break.

~~~
<worker 1>
<worker 2>
.
.
.
<worker n>
~~~

Initialize the HDFS by typing in the following command. Note that the
command will **delete all data** which has been previously stored in the
HDFS. However, since we have just installed a fresh HDFS, it should be
safe to answer the confirmation with *yes*.

~~~bash
bin/hadoop namenode -format
~~~

Finally, we need to make sure that the Hadoop directory is available to
all worker nodes which are intended to act as DataNodes and that all nodes
**find the directory under the same path**. We recommend to use a shared network
directory (e.g. an NFS share) for that. Alternatively, one can copy the
directory to all nodes (with the disadvantage that all configuration and
code updates need to be synced to all nodes).

### Starting HDFS

To start the HDFS log on to the master and type in the following
commands

~~~bash
cd hadoop-*
bin/start-dfs.sh
~~~

If your HDFS setup is correct, you should be able to open the HDFS
status website at *http://MASTER:50070*. In a matter of a seconds,
all DataNodes should appear as live nodes. For troubleshooting we would
like to point you to the [Hadoop Quick
Start](http://wiki.apache.org/hadoop/QuickStart)
guide.


