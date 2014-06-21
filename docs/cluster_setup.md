---
title:  "Cluster Setup"
---

This documentation is intended to provide instructions on how to run
Stratosphere in a fully distributed fashion on a static (but possibly
heterogeneous) cluster.

This involves two steps. First, installing and configuring Stratosphere and
second installing and configuring the [Hadoop Distributed
Filesystem](http://hadoop.apache.org/) (HDFS).

# Preparing the Cluster

## Software Requirements

Stratosphere runs on all *UNIX-like environments*, e.g. **Linux**, **Mac OS X**,
and **Cygwin** (for Windows) and expects the cluster to consist of **one master
node** and **one or more worker nodes**. Before you start to setup the system,
make sure you have the following software installed **on each node**:

- **Java 1.6.x** or higher,
- **ssh** (sshd must be running to use the Stratosphere scripts that manage
  remote components)

If your cluster does not fulfill these software requirements you will need to
install/upgrade it.

For example, on Ubuntu Linux, type in the following commands to install Java and
ssh:

```
sudo apt-get install ssh 
sudo apt-get install openjdk-7-jre
```

You can check the correct installation of Java by issuing the following command:

```
java -version
```

The command should output something comparable to the following on every node of
your cluster (depending on your Java version, there may be small differences):

```
java version "1.6.0_22"
Java(TM) SE Runtime Environment (build 1.6.0_22-b04)
Java HotSpot(TM) 64-Bit Server VM (build 17.1-b03, mixed mode)
```

To make sure the ssh daemon is running properly, you can use the command

```
ps aux | grep sshd
```

Something comparable to the following line should appear in the output
of the command on every host of your cluster:

```
root       894  0.0  0.0  49260   320 ?        Ss   Jan09   0:13 /usr/sbin/sshd
```

## Configuring Remote Access with ssh

In order to start/stop the remote processes, the master node requires access via
ssh to the worker nodes. It is most convenient to use ssh's public key
authentication for this. To setup public key authentication, log on to the
master as the user who will later execute all the Stratosphere components. **The
same user (i.e. a user with the same user name) must also exist on all worker
nodes**. For the remainder of this instruction we will refer to this user as
*stratosphere*. Using the super user *root* is highly discouraged for security
reasons.

Once you logged in to the master node as the desired user, you must generate a
new public/private key pair. The following command will create a new
public/private key pair into the *.ssh* directory inside the home directory of
the user *stratosphere*. See the ssh-keygen man page for more details. Note that
the private key is not protected by a passphrase.

```
ssh-keygen -b 2048 -P '' -f ~/.ssh/id_rsa
```

Next, copy/append the content of the file *.ssh/id_rsa.pub* to your
authorized_keys file. The content of the authorized_keys file defines which
public keys are considered trustworthy during the public key authentication
process. On most systems the appropriate command is

```
cat .ssh/id_rsa.pub >> .ssh/authorized_keys
```

On some Linux systems, the authorized keys file may also be expected by the ssh
daemon under *.ssh/authorized_keys2*. In either case, you should make sure the
file only contains those public keys which you consider trustworthy for each
node of cluster.

Finally, the authorized keys file must be copied to every worker node of your
cluster. You can do this by repeatedly typing in

```
scp .ssh/authorized_keys <worker>:~/.ssh/
```

and replacing *\<worker\>* with the host name of the respective worker node.
After having finished the copy process, you should be able to log on to each
worker node from your master node via ssh without a password.

## Setting JAVA_HOME on each Node

Stratosphere requires the `JAVA_HOME` environment variable to be set on the
master and all worker nodes and point to the directory of your Java
installation.

You can set this variable in `conf/stratosphere-conf.yaml` via the
`env.java.home` key.

Alternatively, add the following line to your shell profile. If you use the
*bash* shell (probably the most common shell), the shell profile is located in
*\~/.bashrc*:

```
export JAVA_HOME=/path/to/java_home/
```

If your ssh daemon supports user environments, you can also add `JAVA_HOME` to
*.\~/.ssh/environment*. As super user *root* you can enable ssh user
environments with the following commands:

```
echo "PermitUserEnvironment yes" >> /etc/ssh/sshd_config
/etc/init.d/ssh restart
```

# Hadoop Distributed Filesystem (HDFS) Setup

The Stratosphere system currently uses the Hadoop Distributed Filesystem (HDFS)
to read and write data in a distributed fashion.

Make sure to have a running HDFS installation. The following instructions are
just a general overview of some required settings. Please consult one of the
many installation guides available online for more detailed instructions.

**Note that the following instructions are based on Hadoop 1.2 and might differ
**for Hadoop 2.

## Downloading, Installing, and Configuring HDFS

Similar to the Stratosphere system HDFS runs in a distributed fashion. HDFS
consists of a **NameNode** which manages the distributed file system's meta
data. The actual data is stored by one or more **DataNodes**. For the remainder
of this instruction we assume the HDFS's NameNode component runs on the master
node while all the worker nodes run an HDFS DataNode.

To start, log on to your master node and download Hadoop (which includes  HDFS)
from the Apache [Hadoop Releases](http://hadoop.apache.org/releases.html) page.

Next, extract the Hadoop archive.

After having extracted the Hadoop archive, change into the Hadoop directory and
edit the Hadoop environment configuration file:

```
cd hadoop-*
vi conf/hadoop-env.sh
```

Uncomment and modify the following line in the file according to the path of
your Java installation.

``` export JAVA_HOME=/path/to/java_home/ ```

Save the changes and open the HDFS configuration file *conf/hdfs-site.xml*. HDFS
offers multiple configuration parameters which affect the behavior of the
distributed file system in various ways. The following excerpt shows a minimal
configuration which is required to make HDFS work. More information on how to
configure HDFS can be found in the [HDFS User
Guide](http://hadoop.apache.org/docs/r1.2.1/hdfs_user_guide.html) guide.

```xml
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
```

Replace *MASTER* with the IP/host name of your master node which runs the
*NameNode*. *DATAPATH* must be replaced with path to the directory in which the
actual HDFS data shall be stored on each worker node. Make sure that the
*stratosphere* user has sufficient permissions to read and write in that
directory.

After having saved the HDFS configuration file, open the file *conf/slaves* and
enter the IP/host name of those worker nodes which shall act as *DataNode*s.
Each entry must be separated by a line break.

```
<worker 1>
<worker 2>
.
.
.
<worker n>
```

Initialize the HDFS by typing in the following command. Note that the
command will **delete all data** which has been previously stored in the
HDFS. However, since we have just installed a fresh HDFS, it should be
safe to answer the confirmation with *yes*.

```
bin/hadoop namenode -format
```

Finally, we need to make sure that the Hadoop directory is available to
all worker nodes which are intended to act as DataNodes and that all nodes
**find the directory under the same path**. We recommend to use a shared network
directory (e.g. an NFS share) for that. Alternatively, one can copy the
directory to all nodes (with the disadvantage that all configuration and
code updates need to be synced to all nodes).

## Starting HDFS

To start the HDFS log on to the master and type in the following
commands

```
cd hadoop-*
binn/start-dfs.sh
```

If your HDFS setup is correct, you should be able to open the HDFS
status website at *http://MASTER:50070*. In a matter of a seconds,
all DataNodes should appear as live nodes. For troubleshooting we would
like to point you to the [Hadoop Quick
Start](http://wiki.apache.org/hadoop/QuickStart)
guide.

# Stratosphere Setup

Go to the [downloads page]({{site.baseurl}}/downloads/) and get the ready to run
package. Make sure to pick the Stratosphere package **matching your Hadoop
version**.

After downloading the latest release, copy the archive to your master node and
extract it:

```
tar xzf stratosphere-*.tgz
cd stratosphere-*
```

## Configuring the Cluster

After having extracted the system files, you need to configure Stratosphere for
the cluster by editing *conf/stratosphere-conf.yaml*.

Set the `jobmanager.rpc.address` key to point to your master node. Furthermode
define the maximum amount of main memory the JVM is allowed to allocate on each
node by setting the `jobmanager.heap.mb` and `taskmanager.heap.mb` keys.

The value is given in MB. If some worker nodes have more main memory which you
want to allocate to the Stratosphere system you can overwrite the default value
by setting an environment variable `STRATOSPHERE_TM_HEAP` on the respective
node.

Finally you must provide a list of all nodes in your cluster which shall be used
as worker nodes. Therefore, similar to the HDFS configuration, edit the file
*conf/slaves* and enter the IP/host name of each worker node. Each worker node
will later run a TaskManager.

Each entry must be separated by a new line, as in the following example:

```
192.168.0.100
192.168.0.101
.
.
.
192.168.0.150
```

The Stratosphere directory must be available on every worker under the same
path. Similarly as for HDFS, you can use a shared NSF directory, or copy the
entire Stratosphere directory to every worker node.

## Configuring the Network Buffers

Network buffers are a critical resource for the communication layers. They are
used to buffer records before transmission over a network, and to buffer
incoming data before dissecting it into records and handing them to the
application. A sufficient number of network buffers are critical to achieve a
good throughput.

In general, configure the task manager to have so many buffers that each logical
network connection on you expect to be open at the same time has a dedicated
buffer. A logical network connection exists for each point-to-point exchange of
data over the network, which typically happens at repartitioning- or
broadcasting steps. In those, each parallel task inside the TaskManager has to
be able to talk to all other parallel tasks. Hence, the required number of
buffers on a task manager is *total-degree-of-parallelism* (number of targets)
\* *intra-node-parallelism* (number of sources in one task manager) \* *n*.
Here, *n* is a constant that defines how many repartitioning-/broadcasting steps
you expect to be active at the same time.

Since the *intra-node-parallelism* is typically the number of cores, and more
than 4 repartitioning or broadcasting channels are rarely active in parallel, it
frequently boils down to *\#cores\^2\^* \* *\#machines* \* 4. To support for
example a cluster of 20 8-core machines, you should use roughly 5000 network
buffers for optimal throughput.

Each network buffer is by default 64 KiBytes large. In the above example, the
system would allocate roughly 300 MiBytes for network buffers.

The number and size of network buffers can be configured with the following
parameters:

- `taskmanager.network.numberOfBuffers`, and
- `taskmanager.network.bufferSizeInBytes`.

## Configuring Temporary I/O Directories

Although Stratosphere aims to process as much data in main memory as possible,
it is not uncommon that  more data needs to be processed than memory is
available. Stratosphere's runtime is designed to  write temporary data to disk
to handle these situations.

The `taskmanager.tmp.dirs` parameter specifies a list of directories into which
Stratosphere writes temporary files. The paths of the directories need to be
separated by ':' (colon character).  Stratosphere will concurrently write (or
read) one temporary file to (from) each configured directory.  This way,
temporary I/O can be evenly distributed over multiple independent I/O devices
such as hard disks to improve performance.  To leverage fast I/O devices (e.g.,
SSD, RAID, NAS), it is possible to specify a directory multiple times.

If the `taskmanager.tmp.dirs` parameter is not explicitly specified,
Stratosphere writes temporary data to the temporary  directory of the operating
system, such as */tmp* in Linux systems.

Please see the [configuration page](config.html) for details and additional
configuration options.

## Starting Stratosphere

The following script starts a JobManager on the local node and connects via
SSH to all worker nodes listed in the *slaves* file to start the
TaskManager on each node. Now your Stratosphere system is up and
running. The JobManager running on the local node will now accept jobs
at the configured RPC port.

Assuming that you are on the master node and inside the Stratosphere directory:

```
bin/start-cluster.sh
```
