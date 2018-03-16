---
title: "SSL Setup"
nav-parent_id: ops
nav-pos: 10
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

This page provides instructions on how to enable SSL for the network communication between different flink components.

## SSL Configuration

SSL can be enabled for all network communication between flink components. SSL keystores and truststore has to be deployed on each flink node and configured (conf/flink-conf.yaml) using keys in the security.ssl.* namespace (Please see the [configuration page](config.html) for details). SSL can be selectively enabled/disabled for different transports using the following flags. These flags are only applicable when security.ssl.enabled is set to true.

* **taskmanager.data.ssl.enabled**: SSL flag for data communication between task managers
* **blob.service.ssl.enabled**: SSL flag for blob service client/server communication
* **akka.ssl.enabled**: SSL flag for the akka based control connection between the flink client, jobmanager and taskmanager 
* **jobmanager.web.ssl.enabled**: Flag to enable https access to the jobmanager's web frontend

## Deploying Keystores and Truststores

You need to have a Java Keystore generated and copied to each node in the flink cluster. The common name or subject alternative names in the certificate should match the node's hostname and IP address. Keystores and truststores can be generated using the keytool utility (https://docs.oracle.com/javase/8/docs/technotes/tools/unix/keytool.html). All flink components should have read access to the keystore and truststore files.

### Example: Creating self signed CA and keystores for a 2 node cluster

Execute the following keytool commands to create a truststore with a self signed CA

~~~
keytool -genkeypair -alias ca -keystore ca.keystore -dname "CN=Sample CA" -storepass password -keypass password -keyalg RSA -ext bc=ca:true
keytool -keystore ca.keystore -storepass password -alias ca -exportcert > ca.cer
keytool -importcert -keystore ca.truststore -alias ca -storepass password -noprompt -file ca.cer
~~~

Now create keystores for each node with certificates signed by the above CA. Let node1.company.org and node2.company.org be the hostnames with IPs 192.168.1.1 and 192.168.1.2 respectively

#### Node 1
~~~
keytool -genkeypair -alias node1 -keystore node1.keystore -dname "CN=node1.company.org" -ext SAN=dns:node1.company.org,ip:192.168.1.1 -storepass password -keypass password -keyalg RSA
keytool -certreq -keystore node1.keystore -storepass password -alias node1 -file node1.csr
keytool -gencert -keystore ca.keystore -storepass password -alias ca -ext SAN=dns:node1.company.org,ip:192.168.1.1 -infile node1.csr -outfile node1.cer
keytool -importcert -keystore node1.keystore -storepass password -file ca.cer -alias ca -noprompt
keytool -importcert -keystore node1.keystore -storepass password -file node1.cer -alias node1 -noprompt
~~~

#### Node 2
~~~
keytool -genkeypair -alias node2 -keystore node2.keystore -dname "CN=node2.company.org" -ext SAN=dns:node2.company.org,ip:192.168.1.2 -storepass password -keypass password -keyalg RSA
keytool -certreq -keystore node2.keystore -storepass password -alias node2 -file node2.csr
keytool -gencert -keystore ca.keystore -storepass password -alias ca -ext SAN=dns:node2.company.org,ip:192.168.1.2 -infile node2.csr -outfile node2.cer
keytool -importcert -keystore node2.keystore -storepass password -file ca.cer -alias ca -noprompt
keytool -importcert -keystore node2.keystore -storepass password -file node2.cer -alias node2 -noprompt
~~~

## Standalone Deployment
Configure each node in the standalone cluster to pick up the keystore and truststore files present in the local file system.

### Example: 2 node cluster

* Generate 2 keystores, one for each node, and copy them to the filesystem on the respective node. Also copy the pulic key of the CA (which was used to sign the certificates in the keystore) as a Java truststore on both the nodes
* Configure conf/flink-conf.yaml to pick up these files

#### Node 1
~~~
security.ssl.enabled: true
security.ssl.keystore: /usr/local/node1.keystore
security.ssl.keystore-password: abc123
security.ssl.key-password: abc123
security.ssl.truststore: /usr/local/ca.truststore
security.ssl.truststore-password: abc123
~~~

#### Node 2
~~~
security.ssl.enabled: true
security.ssl.keystore: /usr/local/node2.keystore
security.ssl.keystore-password: abc123
security.ssl.key-password: abc123
security.ssl.truststore: /usr/local/ca.truststore
security.ssl.truststore-password: abc123
~~~

* Restart the flink components to enable SSL for all of flink's internal communication
* Verify by accessing the jobmanager's UI using https url. The task manager's path in the UI should show akka.ssl.tcp:// as the protocol
* The blob server and task manager's data communication can be verified from the log files

## YARN Deployment
The keystores and truststore can be deployed in a YARN setup in multiple ways depending on the cluster setup. Following are 2 ways to achieve this

### 1. Deploy keystores before starting the YARN session
The keystores and truststore should be generated and deployed on all nodes in the YARN setup where flink components can potentially be executed. The same flink config file from the flink YARN client is used for all the flink components running in the YARN cluster. Therefore we need to ensure the keystore is deployed and accessible using the same filepath in all the YARN nodes.

#### Example config
~~~
security.ssl.enabled: true
security.ssl.keystore: /usr/local/node.keystore
security.ssl.keystore-password: abc123
security.ssl.key-password: abc123
security.ssl.truststore: /usr/local/ca.truststore
security.ssl.truststore-password: abc123
~~~

Now you can start the YARN session from the CLI like you would normally do.

### 2. Use YARN cli to deploy the keystores and truststore
We can use the YARN client's ship files option (-yt) to distribute the keystores and truststore. Since the same keystore will be deployed at all nodes, we need to ensure a single certificate in the keystore can be served for all nodes. This can be done by either using the Subject Alternative Name(SAN) extension in the certificate and setting it to cover all nodes (hostname and ip addresses) in the cluster or by using wildcard subdomain names (if the cluster is setup accordingly). 

#### Example
* Supply the following parameters to the keytool command when generating the keystore: -ext SAN=dns:node1.company.org,ip:192.168.1.1,dns:node2.company.org,ip:192.168.1.2
* Copy the keystore and the CA's truststore into a local directory (at the cli's working directory), say deploy-keys/
* Update the configuration to pick up the files from a relative path

~~~
security.ssl.enabled: true
security.ssl.keystore: deploy-keys/node.keystore
security.ssl.keystore-password: password
security.ssl.key-password: password
security.ssl.truststore: deploy-keys/ca.truststore
security.ssl.truststore-password: password
~~~

* Start the YARN session using the -yt parameter

~~~
flink run -m yarn-cluster -yt deploy-keys/ TestJob.jar
~~~

When deployed using YARN, flink's web dashboard is accessible through YARN proxy's Tracking URL. To ensure that the YARN proxy is able to access flink's https url you need to configure YARN proxy to accept flink's SSL certificates. Add the custom CA certificate into Java's default truststore on the YARN Proxy node.

{% top %}
