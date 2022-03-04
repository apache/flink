---
title: Incorporating Security Features in a Running Cluster
weight: 4
type: docs
aliases:
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

# Incorporating Security Features in a Running Cluster

This guide describes how Flink security works in the context of various [deployment modes]({{< ref "docs/deployment/overview" >}}), 
[file systems]({{< ref "docs/deployment/filesystems/overview" >}}), [connectors]({{< ref "docs/connectors" >}}), 
and [state backends]({{< ref "docs/ops/state/state_backends" >}}).

## Running a Secure Flink Cluster in Standalone Mode

Standalone mode is the most barebone way of deploying a Flink cluster where Flink processes are launched 
as processes on the operating system. You can learn more [here]({{< ref "docs/deployment/resource-providers/standalone/overview" >}}).

Incorporating security in this mode involves the following steps:
1. Add [security-related configuration options]({{< ref "docs/deployment/config" >}}#auth-with-external-systems)) 
   to the Flink configuration file (`flink-conf.yaml`) on all cluster nodes.
2. Ensure that the Kerberos keytab file exists at the path indicated by the `security.kerberos.login.keytab`
   setting on all cluster nodes.
3. Deploy the Flink cluster as normal.

{{< hint info >}}
`HADOOP_TOKEN_FILE_LOCATION` is an important environment variable that specifies the location of your 
credential files/tokens. The service instances uses this location to find the file to load the credentials 
and run the tasks. Launched containers must load the delegation tokens from this location, and use them 
(including renewals) until they can no longer be renewed.
{{< /hint >}}

You need to have an appropriate Java keystore and truststore accessible from each node in the Flink 
cluster. For standalone setups, this means copying the files to each node, or adding them to a shared 
mounted directory. For externally facing REST endpoints, the common name or subject alternative names 
in the certificate should match the node's hostname and IP address.

Keystores and truststores can be generated using the [keytool utility](https://docs.oracle.com/javase/8/docs/technotes/tools/unix/keytool.html).

## Running a Secure Flink Cluster in Kubernetes

Flink’s native Kubernetes integration allows you to directly deploy Flink on a running Kubernetes cluster. 
You can learn more [here]({{< ref "docs/deployment/resource-providers/native_kubernetes" >}}).

Incorporating security in this mode involves the following steps:
1. Add [security-related configuration options]({{< ref "docs/deployment/config" >}}#auth-with-external-systems))  
   to the Flink configuration file (`flink-conf.yaml`) on the client node.
2. Ensure that the Kerberos keytab file exists at the path as indicated by the `security.kerberos.login.keytab` 
   on the client node.
3. Deploy the Flink cluster as normal.

### Setting up Kerberos

When deploying Flink on Kubernetes, the Kerberos keytab file is automatically copied from the client 
node to the Flink containers.

To enable Kerberos authentication, the Kerberos configuration file is also required. This file should 
be present in the cluster nodes / classpath. If you want Flink to upload a new file, you need to configure 
the `security.kerberos.krb5-conf.path` setting to indicate the path of the Kerberos configuration file
and Flink will copy this file to its containers/pods.

{{< hint info >}}
`HADOOP_TOKEN_FILE_LOCATION` is an important environment variable that specifies the location of your
credential files/tokens. The service instances uses this location to find the file to load the credentials
and run the tasks. Launched containers must load the delegation tokens from this location, and use them
(including renewals) until they can no longer be renewed.
{{< /hint >}}

### Setting up keystores and truststores

You need to have an appropriate Java keystore and truststore accessible from each node in the Flink
cluster. For container-based setups, this means adding the keystore and truststore files to the container 
images. For externally facing REST endpoints, the common name or subject alternative names in the 
certificate should match the node's hostname and IP address.

## Running a Secure Flink Cluster in YARN

Flink services are submitted to YARN’s ResourceManager, which spawns containers on machines managed 
by YARN NodeManagers. Flink deploys its JobManager and TaskManager instances into such containers.
You can learn more [here]({{< ref "docs/deployment/resource-providers/yarn" >}}).

Incorporating security in this mode involves the following steps:
1. Add [security-related configuration options]({{< ref "docs/deployment/config" >}}#auth-with-external-systems))  
   to the Flink configuration file (`flink-conf.yaml`) on the client node.
2. Ensure that the Kerberos keytab file exists at the path as indicated by the `security.kerberos.login.keytab`
   on the client node.
3. Deploy the Flink cluster as normal.

### Setting up Kerberos

When deploying Flink on YARN, the Kerberos keytab is automatically copied from the client node to the 
Flink containers.

To enable Kerberos authentication, the Kerberos configuration file is also required. This file can be
either fetched from the cluster environment or uploaded by Flink. In the latter case, you need to
configure the `security.kerberos.krb5-conf.path` setting to indicate the path of the Kerberos configuration
file and Flink will copy this file to its containers/pods.

{{< hint info >}}
`HADOOP_TOKEN_FILE_LOCATION` is an important environment variable that specifies the location of your
credential files/tokens. The service instances uses this location to find the file to load the credentials
and run the tasks. Launched containers must load the delegation tokens from this location, and use them
(including renewals) until they can no longer be renewed.
{{< /hint >}}

#### Using `kinit` and without keytabs

[`kinit`](https://web.mit.edu/kerberos/krb5-1.12/doc/user/user_commands/kinit.html) is used to obtain 
and cache Kerberos ticket-granting tickets.

With YARN, it is possible to deploy a secure Flink cluster without a Kerberos keytab, which avoids the 
complexity of generating a keytab and entrusting the cluster manager with it. In this scenario, the 
ticket-granting-ticket cache, which is managed by `kinit`, is used and the Flink CLI acquires Hadoop 
delegation tokens (for HDFS and for HBase) instead. The main disadvantage is that the cluster is necessarily 
short-lived since the generated delegation tokens will expire (typically within a week).

These are the steps to run a secure Flink cluster using `kinit`:
1. Add [security-related configuration options]({{< ref "docs/deployment/config" >}}#auth-with-external-systems))  
   to the Flink configuration file (`flink-conf.yaml`) on the client node.
2. Log in using the `kinit` command.
3. Deploy the Flink cluster as normal.

### Setting up keystores and truststores

You need to have an appropriate Java keystore and truststore accessible from each node in the Flink
cluster. For YARN setups, the cluster deployment phase can automatically distribute the keystore and 
truststore files. For externally facing REST endpoints, the common name or subject alternative names in the
certificate should match the node's hostname and IP address.

For the externally facing REST endpoint, the common name or subject alternative names in the certificate
should match the node's hostname and IP address.

For more information, see the [documentation on YARN security](https://github.com/apache/hadoop/blob/trunk/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-site/src/site/markdown/YarnApplicationSecurity.md).

## Setting up SSL/TLS 

The easiest way to set up SSL/TLS is to generate a dedicated public/private key pair and self-signed 
certificate for the Flink deployment. The keystore and truststore are identical and contains
only that key pair and certificate. 

In an environment where operators are constrained to use firm-wide Internal Certificate Authorities 
(cannot generate self-signed certificates), we recommend to still have a dedicated key pair and certificate
for the Flink deployment, signed by that CA. However, the truststore must then also contain the
CA's public certificate to accept the deployment's certificate during the SSL/TLS handshake (a requirement
in the JDK truststore implementation).

{{< hint danger >}}
It is critical that you specify the fingerprint of the deployment certificate (via the `security.ssl.internal.cert.fingerprint` 
setting), when it is not self-signed. This is to pin that certificate as the only trusted certificate 
and prevent the truststore from trusting all certificates signed by that CA.
{{< /hint >}}

{{< hint info >}}
Since internal connections are mutually authenticated with shared certificates, Flink can skip 
hostname verification. This makes container-based setups easier.
{{< /hint >}}

### Tips for a YARN Deployment

To secure the external REST endpoint, you need to issue the REST endpoint's certificate such that it 
is valid for all hosts that the JobManager may get deployed to. This can be done with a wildcard DNS 
name or by adding multiple DNS names.

The easiest way to deploy keystores and truststore is via the YARN client's *ship files* option (`-yt`).
Copy the keystore and truststore files into a local directory (i.e. `deploy-keys/`) and start the YARN 
session like this: 

```bash
flink run -m yarn-cluster -yt deploy-keys/ flinkapp.jar
```

When deployed using YARN, Flink's web UI is accessible through the YARN proxy's "Tracking URL". To 
ensure that the YARN proxy is able to access Flink's HTTPS URL, you need to configure the YARN proxy
to accept Flink's SSL certificates. To do this, add the custom CA certificate into Java's default 
truststore on the YARN proxy node.
