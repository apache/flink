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

To enable Kerberos authentication, the Kerberos configuration file is also required. This file can be
either fetched from the cluster environment or uploaded by Flink. In the latter case, you need to
configure the `security.kerberos.krb5-conf.path` setting to indicate the path of the Kerberos configuration
file and Flink will copy this file to its containers/pods.

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

## Setting Up SSL 

The easiest way to set up SSL is to generate a dedicated public/private key pair and self-signed 
certificate for the Flink deployment. The keystore and truststore are identical and contains
only that key pair and certificate. 

In an environment where operators are constrained to use firm-wide Internal Certificate Authorities 
(cannot generate self-signed certificates), we recommend to still have a dedicated key pair and certificate
for the Flink deployment, signed by that CA. However, the truststore must then also contain the
CA's public certificate to accept the deployment's certificate during the SSL handshake (a requirement
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

### Example SSL Setup for Standalone Deployment and Kubernetes Deployment

#### Internal Connections

Execute the following keytool commands to create a key pair in a keystore:

```bash
$ keytool -genkeypair \
  -alias flink.internal \
  -keystore internal.keystore \
  -dname "CN=flink.internal" \
  -storepass internal_store_password \
  -keyalg RSA \
  -keysize 4096 \
  -storetype PKCS12
```

The single key/certificate in the keystore is used the same way by the server and the client endpoints
(mutual authentication). The key pair acts as the shared secret for internal security, and you can
directly use it as a keystore and a truststore.

```yaml
security.ssl.internal.enabled: true
security.ssl.internal.keystore: /path/to/flink/conf/internal.keystore
security.ssl.internal.truststore: /path/to/flink/conf/internal.keystore
security.ssl.internal.keystore-password: internal_store_password
security.ssl.internal.truststore-password: internal_store_password
security.ssl.internal.key-password: internal_store_password
```

#### External Connections (REST Endpoints)

The REST endpoint may receive connections from external processes, including tools that are not part
of Flink (i.e. cURL requests to the REST API). Setting up a proper certificate that is signed through 
a CA hierarchy may make sense for the REST endpoint.

However, as mentioned above, the REST endpoint does not authenticate clients and typically needs to
be secured via a proxy anyways.

**REST endpoint (with a simple self-signed certificate)**

This example shows how to create a simple keystore / truststore pair. The truststore does not contain
the private key and can be shared with other applications. In this example, *myhost.company.org / ip:10.0.2.15*
is the node (or service) for the JobManager.

```bash
$ keytool -genkeypair -alias flink.rest -keystore rest.keystore -dname "CN=myhost.company.org" -ext "SAN=dns:myhost.company.org,ip:10.0.2.15" -storepass rest_keystore_password -keyalg RSA -keysize 4096 -storetype PKCS12

$ keytool -exportcert -keystore rest.keystore -alias flink.rest -storepass rest_keystore_password -file flink.cer

$ keytool -importcert -keystore rest.truststore -alias flink.rest -storepass rest_truststore_password -file flink.cer -noprompt
```

```yaml
security.ssl.rest.enabled: true
security.ssl.rest.keystore: /path/to/flink/conf/rest.keystore
security.ssl.rest.truststore: /path/to/flink/conf/rest.truststore
security.ssl.rest.keystore-password: rest_keystore_password
security.ssl.rest.truststore-password: rest_truststore_password
security.ssl.rest.key-password: rest_keystore_password
```

**REST endpoint (with a self-signed CA)**

Execute the following keytool commands to create a truststore with a self-signed CA:

```bash
$ keytool -genkeypair -alias ca -keystore ca.keystore -dname "CN=Sample CA" -storepass ca_keystore_password -keyalg RSA -keysize 4096 -ext "bc=ca:true" -storetype PKCS12

$ keytool -exportcert -keystore ca.keystore -alias ca -storepass ca_keystore_password -file ca.cer

$ keytool -importcert -keystore ca.truststore -alias ca -storepass ca_truststore_password -file ca.cer -noprompt
```

Now create a keystore for the REST endpoint with a certificate signed by the above CA.
Let *flink.company.org / ip:10.0.2.15* be the hostname of the JobManager.

```bash
$ keytool -genkeypair -alias flink.rest -keystore rest.signed.keystore -dname "CN=flink.company.org" -ext "SAN=dns:flink.company.org" -storepass rest_keystore_password -keyalg RSA -keysize 4096 -storetype PKCS12

$ keytool -certreq -alias flink.rest -keystore rest.signed.keystore -storepass rest_keystore_password -file rest.csr

$ keytool -gencert -alias ca -keystore ca.keystore -storepass ca_keystore_password -ext "SAN=dns:flink.company.org,ip:10.0.2.15" -infile rest.csr -outfile rest.cer

$ keytool -importcert -keystore rest.signed.keystore -storepass rest_keystore_password -file ca.cer -alias ca -noprompt

$ keytool -importcert -keystore rest.signed.keystore -storepass rest_keystore_password -file rest.cer -alias flink.rest -noprompt
```

Now add the following configuration to your `flink-conf.yaml`:

```yaml
security.ssl.rest.enabled: true
security.ssl.rest.keystore: /path/to/flink/conf/rest.signed.keystore
security.ssl.rest.truststore: /path/to/flink/conf/ca.truststore
security.ssl.rest.keystore-password: rest_keystore_password
security.ssl.rest.key-password: rest_keystore_password
security.ssl.rest.truststore-password: ca_truststore_password
```

**Querying the REST endpoint with the cURL utility**

You can convert the keystore into the `PEM` format using `openssl`:

```bash
$ openssl pkcs12 -passin pass:rest_keystore_password -in rest.keystore -out rest.pem -nodes
```

Then you can query the REST Endpoint with `curl`:

```bash
$ curl --cacert rest.pem flink_url
```

If mutual SSL is enabled:

```bash
$ curl --cacert rest.pem --cert rest.pem flink_url
```
