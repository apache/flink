---
title: "SSL Setup"
nav-parent_id: setup
nav-pos: 9
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

This page provides instructions on how to enable transport security (SSL) for the network communication between different flink components.

## SSL Configuration

SSL can be enabled for all network endpoints within Flink.  All endpoints share a common configuration.  SSL may be disabled for a given endpoint using the corresponding override option.

See the [Configuration]({{site.baseurl}}/setup/config.html#ssl-settings) page for detailed options.

Here's a summary of each endpoint's SSL features and override option.
 
| Endpoint Name   | Override Option (1)                               | Default  | Trust Verifier (2)  | Hostname Verification |
|-----------------|----------------------------------------------------|---------|---------------------|-----------------------|
| Data Exchange   | `taskmanager.data.ssl.enabled`                     | `true`  | Flink               | Optional (3)          |
| Blob Service    | `blob.service.ssl.enabled`                         | `true`  | Flink               | Optional (3)          |
| Akka Remoting   | `akka.ssl.enabled`                                 | `true`  | Flink               | Never                 |
| Web UI          | `jobmanager.web.ssl.enabled`                       | `true`  | Web Browser / Proxy | Always (4)            |
| Artifact Server | `mesos.resourcemanager.artifactserver.ssl.enabled` | `true`  | Mesos Agent         | Always (4)            |

(1) SSL must be also enabled with `security.ssl.enabled` which defaults to `false`. 

(2) Indicates which process acts as the client who verifies the certificate against a truststore.

(3) Configurable with `security.ssl.verify-hostname` which defaults to `true`.

(4) Verification is performed by an external component (as per truststore location).

## Certificate Requirements

In some deployment modes, all endpoints share a common keystore and rely on a single certificate.  That certificate must be compatible with the configuration; depending on your environment, SSL may need to be disabled for some endpoints, and/or hostname verification disabled.   

### Supporting Trust Verification

The server certificate is verified against a set of trusted certificates as contained in a truststore.   The truststore may contain either the certificate of each node, or simply a common intermediate/CA certificate.

The truststore configured for use by Flink is used for some, but not all, endpoints.   As shown in the column labeled 'trust verifier' in the
above table, the Web UI and Artifact Server endpoints are accessed by external components, therefore the truststore of those components must be considered.

For example, you may need to disable SSL on the Web UI and Artifact Server endpoints to use a self-signed certificate.

### Supporting Hostname Verification

Hostname verification is a step performed by an SSL client, that checks whether the certificate provided by the server matches the expected hostname of the connection URL.   This check is in addition to normal trust verification.  Use either a certificate containing multiple Subject Alternate Names (SANs) - one for each host - or a wildcard certificate.   A wildcard certificate is ideal for YARN/Mesos environments because the available hosts may change over time.

Flink uses the canonical hostname (i.e. FQDN) for hostname verification purposes.   

## Deploying Keystores and Truststores

### Standalone Mode

In local and standalone modes, copy or create a keystore and truststore on each node, and configure the `security.ssl.keystore` and `security.ssl.truststore` paths accordingly. 

### YARN Mode

In YARN deployment mode, the configured paths are interpreted as local paths on each node.  Copy the keystore and truststore to the same location on each node.  Note that the `flink` CLI also uses the configured keystore and truststore.

Use the file-shipping functionality of `yarn-session.sh` to automatically copy the keystore and truststore to each node.

### Mesos Mode

In Mesos deployment mode, the Application Master automatically copies the configured keystore and truststore to any TaskManagers that it launches.  It is not possible to  use a different keystore or truststore on each host.

## Examples

### Creating a Private Certificate Authority

Execute the following keytool commands to create a keystore acting as a private certificate authority.

```
$ keytool -genkeypair -alias ca -keystore ca.keystore -dname "CN=Sample CA" -storepass password -keypass password -keyalg RSA -ext bc=ca:true
$ keytool -keystore ca.keystore -storepass password -alias ca -exportcert > ca.cer
$ keytool -importcert -keystore ca.truststore -alias ca -storepass password -noprompt -file ca.cer
```

The above produces a keystore (named `ca.keystore`) for signing certificate requests, as shown next.  

Configure Flink to use the generated truststore (named `ca.truststore`) since it contains the CA public certificate.

### Creating a Multi-Host Certificate

Execute the following keytool commands to create a certificate containing a Subject Alternate Name (SAN) for each host and signed by the private CA.  Let `node1.example.com` and `node2.example.com` be the hostnames of nodes 1 and 2 respectively.

```
$ keytool -genkeypair -alias node -keystore node.keystore -dname "CN=example.com" -ext SAN=DNS:node1.example.com,DNS:node2.example.com -storepass password -keypass password -keyalg RSA
$ keytool -certreq -keystore node.keystore -storepass password -alias node -file node.csr
$ keytool -gencert -keystore ca.keystore -storepass password -alias ca -ext SAN=DNS:node1.example.com,DNS:node2.example.com -infile node.csr -outfile node.cer
$ keytool -importcert -keystore node.keystore -storepass password -file ca.cer -alias ca -noprompt
$ keytool -importcert -keystore node.keystore -storepass password -file node.cer -alias node -noprompt
```

The above produces a keystore named `node.keystore`.

### Creating a Wildcard Certificate

Execute the following keytool commands to create a wildcard certificate covering all nodes in the cluster and signed by the private CA.  Let `example.com` be the sub-domain of all nodes in the cluster.  Note that SANs aren't used in this scenario.

```
$ keytool -genkeypair -alias wildcard -keystore wildcard.keystore -dname "CN=*.example.com" -storepass password -keypass password -keyalg RSA
$ keytool -certreq -keystore wildcard.keystore -storepass password -alias wildcard -file wildcard.csr
$ keytool -gencert -keystore ca.keystore -storepass password -alias ca -infile wildcard.csr -outfile wildcard.cer
$ keytool -importcert -keystore wildcard.keystore -storepass password -file ca.cer -alias ca -noprompt
$ keytool -importcert -keystore wildcard.keystore -storepass password -file wildcard.cer -alias wildcard -noprompt
```

The above produces a keystore named `wildcard.keystore`.

### Example: Standalone Deployment
Configure each node in the standalone cluster to pick up the keystore and truststore that you've copied to the local file system.

#### Configuration
~~~
security.ssl.enabled: true
security.ssl.keystore: /etc/node.keystore
security.ssl.keystore-password: password
security.ssl.key-password: password
security.ssl.truststore: /etc/ca.truststore
security.ssl.truststore-password: password
~~~

Restart the flink JobManager and TaskManager as necessary.

### Example: YARN Deployment
The keystore and truststore can be deployed to YARN in numerous ways depending on the cluster setup. Following are 2 ways to achieve this:

#### 1. Deploy keystore/truststore manually to all nodes
Copy the keystore and truststore to all nodes in the YARN cluster where flink components can potentially be executed. Since the same configuration file is used by all nodes, copy the files to the same path on all nodes.

Now, start the YARN session from the CLI as normal.

#### 2. Use file-shipping to deploy the keystores and truststore
We can use the Flink client's ship files option (-yt) to distribute the keystore and truststore.  

Copy the keystore and truststore to a directory on the client called `certs/`.  The directory must be within the working directory from where you execute the CLI command.

```
security.ssl.enabled: true
security.ssl.keystore: certs/node.keystore
security.ssl.keystore-password: password
security.ssl.key-password: password
security.ssl.truststore: certs/ca.truststore
security.ssl.truststore-password: password
```

Start the YARN session using the `-yt` parameter:
```
$ flink run -m yarn-cluster -yt certs/ WordCount.jar
```

When deployed using YARN, Flink's web dashboard is accessible through the YARN UI (via the Tracking URL).   YARN encapsulates the web dashboard behind a proxy 
whose truststore must contain your CA certificate.  The YARN proxy uses Java's default truststore (see the [JSSE Reference Guide](http://docs.oracle.com/javase/7/docs/technotes/guides/security/jsse/JSSERefGuide.html#X509TrustManager) for details).

## Troubleshooting

### General Suggestions

- Increase Flink's log verbosity.
- Enable SSL debug information:
```
$ export JVM_ARGS=$JVM_ARGS -Djavax.net.debug=ssl
```
- Disable hostname verification.
- Selectively disable SSL on Flink endpoints.

### Specific Issues

##### "Unable to find valid certification path to requested target"
This message indicates that the keystore certificate could not be validated with the provided truststore.

##### "java.security.cert.CertificateException: No name matching &lt;host&gt; found"
This message indicates a hostname verification failure.

##### Connection timeout (CLI)
May indicate a trust verification failure between the CLI and the Job Manager, since Akka silently rejects untrusted connections.

##### Task Manager fails to start (Mesos-only)
Mesos may be unable to download artifacts from the AppMaster's internal HTTP(s) server.  
- Look at the `stderr` file in the Mesos task container for fetcher-related errors.
- Disable SSL on the Artifact Server endpoint.

##### Job fails during execution
May indicate a hostname verification or other SSL issue within the Netty-based Data Exchange endpoint, since such
connections are made on-demand during job execution.
