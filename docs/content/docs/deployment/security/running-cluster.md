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

This document briefly describes how Flink security works in the context of various deployment
mechanisms (Standalone, native Kubernetes, YARN), filesystems, connectors, and state backends.

## Deployment Modes

Here is some information specific to each deployment mode.

### Standalone Mode

Steps to run a secure Flink cluster in standalone/cluster mode:

1. Add security-related configuration options to the Flink configuration file (on all cluster nodes)
   (see [here]({{< ref "docs/deployment/config" >}}#auth-with-external-systems)).
2. Ensure that the keytab file exists at the path indicated by `security.kerberos.login.keytab` on
   all cluster nodes.
3. Deploy Flink cluster as normal.

### Native Kubernetes and YARN Mode

Steps to run a secure Flink cluster in native Kubernetes and YARN mode:

1. Add security-related configuration options to the Flink configuration file on the client
   (see [here]({{< ref "docs/deployment/config" >}}#auth-with-external-systems)).
2. Ensure that the keytab file exists at the path as indicated by `security.kerberos.login.keytab` on
   the client node.
3. Deploy Flink cluster as normal.

In YARN and native Kubernetes mode, the keytab is automatically copied from the client to the Flink
containers.

To enable Kerberos authentication, the Kerberos configuration file is also required. This file can be
either fetched from the cluster environment or uploaded by Flink. In the latter case, you need to
configure the `security.kerberos.krb5-conf.path` to indicate the path of the Kerberos configuration
file and Flink will copy this file to its containers/pods.

For more information, see the [documentation on YARN security](https://github.com/apache/hadoop/blob/trunk/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-site/src/site/markdown/YarnApplicationSecurity.md).

#### Using `kinit` (YARN only)

In YARN mode, it is possible to deploy a secure Flink cluster without a keytab, using only the ticket
cache (as managed by `kinit`). This avoids the complexity of generating a keytab and avoids entrusting
the cluster manager with it. In this scenario, the Flink CLI acquires Hadoop delegation tokens (for
HDFS and for HBase). The main drawback is that the cluster is necessarily short-lived since the generated
delegation tokens will expire (typically within a week).

Steps to run a secure Flink cluster using `kinit`:

1. Add security-related configuration options to the Flink configuration file on the client
   (see [here]({{< ref "docs/deployment/config" >}}#auth-with-external-systems)).
2. Login using the `kinit` command.
3. Deploy Flink cluster as normal.


## SSL - Tips for YARN Deployment

For YARN, you can use the tools of Yarn to help:

- Configuring security for internal communication is exactly the same as in the example above.

- To secure the REST endpoint, you need to issue the REST endpoint's certificate such that it is
  valid for all hosts that the JobManager may get deployed to. This can be done with a wild card
  DNS name, or by adding multiple DNS names.

- The easiest way to deploy keystores and truststore is by YARN client's *ship files* option (`-yt`).
  Copy the keystore and truststore files into a local directory (say `deploy-keys/`) and start the
  YARN session as follows: `flink run -m yarn-cluster -yt deploy-keys/ flinkapp.jar`

- When deployed using YARN, Flink's web dashboard is accessible through YARN proxy's Tracking URL.
  To ensure that the YARN proxy is able to access Flink's HTTPS URL, you need to configure YARN proxy
  to accept Flink's SSL certificates.
  For that, add the custom CA certificate into Java's default truststore on the YARN Proxy node.


## Creating and Deploying Keystores and Truststores

Keys, Certificates, and the Keystores and Truststores can be generated using the [keytool utility](https://docs.oracle.com/javase/8/docs/technotes/tools/unix/keytool.html).
You need to have an appropriate Java Keystore and Truststore accessible from each node in the Flink cluster.

- For standalone setups, this means copying the files to each node, or adding them to a shared mounted directory.
- For container based setups, add the keystore and truststore files to the container images.
- For Yarn setups, the cluster deployment phase can automatically distribute the keystore and truststore files.

For the externally facing REST endpoint, the common name or subject alternative names in the certificate
should match the node's hostname and IP address.

## Example SSL Setup Standalone and Kubernetes

**Internal Connectivity**

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

The single key/certificate in the keystore is used the same way by the server and client endpoints
(mutual authentication). The key pair acts as the shared secret for internal security, and we can
directly use it as keystore and truststore.

```yaml
security.ssl.internal.enabled: true
security.ssl.internal.keystore: /path/to/flink/conf/internal.keystore
security.ssl.internal.truststore: /path/to/flink/conf/internal.keystore
security.ssl.internal.keystore-password: internal_store_password
security.ssl.internal.truststore-password: internal_store_password
security.ssl.internal.key-password: internal_store_password
```

**REST Endpoint**

The REST endpoint may receive connections from external processes, including tools that are not part
of Flink (for example curl request to the REST API). Setting up a proper certificate that is signed
though a CA hierarchy may make sense for the REST endpoint.

However, as mentioned above, the REST endpoint does not authenticate clients and thus typically needs
to be secured via a proxy anyways.

**REST Endpoint (simple self signed certificate)**

This example shows how to create a simple keystore / truststore pair. The truststore does not contain
the primary key and can be shared with other applications. In this example, *myhost.company.org / ip:10.0.2.15*
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

**REST Endpoint (with a self signed CA)**

Execute the following keytool commands to create a truststore with a self signed CA.

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

**Tips to query REST Endpoint with cURL utility**

You can convert the keystore into the `PEM` format using `openssl`:

```bash
$ openssl pkcs12 -passin pass:rest_keystore_password -in rest.keystore -out rest.pem -nodes
```

Then you can query REST Endpoint with `curl`:

```bash
$ curl --cacert rest.pem flink_url
```

If mutual SSL is enabled:

```bash
$ curl --cacert rest.pem --cert rest.pem flink_url
```
