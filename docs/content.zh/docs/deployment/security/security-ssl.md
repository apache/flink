---
title: "SSL 设置"
weight: 2
type: docs
aliases:
  - /zh/deployment/security/security-ssl.html
  - /zh/ops/security-ssl.html
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

# SSL 设置

This page provides instructions on how to enable TLS/SSL authentication and encryption for network communication with and between Flink processes.
**NOTE: TLS/SSL authentication is not enabled by default.**

## Internal and External Connectivity

When securing network connections between machines processes through authentication and encryption, Apache Flink differentiates between *internal* and *external* connectivity.
*Internal Connectivity* refers to all connections made between Flink processes. These connections run Flink custom protocols. Users never connect directly to internal connectivity endpoints.
*External / REST Connectivity* endpoints refers to all connections made from the outside to Flink processes. This includes the web UI and REST commands to
start and control running Flink jobs/applications, including the communication of the Flink CLI with the JobManager / Dispatcher.

For more flexibility, security for internal and external connectivity can be enabled and configured separately.

{{< img src="/fig/ssl_internal_external.svg" alt="Internal and External Connectivity" width=75% >}}

### Internal Connectivity

Internal connectivity includes:

  - Control messages: RPC between JobManager / TaskManager / Dispatcher / ResourceManager
  - The data plane: The connections between TaskManagers to exchange data during shuffles, broadcasts, redistribution, etc.
  - The Blob Service (distribution of libraries and other artifacts).

All internal connections are SSL authenticated and encrypted. The connections use **mutual authentication**, meaning both server
and client side of each connection need to present the certificate to each other. The certificate acts effectively as a shared
secret when a dedicated CA is used to exclusively sign an internal cert.
The certificate for internal communication is not needed by any other party to interact with Flink, and can be simply
added to the container images, or attached to the YARN deployment.

  - The easiest way to realize this setup is by generating a dedicated public/private key pair and self-signed certificate
    for the Flink deployment. The key- and truststore are identical and contains only that key pair / certificate.
    An example is [shown below](#example-ssl-setup-standalone-and-kubernetes).

  - In an environment where operators are constrained to use firm-wide Internal CA (cannot generated self-signed certificates),
    the recommendation is to still have a dedicated key pair / certificate for the Flink deployment, signed by that CA.
    However, the TrustStore must then also contain the CA's public certificate tho accept the deployment's certificate
    during the SSL handshake (requirement in JDK TrustStore implementation).
    
    **NOTE:** Because of that, it is critical that you specify the fingerprint of the deployment certificate
    (`security.ssl.internal.cert.fingerprint`), when it is not self-signed, to pin that certificate as the only trusted
    certificate and prevent the TrustStore from trusting all certificates signed by that CA.

*Note: Because internal connections are mutually authenticated with shared certificates, Flink can skip hostname verification.
This makes container-based setups easier.*

### External / REST Connectivity

All external connectivity is exposed via an HTTP/REST endpoint, used for example by the web UI and the CLI:

  - Communication with the *Dispatcher* to submit jobs (session clusters)
  - Communication with the *JobMaster* to inspect and modify a running job/application

The REST endpoints can be configured to require SSL connections. The server will, however, accept connections from any client by default, meaning the REST endpoint does not authenticate the client.

Simple mutual authentication may be enabled by configuration if authentication of connections to the REST endpoint is required, but we recommend to deploy a "side car proxy":
Bind the REST endpoint to the loopback interface (or the pod-local interface in Kubernetes) and start a REST proxy that authenticates and forwards the requests to Flink.
Examples for proxies that Flink users have deployed are [Envoy Proxy](https://www.envoyproxy.io/) or
[NGINX with MOD_AUTH](http://nginx.org/en/docs/http/ngx_http_auth_request_module.html).

The rationale behind delegating authentication to a proxy is that such proxies offer a wide variety of authentication options and thus better integration into existing infrastructures.


### Queryable State

Connections to the queryable state endpoints is currently not authenticated or encrypted.


## Configuring SSL

SSL can be enabled separately for *internal* and *external* connectivity:

  - **security.ssl.internal.enabled**: Enable SSL for all *internal* connections.
  - **security.ssl.rest.enabled**: Enable SSL for *REST / external* connections.

*Note: For backwards compatibility, the **security.ssl.enabled** option still exists and enables SSL for both internal and REST endpoints.*

For internal connectivity, you can optionally disable security for different connection types separately.
When `security.ssl.internal.enabled` is set to `true`, you can set the following parameters to `false` to disable SSL for that particular connection type:

  - `taskmanager.data.ssl.enabled`: Data communication between TaskManagers
  - `blob.service.ssl.enabled`: Transport of BLOBs from JobManager to TaskManager
  - `akka.ssl.enabled`: Akka-based RPC connections between JobManager / TaskManager / ResourceManager

### Keystores and Truststores

The SSL configuration requires to configure a **keystore** and a **truststore**. The *keystore* contains the public certificate
(public key) and the private key, while the truststore contains the trusted certificates or the trusted authorities. Both stores
need to be set up such that the truststore trusts the keystore's certificate.

#### Internal Connectivity

Because internal communication is mutually authenticated between server and client side, keystore and truststore typically refer to a dedicated
certificate that acts as a shared secret. In such a setup, the certificate can use wild card hostnames or addresses.
WHen using self-signed certificates, it is even possible to use the same file as keystore and truststore.

```yaml
security.ssl.internal.keystore: /path/to/file.keystore
security.ssl.internal.keystore-password: keystore_password
security.ssl.internal.key-password: key_password
security.ssl.internal.truststore: /path/to/file.truststore
security.ssl.internal.truststore-password: truststore_password
```

When using a certificate that is not self-signed, but signed by a CA, you need to use certificate pinning to allow only a 
a specific certificate to be trusted when establishing the connectivity.

```yaml
security.ssl.internal.cert.fingerprint: 00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00
```

#### REST Endpoints (external connectivity)

For REST endpoints, by default the keystore is used by the server endpoint, and the truststore is used by the REST clients (including the CLI client)
to accept the server's certificate. In the case where the REST keystore has a self-signed certificate, the truststore must trust that certificate directly.
If the REST endpoint uses a certificate that is signed through a proper certification hierarchy, the roots of that hierarchy should
be in the trust store.

If mutual authentication is enabled, the keystore and the truststore are used by both, the server endpoint and the REST clients as with internal connectivity.

```yaml
security.ssl.rest.keystore: /path/to/file.keystore
security.ssl.rest.keystore-password: keystore_password
security.ssl.rest.key-password: key_password
security.ssl.rest.truststore: /path/to/file.truststore
security.ssl.rest.truststore-password: truststore_password
security.ssl.rest.authentication-enabled: false
```

### Cipher suites

{{< hint warning >}}
The [IETF RFC 7525](https://tools.ietf.org/html/rfc7525) recommends to use a specific set of cipher suites for strong security.
Because these cipher suites were not available on many setups out of the box, Flink's default value is set to a slightly
weaker but more compatible cipher suite.
We recommend that SSL setups update to the stronger cipher suites, if possible, by adding the below entry to the Flink configuration:


```yaml
security.ssl.algorithms: TLS_DHE_RSA_WITH_AES_128_GCM_SHA256,TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,TLS_DHE_RSA_WITH_AES_256_GCM_SHA384,TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384
```

If these cipher suites are not supported on your setup, you will see that Flink processes will not be able to connect to each other.

{{< /hint >}}

### Complete List of SSL Options

{{< generated/security_configuration >}}

## Creating and Deploying Keystores and Truststores

Keys, Certificates, and the Keystores and Truststores can be generatedd using the [keytool utility](https://docs.oracle.com/javase/8/docs/technotes/tools/unix/keytool.html).
You need to have an appropriate Java Keystore and Truststore accessible from each node in the Flink cluster.

  - For standalone setups, this means copying the files to each node, or adding them to a shared mounted directory.
  - For container based setups, add the keystore and truststore files to the container images.
  - For Yarn setups, the cluster deployment phase can automatically distribute the keystore and truststore files.

For the externally facing REST endpoint, the common name or subject alternative names in the certificate should match the node's hostname and IP address.


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

The single key/certificate in the keystore is used the same way by the server and client endpoints (mutual authentication).
The key pair acts as the shared secret for internal security, and we can directly use it as keystore and truststore.

```yaml
security.ssl.internal.enabled: true
security.ssl.internal.keystore: /path/to/flink/conf/internal.keystore
security.ssl.internal.truststore: /path/to/flink/conf/internal.keystore
security.ssl.internal.keystore-password: internal_store_password
security.ssl.internal.truststore-password: internal_store_password
security.ssl.internal.key-password: internal_store_password
```

**REST Endpoint**

The REST endpoint may receive connections from external processes, including tools that are not part of Flink (for example curl request to the REST API).
Setting up a proper certificate that is signed though a CA hierarchy may make sense for the REST endpoint.

However, as mentioned above, the REST endpoint does not authenticate clients and thus typically needs to be secured via a proxy anyways.

**REST Endpoint (simple self signed certificate)**

This example shows how to create a simple keystore / truststore pair. The truststore does not contain the primary key and can
be shared with other applications. In this example, *myhost.company.org / ip:10.0.2.15* is the node (or service) for the JobManager.

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

**Tips to query REST Endpoint with curl utility**

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

## Tips for YARN Deployment

For YARN, you can use the tools of Yarn to help:

  - Configuring security for internal communication is exactly the same as in the example above.

  - To secure the REST endpoint, you need to issue the REST endpoint's certificate such that it is valid for all hosts
    that the JobManager may get deployed to. This can be done with a wild card DNS name, or by adding multiple DNS names.

  - The easiest way to deploy keystores and truststore is by YARN client's *ship files* option (`-yt`).
    Copy the keystore and truststore files into a local directory (say `deploy-keys/`) and start the YARN session as
    follows: `flink run -m yarn-cluster -yt deploy-keys/ flinkapp.jar`

  - When deployed using YARN, Flink's web dashboard is accessible through YARN proxy's Tracking URL.
    To ensure that the YARN proxy is able to access Flink's HTTPS URL, you need to configure YARN proxy to accept Flink's SSL certificates.
    For that, add the custom CA certificate into Java's default truststore on the YARN Proxy node.

{{< top >}}
