---
title: "Encryption and Authentication using SSL"
weight: 3
type: docs
aliases:
  - /deployment/security/ssl.html
  - /ops/security-ssl.html
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

# Encryption and Authentication using SSL

Flink supports mutual authentication (when two parties authenticate each other at the same time) and 
encryption of network communication with SSL for internal and external communication. 

**By default, SSL/TLS authentication and encryption is not enabled** (to have defaults work out-of-the-box).

This guide will explain internal vs external connectivity, and provide instructions on how to enable 
SSL/TLS authentication and encryption for network communication with and between Flink processes. We 
will go through steps such as generating certificates, setting up TrustStores and KeyStores, and 
configuring cipher suites.

For how-tos and tips for different deployment environments (i.e. standalone clusters, Kubernetes, YARN),
check out the section on [Incorporating Security Features in a Running Cluster](#).

## Internal and External Communication 

There are two types of network connections to authenticate and encrypt: internal and external.

{{< img src="/fig/ssl_internal_external.svg" alt="Internal and External Connectivity" width=75% >}}

For more flexibility, security for internal and external connectivity can be enabled and configured
separately.

### Internal Connectivity

Flink internal communication refers to all connections made between Flink processes. These include
RPC calls, data transfer, and blob service communication to distribute libraries or other artifacts.
For such connections, all Flink processes (i.e. between the Task Managers, between the Task Managers
and the Flink Master) perform mutual authentication, where senders and receivers validate each other
via an SSL certificate. The certificate acts as a shared secret and can be embedded into containers
or attached to your deployment setup. These connections run Flink custom protocols. Users never connect
directly to internal connectivity endpoints.

Internal connectivity includes:

  - Control messages: RPC between JobManager / TaskManager / Dispatcher / ResourceManager
  - The data plane: The connections between TaskManagers to exchange data during shuffles, broadcasts, 
    redistribution, etc
  - The Blob Service (distribution of libraries and other artifacts)

All internal connections are SSL authenticated and encrypted. The connections use **mutual authentication**, 
meaning both server and client side of each connection need to present the certificate to each other. 
The certificate acts effectively as a shared secret when a dedicated CA is used to exclusively sign 
an internal cert. The certificate for internal communication is not needed by any other party to interact 
with Flink, and can be simply added to the container images, or attached to the YARN deployment.

  - The easiest way to realize this setup is by generating a dedicated public/private key pair and 
    self-signed certificate for the Flink deployment. The key and truststore are identical and contains 
    only that key pair / certificate. An example is [shown below](#example-ssl-setup-standalone-and-kubernetes).

  - In an environment where operators are constrained to use firm-wide Internal CA (cannot generate 
    self-signed certificates), the recommendation is to still have a dedicated key pair / certificate 
    for the Flink deployment, signed by that CA. However, the TrustStore must then also contain the 
    CA's public certificate tho accept the deployment's certificate during the SSL handshake (requirement 
    in JDK TrustStore implementation).
    
    **NOTE:** Because of that, it is critical that you specify the fingerprint of the deployment certificate
    (`security.ssl.internal.cert.fingerprint`), when it is not self-signed, to pin that certificate 
    as the only trusted certificate and prevent the TrustStore from trusting all certificates signed 
    by that CA.

*Note: Because internal connections are mutually authenticated with shared certificates, Flink can 
skip hostname verification. This makes container-based setups easier.*

### External Connectivity

Flink external communication refers to all connections made from the outside to Flink processes. This
includes the web UI and REST commands to start and control running Flink jobs/applications, including
the communication of the Flink CLI with the JobManager / Dispatcher. These include submitting and controlling
Flink applications and accessing the REST interface (i.e. the web UI). Most of these connections use
REST/HTTP endpoints. Some external services used as sources or sinks may use some other network protocol.

You can enable SSL encryption and mutual authentication for external connections. However, the
recommended approach is setting up and configuring a dedicated proxy service that controls access to
the REST endpoint because proxy services offer more authentication and configuration options than Flink.
Encryption and authentication for communication to queryable state is not supported yet.

- external communication - used by the web UI and other user application's to interact with flink

All external connectivity is exposed via an HTTP/REST endpoint, used for example by the web UI and the CLI:

  - Communication with the *Dispatcher* to submit jobs (session clusters)
  - Communication with the *JobMaster* to inspect and modify a running job/application

The REST endpoints can be configured to require SSL connections. The server will, however, accept 
connections from any client by default, meaning the REST endpoint does not authenticate the client.

Simple mutual authentication may be enabled by configuration if authentication of connections to the 
REST endpoint is required, but we recommend deploying a "sidecar proxy":
Bind the REST endpoint to the loopback interface (or the pod-local interface in Kubernetes) and start 
a REST proxy that authenticates and forwards the requests to Flink.
Examples for proxies that Flink users have deployed are [Envoy Proxy](https://www.envoyproxy.io/) or
[NGINX with MOD_AUTH](http://nginx.org/en/docs/http/ngx_http_auth_request_module.html).

The rationale behind delegating authentication to a proxy is that such proxies offer a wide variety 
of authentication options and thus better integration into existing infrastructures.


### Queryable State

Connections to the [queryable state]({{< ref "docs/dev/datastream/fault-tolerance/queryable_state" >}}) 
endpoints is currently not authenticated or encrypted.

## SSL setup

<SVG width='100%' src="/images/security/ssl-mutual-auth.svg" />

<Notes>

Each participant has a keystore and a truststore.

A keystore contains a certificate (which contains a public key), and a private key.
A truststore contains trusted certificates, and certificate chains/authorities.
Keystores and truststores are files.

Establishing encrypted, authenticated communication is a multi-step process, shown in the figure. Certificates are exchanged and validated against the truststore, after which the two parties can safely communicate.

</Notes>

### Typical SSL setup

For mutually authenticated internal connections:

* keystore and truststore can contain the same dedicated certificate
* can use wildcard hostnames or addresses
* can even use the same file for both keystore and truststore

<Notes>

In the case of internal communication between servers in a Flink cluster, a secure setup can be easily established. All that is needed is a single, self-signed certificate that all parties use as both their keystore and truststore.

You can also use this simple approach to mutual authentication for communication between clients and the Flink Master, if you want. We'll look at the details of securing client connections next.

</Notes>

### Configuring keystores and truststores

The SSL configuration requires configuring a **keystore** and a **truststore**. The *keystore* contains
the public certificate (public key) and the private key, while the truststore contains the trusted
certificates or the trusted authorities. Both stores need to be set up such that the truststore trusts
the keystore's certificate.

* Use the [keytool utility](https://docs.oracle.com/javase/8/docs/technotes/tools/unix/keytool.html) to generate keys, certificates, keystores, and truststores

    ```
    keytool -genkeypair -alias flink.internal -keystore internal.keystore \
    -dname "CN=flink.internal" -storepass internal_store_password -keyalg RSA \
    -keysize 4096 -storetype PKCS12
    ```

* Standalone clusters
    - copy the files to each node, or add them to a shared mounted filesystem
* Containerized clusters
    - add the files to the container images
* Yarn
    - the cluster deployment phase can distribute these files
* See the [docs](https://nightlies.apache.org/flink/flink-docs-stable/ops/security-ssl.html#example-ssl-setup-standalone-and-kubernetes) for more details and examples

### Using Cipher Suites

While the acts of encryption and decryption themselves are performed by keys, cipher suites outline
the set of steps that the keys must follow to do so and the order in which these steps are executed.
There are numerous cipher suites out there, each one with varying instructions on the encryption and
decryption process.

{{< hint warning >}}
The [IETF RFC 7525](https://tools.ietf.org/html/rfc7525) recommends using a specific set of cipher
suites for strong security. Because these cipher suites were not available on many setups out-of-the-box,
Flink's default value is set to a slightly weaker but more compatible cipher suite. We recommend that
SSL setups update to the stronger cipher suites, if possible, by adding the below entry to the Flink
configuration:

```yaml
security.ssl.algorithms: TLS_DHE_RSA_WITH_AES_128_GCM_SHA256,TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,TLS_DHE_RSA_WITH_AES_256_GCM_SHA384,TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384
```

If these cipher suites are not supported on your setup, you will see that Flink processes will not
be able to connect to each other.

{{< /hint >}}

* Out of the box, Flink uses `TLS_RSA_WITH_AES_128_CBC_SHA`
* If stronger encryption is available in your environment, we recommend

```
security.ssl.algorithms: TLS_DHE_RSA_WITH_AES_128_GCM_SHA256,TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256, 
TLS_DHE_RSA_WITH_AES_256_GCM_SHA384, TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384
```

<Notes>

Flink defaults to TLS_RSA_WITH_AES_128_CBC_SHA because it is more widely available, but it is weaker
than the alternatives.

</Notes>

### Configuring SSL

SSL can be enabled separately for *internal* and *external* connectivity:

  - **security.ssl.internal.enabled**: Enable SSL for all *internal* connections.
  - **security.ssl.rest.enabled**: Enable SSL for *REST / external* connections.

*Note: For backwards compatibility, the **security.ssl.enabled** option still exists and enables SSL 
for both internal and REST endpoints.*

For internal connectivity, you can optionally disable security for different connection types separately.
When `security.ssl.internal.enabled` is set to `true`, you can set the following parameters to `false` 
to disable SSL for that particular connection type:

  - `taskmanager.data.ssl.enabled`: Data communication between TaskManagers
  - `blob.service.ssl.enabled`: Transport of BLOBs from JobManager to TaskManager
  - `akka.ssl.enabled`: Akka-based RPC connections between JobManager / TaskManager / ResourceManager



### Configuring SSL for Internal Connectivity

Because internal communication is mutually authenticated between server and client side, keystore and 
truststore typically refer to a dedicated certificate that acts as a shared secret. In such a setup, 
the certificate can use wild card hostnames or addresses. When using self-signed certificates, it is 
even possible to use the same file as keystore and truststore.

```yaml
security.ssl.internal.keystore: /path/to/file.keystore
security.ssl.internal.keystore-password: keystore_password
security.ssl.internal.key-password: key_password
security.ssl.internal.truststore: /path/to/file.truststore
security.ssl.internal.truststore-password: truststore_password
```

When using a certificate that is not self-signed, but signed by a CA, you need to use certificate 
pinning to allow only a specific certificate to be trusted when establishing the connectivity.

```yaml
security.ssl.internal.cert.fingerprint: 00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00
```

### Configuring SSL for External Connectivity (REST Endpoints)

For REST endpoints, by default the keystore is used by the server endpoint, and the truststore is used 
by the REST clients (including the CLI client) to accept the server's certificate. In the case where 
the REST keystore has a self-signed certificate, the truststore must trust that certificate directly.
If the REST endpoint uses a certificate that is signed through a proper certification hierarchy, the 
roots of that hierarchy should be in the trust store.

If mutual authentication is enabled, the keystore and the truststore are used by both, the server 
endpoint and the REST clients as with internal connectivity.

```yaml
security.ssl.rest.keystore: /path/to/file.keystore
security.ssl.rest.keystore-password: keystore_password
security.ssl.rest.key-password: key_password
security.ssl.rest.truststore: /path/to/file.truststore
security.ssl.rest.truststore-password: truststore_password
security.ssl.rest.authentication-enabled: false
```

### Complete List of SSL Options

{{< generated/security_configuration >}}
