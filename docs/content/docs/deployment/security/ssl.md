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

Flink internal communication refers to all connections made between Flink processes. These include:

- Control messages: RPC between JobManager / TaskManager / Dispatcher / ResourceManager
- Transfers on the data plane: connections between TaskManagers to exchange data during shuffles, 
  broadcasts, redistribution, etc
- Blob service communication: distribution of libraries and other artifacts

All internal connections are SSL authenticated and encrypted. The connections use **mutual authentication**,
meaning both server and client side of each connection need to present the certificate to each other. 
The certificate acts as a shared secret and can be embedded into container images or attached to your 
deployment setup. These connections run Flink custom protocols. Users never connect directly to internal 
connectivity endpoints.

### External Connectivity

Flink external communication refers to all connections made from the outside to Flink processes. 
This includes: 
- communication with the Dispatcher to submit Flink jobs (session clusters)
- communication of the Flink CLI with the JobManager to inspect and modify a running Flink job/application

Most of these connections are exposed via REST/HTTP endpoints (and used by the web UI). Some external 
services used as sources or sinks may use some other network protocol.

The server will, by default, accept connections from any client, meaning that the REST endpoint does 
not authenticate the client. These REST endpoints, however, can be configured to require SSL encryption 
and mutual authentication. 

However, the recommended approach is setting up and configuring a dedicated proxy service (a "sidecar 
proxy") that controls access to the REST endpoint. This involves binding the REST endpoint to the 
loopback interface (or the pod-local interface in Kubernetes) and starting a REST proxy that authenticates 
and forwards the requests to Flink. Examples for proxies that Flink users have deployed are [Envoy Proxy](https://www.envoyproxy.io/) 
or [NGINX with MOD_AUTH](http://nginx.org/en/docs/http/ngx_http_auth_request_module.html).

The rationale behind delegating authentication to a proxy is that such proxies offer a wide variety
of authentication options and thus better integration into existing infrastructures.

## Queryable State

Connections to the [queryable state]({{< ref "docs/dev/datastream/fault-tolerance/queryable_state" >}}) 
endpoints is currently not authenticated or encrypted.

## SSL Setups

{{< img src="/fig/ssl_mutual_auth.svg" alt="SSL Mutual Authentication" width=75% >}}

Each participant has a keystore and a truststore, which are files. 

A keystore contains a certificate (which contains a public key) and a private key. A truststore 
contains trusted certificates and certificate chains/authorities. 

Establishing encrypted, authenticated communication is a multi-step process, shown in the figure. 
Certificates are exchanged and validated against the truststore, after which the two parties can 
safely communicate.

### Typical SSL Setup in Flink

For mutually authenticated internal connections, note that:

- a keystore and a truststore can contain the same dedicated certificate 
- the same file can be used for both keystore and truststore
- wildcard hostnames or addresses can be used 

For internal communication between servers in a Flink cluster, a secure setup can be established with 
a single, self-signed certificate that all parties use as both their keystore and truststore. You can 
also use this approach for external communication when establishing mutual authentication for communication 
between clients and the Flink Master.

### Configuring Keystores and Truststores

The SSL configuration requires configuring a keystore and a truststore such that the truststore trusts
the keystore's certificate.

You can use the [keytool utility](https://docs.oracle.com/javase/8/docs/technotes/tools/unix/keytool.html) 
to generate keys, certificates, keystores, and truststores:

```bash
    keytool -genkeypair -alias flink.internal -keystore internal.keystore \
    -dname "CN=flink.internal" -storepass internal_store_password -keyalg RSA \
    -keysize 4096 -storetype PKCS12
```

| Deployment mode        | How to add the files                                                    |
|------------------------|-------------------------------------------------------------------------|
| Standalone clusters    | copy the files to each node, or add them to a shared mounted filesystem | 
| Containerized clusters | add the files to the container images                                   |
| YARN                   | the cluster deployment phase can distribute these files                 |

### Using Cipher Suites

While the acts of encryption and decryption themselves are performed by keys, cipher suites outline
the set of steps that the keys must follow to do so and the order in which these steps are executed.
There are numerous cipher suites out there, each one with varying instructions on the encryption and
decryption process.

{{< hint warning >}}
The [IETF RFC 7525](https://tools.ietf.org/html/rfc7525) recommends using a specific set of cipher
suites for strong security. Since these cipher suites are not available on many setups out-of-the-box,
Flink defaults to TLS_RSA_WITH_AES_128_CBC_SHA (a slightly weaker but more widely available cipher suite). 

If stronger encryption is available in your environment, we recommend that you update your SSL setup
to the stronger cipher suites by adding the below entry to the Flink configuration file (`flink-conf.yaml`):

```yaml
security.ssl.algorithms: TLS_DHE_RSA_WITH_AES_128_GCM_SHA256,TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,TLS_DHE_RSA_WITH_AES_256_GCM_SHA384,TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384
```

If these cipher suites are not supported in your setup, you will see that Flink processes will not
be able to connect to each other.
{{< /hint >}}

### Configuring SSL for Internal Connectivity

The following setting in `flink-conf.yaml` is used to enable SSL for all internal connections:

```yaml
security.ssl.internal.enabled: true
```

{{< hint info >}}
For backwards compatibility, the **security.ssl.enabled** option still exists and enables SSL
for both internal and REST endpoints.
{{< /hint >}}

You can optionally disable security for different connection types separately. 
When `security.ssl.internal.enabled` is set to `true`, you can set the following parameters to `false`
to disable SSL for that particular connection type:

- `taskmanager.data.ssl.enabled`: Data communication between TaskManagers
- `blob.service.ssl.enabled`: Transport of BLOBs from JobManager to TaskManager
- `akka.ssl.enabled`: Akka-based RPC connections between JobManager / TaskManager / ResourceManager

Because internal communication is mutually authenticated between the server and the client, keystore and 
truststore typically refer to a dedicated certificate that acts as a shared secret. In such a setup, 
the certificate can use wildcard hostnames or addresses. When using self-signed certificates, it is 
even possible to use the same file as keystore and truststore.

Take note of the following configuration settings:

```yaml
security.ssl.internal.keystore: /path/to/file.keystore
security.ssl.internal.keystore-password: keystore_password
security.ssl.internal.key-password: key_password
security.ssl.internal.truststore: /path/to/file.truststore
security.ssl.internal.truststore-password: truststore_password
```

When using a certificate that is not self-signed, but signed by a CA, you need to use certificate 
pinning to allow only a specific certificate to be trusted when establishing the connectivity:

```yaml
security.ssl.internal.cert.fingerprint: 00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00
```

### Configuring SSL for External Connectivity (REST Endpoints)

The following setting in `flink-conf.yaml` is used to enable SSL for REST/external connections:

```yaml
security.ssl.rest.enabled: true
```

{{< hint info >}}
For backwards compatibility, the **security.ssl.enabled** option still exists and enables SSL
for both internal and REST endpoints.
{{< /hint >}}

By default, the keystore is used by the server REST endpoints, and the truststore is used 
by the REST clients (including the CLI client) to accept the server's certificate. In the case where 
the REST keystore has a self-signed certificate, the truststore must trust that certificate directly.
If the REST endpoint uses a certificate that is signed through a proper certification hierarchy, the 
roots of that hierarchy should be in the truststore.

If mutual authentication is enabled, the keystore and the truststore are used by both the server 
endpoints and the REST clients.

Take note of the following configuration settings:

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
