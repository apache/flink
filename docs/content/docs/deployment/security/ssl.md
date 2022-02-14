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



### External Connectivity



### Queryable State

Connections to the [queryable state]({{< ref "docs/dev/datastream/fault-tolerance/queryable_state" >}}) 
endpoints is currently not authenticated or encrypted.

## SSL setups

<SVG width='100%' src="/images/security/ssl-mutual-auth.svg" />

Each participant has a keystore and a truststore, which are files. 

A keystore contains a certificate (which contains a public key) and a private key. A truststore 
contains trusted certificates and certificate chains/authorities. 

Establishing encrypted, authenticated communication is a multi-step process, shown in the figure. 
Certificates are exchanged and validated against the truststore, after which the two parties can 
safely communicate.

### Typical SSL setup in Flink

For mutually authenticated internal connections:

- a keystore and a truststore can contain the same dedicated certificate 
- wildcard hostnames or addresses can be used 
- the same file can be used for both keystore and truststore

In the case of internal communication between servers in a Flink cluster, a secure setup can be easily 
established. All that is needed is a single, self-signed certificate that all parties use as both their 
keystore and truststore.

You can also use this approach for external communication when establishing mutual authentication for 
communication between clients and the Flink Master.

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
