---
title: "Encryption and Authentication using SSL/TLS"
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

# Encryption and Authentication using SSL/TLS for Cluster Components

Flink supports mutual authentication (when two parties authenticate each other at the same time) and 
encryption of network communication with SSL for internal and external communication. 

**By default, SSL/TLS authentication and encryption is not enabled** (to have defaults work out-of-the-box).

This guide will explain internal vs external connectivity, and provide instructions on how to enable 
SSL/TLS authentication and encryption for network communication with and between Flink processes. We 
will go through steps such as generating certificates, setting up TrustStores and KeyStores, and 
configuring cipher suites.

For how-tos and tips for different deployment environments (i.e. standalone clusters, Kubernetes, YARN),
check out the section on [Incorporating Security Features in a Running Cluster]({{< ref "docs/security/running-cluster" >}}).

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

These connections are exposed via REST/HTTP endpoints (and are used by Flink's web UI). Some 
application-specific external services such as sources and sinks may use some other network protocol.

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
for both internal and external/REST endpoints.
{{< /hint >}}

You can disable security for different connection types. When `security.ssl.internal.enabled` is set 
to `true`, you can set the following parameters to `false` to disable SSL for that particular connection 
type:

- `taskmanager.data.ssl.enabled` &#8594; Data communication between TaskManagers
- `blob.service.ssl.enabled` &#8594; Transport of BLOBs from JobManager to TaskManager
- `akka.ssl.enabled` &#8594; Akka-based RPC connections between JobManager / TaskManager / ResourceManager

Because internal communication is mutually authenticated between the server and the client, keystore 
and truststore typically refer to a dedicated certificate that acts as a shared secret. In such a setup, 
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

When using a certificate that is not self-signed, but signed by Certified Authorities (CA), you need 
to use certificate pinning to allow only a specific certificate to be trusted when establishing the 
connectivity:

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
for both internal and external/REST endpoints.
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

### Example SSL Setup

#### Internal Connections

Execute the following keytool commands to create a key pair in a keystore:

```bash
$ keytool -genkeypair -alias flink.internal -keystore internal.keystore -dname "CN=flink.internal" -storepass internal_store_password -keyalg RSA -keysize 4096 -storetype PKCS12
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

{{< hint warning >}}
Storing plaintext passwords in config files should not be used in production. Consider using [Kubernetes
secrets](https://kubernetes.io/docs/concepts/configuration/secret/) or environment variables instead.
{{< /hint >}}

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

{{< hint warning >}}
Storing plaintext passwords in config files should not be used in production. Consider using [Kubernetes
secrets](https://kubernetes.io/docs/concepts/configuration/secret/) or environment variables instead.
{{< /hint >}}

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

{{< hint warning >}}
Storing plaintext passwords in config files should not be used in production. Consider using [Kubernetes
secrets](https://kubernetes.io/docs/concepts/configuration/secret/) or environment variables instead.
{{< /hint >}}

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
