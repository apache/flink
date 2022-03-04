---
title: Authentication with Kerberos
weight: 2
type: docs
aliases:
  - /deployment/security/kerberos.html
  - /ops/security-kerberos.html
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

# Enabling and Configuring Authentication with Kerberos

## What is Kerberos?

[Kerberos](https://web.mit.edu/kerberos/) is a network authentication protocol that provides a secure, 
single-sign-on, trusted, third-party mutual authentication service. It is designed to provide strong 
authentication for client/server applications by using secret-key cryptography.

## How the Flink Security Infrastructure works with Kerberos

A Flink program may use first- or third-party connectors, necessitating arbitrary authentication methods 
(Kerberos, SSL/TLS, username/password, etc.). While satisfying the security requirements for all connectors 
is an ongoing effort, Flink provides first-class support for Kerberos authentication and SSL/TLS only.

Kerberos can be used to authenticate connections to:
- Hadoop and its components (YARN, HDFS, HBase)
- ZooKeeper
- Kafka (0.9+)

The current implementation supports running Flink clusters (JobManager / TaskManager / Jobs) with two
authentication modes: 
- a configured [Kerberos keytab](https://web.mit.edu/kerberos/krb5-devel/doc/basic/keytab_def.html) credential
- [Hadoop delegation tokens](https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/delegation_tokens.html)

In production deployments, streaming jobs usually run for long periods of time. It is important to be 
able to authenticate to secured data sources throughout the lifetime of the job. Kerberos keytabs are 
the preferred authentication approach because they can be used during the entire lifetime of long-running 
stream processing applications, unlike a Hadoop delegation token or ticket cache entry (which expires).

{{< hint info >}}
Keytabs are subject to any password expiration policies that may be imposed on a principal. Thus, if 
a principal's password expires (or the password is changed), a keytab generated using that password 
will be rendered invalid.
{{< /hint >}}

Note that the credentials are tied to a Flink cluster and not to a running job. Thus, all applications 
that run on the same cluster use the same authentication token and all jobs within a cluster will share 
the credentials configured for that cluster. If you need to work with different credentials, you should 
start a new cluster. For example, to use a different keytab for a certain job, simply launch a separate 
Flink cluster with a different configuration. Numerous Flink clusters may run side-by-side in a Kubernetes 
or YARN environment.

Note that it is possible to enable and configure the use of Kerberos independently for each service 
or connector that is capable of being used with Kerberos. For example, you can decide to use Kerberos 
for Hadoop security, but not for ZooKeeper. 

All services using Kerberos will use the same credentials. If you need to run some jobs with different 
Kerberos credentials, those jobs will have to run in a different cluster that is configured to use 
those other credentials. 

## Using Kerberos with Flink Security Modules

The internal architecture of Flink security is based on _security modules_ (which implements [`org.apache.flink.runtime.security.modules.SecurityModule`](https://nightlies.apache.org/flink/flink-docs-master/api/java/org/apache/flink/runtime/security/modules/SecurityModule.html)). 
These modules are installed at startup. 

### Hadoop Security Module

This module uses the Hadoop `UserGroupInformation` (UGI) class to establish a process-wide *login user* 
context. The login user is then used for all interactions with Hadoop, including HDFS, HBase, and YARN.

If Hadoop security is enabled (in `core-site.xml`), the login user will have whatever Kerberos credential 
is configured. Otherwise, the login user conveys only the user identity of the OS account that launched 
the cluster.

### JAAS Security Module

This module provides a dynamic [JAAS](https://docs.oracle.com/javase/8/docs/technotes/guides/security/jaas/JAASRefGuide.html) 
configuration to the cluster, making available the configured Kerberos credential to ZooKeeper, Kafka, 
and other such components that rely on JAAS.

Note that the user may also provide a static JAAS configuration file using the mechanisms described 
in the [Java SE Documentation](http://docs.oracle.com/javase/8/docs/technotes/guides/security/jgss/tutorials/LoginConfigFile.html).   
Static entries override any dynamic entries provided by this module.

### ZooKeeper Security Module

This module configures certain process-wide ZooKeeper security-related settings, namely the ZooKeeper 
service name (default: `zookeeper`) and the JAAS login context name (default: `Client`).

## Ticket Renewal

A Ticket Granting Ticket (TGT) is typically a small, encrypted identification file (but can also be 
a directory, API, keyring, etc) with a limited validity period. The TGT contains the client ID, the 
client network address, the ticket validity period, and the Ticket Granting Server session key.

Each component that uses Kerberos is independently responsible for renewing the Kerberos TGT. Hadoop, 
ZooKeeper, and Kafka all renew the TGT automatically when provided a keytab. In the delegation token 
scenario, YARN itself renews the token (up to its maximum lifespan).

## Summary

The primary goals of the Flink Kerberos security infrastructure are to:

- enable secure data access for jobs within a cluster via connectors
- authenticate to ZooKeeper (if configured to use SASL)
- authenticate to Hadoop components
