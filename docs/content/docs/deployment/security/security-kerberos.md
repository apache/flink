---
title: Kerberos
weight: 3
type: docs
aliases:
  - /deployment/security/security-kerberos.html
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

# Kerberos Authentication Setup and Configuration

This document briefly describes how Flink security works in the context of various deployment
mechanisms (Standalone, native Kubernetes, YARN), filesystems, connectors, and state backends.

## Objective
The primary goals of the Flink Kerberos security infrastructure are:
* to enable secure data access for jobs within a cluster via connectors (e.g. Kafka)
* to authenticate to ZooKeeper (if configured to use SASL)
* to authenticate to Hadoop components (e.g. HDFS, HBase) 

In a production deployment scenario, streaming jobs are understood to run for long periods of time
(days/weeks/months) and be able to authenticate to secure data sources throughout the life of the job.
Kerberos keytab do not expire in that timeframe, unlike credential cache or Hadoop delegation token.

The current implementation supports running Flink clusters (JobManager / TaskManager / jobs) with:
* Keytab file (preferred)
* Credential cache (for example credential cache file created by `kinit`)
* Hadoop delegation tokens (the user provided tokens are not renewed and may be overwritten by Flink)

Keep in mind that all jobs share the credential configured for a given cluster.  
To use a different keytab for a certain job, simply launch a separate Flink cluster with a different configuration.  
Numerous Flink clusters may run side-by-side in a Kubernetes or YARN environment.

## How Flink Security works
Conceptually, a Flink program may use first- or third-party connectors (Kafka, HDFS, Cassandra, Flume, Kinesis etc.)
necessitating arbitrary authentication methods (Kerberos, SSL/TLS, username/password, etc.).
While satisfying the security requirements for all connectors is an ongoing effort,
Flink provides first-class support for Kerberos authentication only.
The following services and connectors are supported for Kerberos authentication:

- Kafka (0.9+)
- HDFS
- HBase
- ZooKeeper

Note that it is possible to enable the use of Kerberos independently for each service or connector.
For example, the user may enable Hadoop security without necessitating the use of Kerberos for ZooKeeper,
or vice versa. The shared element is the configuration of Kerberos credentials, which is then explicitly
used by each component.

The internal architecture is based on security modules (implementing `org.apache.flink.runtime.security.modules.SecurityModule`)
which are installed at startup. The following sections describes each security module.

### Hadoop Security Module
This module uses the Hadoop `UserGroupInformation` (UGI) class to establish a process-wide *login user* context.
The login user is then used for all interactions with Hadoop, including HDFS, HBase, and YARN.

If Hadoop security is enabled (in `core-site.xml`), the login user will have whatever Kerberos credential is configured.
Otherwise, the login user conveys only the user identity of the OS account that launched the cluster.
In order to be specific the login process has the following order of precedence:
* When `hadoop.security.authentication` is set to `kerberos`
  * When `security.kerberos.login.keytab` and `security.kerberos.login.principal` configured then keytab login performed
  * When `security.kerberos.login.use-ticket-cache` configured then credential cache login performed
* All other cases user identity of the OS account used

### JAAS Security Module
This module provides a dynamic JAAS configuration to the cluster, making available the configured Kerberos credential to ZooKeeper,
Kafka, and other such components that rely on JAAS.

Note that the user may also provide a static JAAS configuration file using the mechanisms described in the [Java SE Documentation](http://docs.oracle.com/javase/7/docs/technotes/guides/security/jgss/tutorials/LoginConfigFile.html).   Static entries override any
dynamic entries provided by this module.

### ZooKeeper Security Module
This module configures certain process-wide ZooKeeper security-related settings, namely the ZooKeeper service name (default: `zookeeper`)
and the JAAS login context name (default: `Client`).

## Deployment Modes
Here is some information specific to each deployment mode.  

### Standalone Mode

Steps to run a secure Flink cluster in standalone/cluster mode:

1. Add security-related configuration options to the Flink configuration file (on all cluster nodes) (see [here]({{< ref "docs/deployment/config" >}}#auth-with-external-systems)).
2. Ensure that the keytab file exists at the path indicated by `security.kerberos.login.keytab` on all cluster nodes.
3. Deploy Flink cluster as normal.

### Native Kubernetes and YARN Mode

Steps to run a secure Flink cluster in native Kubernetes and YARN mode:

1. Add security-related configuration options to the Flink configuration file on the client (see [here]({{< ref "docs/deployment/config" >}}#auth-with-external-systems)).
2. Ensure that the keytab file exists at the path as indicated by `security.kerberos.login.keytab` on the client node.
3. Deploy Flink cluster as normal.

In YARN and native Kubernetes mode, the keytab is automatically copied from the client to the Flink containers.

To enable Kerberos authentication, the Kerberos configuration file is also required. This file can be either fetched from the cluster environment or uploaded by Flink. In the latter case, you need to configure the `security.kerberos.krb5-conf.path` to indicate the path of the Kerberos configuration file and Flink will copy this file to its containers/pods.

For more information, see <a href="https://github.com/apache/hadoop/blob/trunk/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-site/src/site/markdown/YarnApplicationSecurity.md">YARN security</a> documentation.

#### Using user credential cache (`kinit`)

Before we go on the usage some important things to know:
* Credential cache can be represented in many forms, the most common form is `FILE`. For further information please read [Kerberos ccache types](https://web.mit.edu/kerberos/krb5-1.12/doc/basic/ccache_def.html#ccache-types).
Ensure that the credential cache is available on all cluster nodes where Kerberos authentication is performed.
* Credential cache can be generated mainly with executing `kinit` command
* Important difference compared to keytab that keytab can be generated in a way that it never expires but credential cache
has an expiry date. Keeping the credential cache up-to-date is fully user responsibility.

It is possible to deploy a secure Flink cluster without a keytab, using only the credential cache.

Steps to run a secure Flink cluster using `kinit`:

1. Add security-related configuration options to the Flink configuration file on the client (see [here]({{< ref "docs/deployment/config" >}}#auth-with-external-systems)).
2. Login using the `kinit` command.
3. Optional: Make the credential cache available on all cluster nodes where Kerberos authentication is performed.
4. Deploy Flink cluster as normal.

## Further Details

### TGT Renewal
Each component that uses Kerberos is independently responsible for renewing the Kerberos ticket-granting-ticket (TGT).
All components renew the TGT automatically when keytab provided however it's the user responsibility when credential cache used.

## Using delegation tokens

In Flink 1.17 delegation token support added as an experimental feature. This is quite a heavyweight
topic so there is a general delegation token [information page]({{< ref "docs/deployment/security/security-delegation-token" >}}).

When talking to Hadoop-based services, Flink can obtain delegation tokens so that non-local
processes can authenticate. There is support for:
* HDFS and other Hadoop file systems
* HBase

When using a Hadoop filesystem (such HDFS or WebHDFS), Flink can obtain the relevant tokens for the
following directories:
* Hadoop default filesystem
* Filesystems configured in: `security.kerberos.access.hadoopFileSystems`
* YARN staging directory

An HBase token will be obtained if HBase is in the application’s classpath, and the HBase
configuration has Kerberos authentication turned (`hbase.security.authentication=kerberos`).

Flink also supports custom delegation token providers using the Java Services mechanism
(see `java.util.ServiceLoader`). Implementations of `org.apache.flink.runtime.security.token.DelegationTokenProvider`
can be made available to Flink by listing their names in the corresponding file in the jar’s `META-INF/services` directory.

{{< top >}}
