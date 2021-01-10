---
title:  "Kerberos Authentication Setup and Configuration"
nav-parent_id: security
nav-pos: 2
nav-title: Kerberos
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

This document briefly describes how Flink security works in the context of various deployment mechanisms (Standalone, native Kubernetes, YARN, or Mesos),
filesystems, connectors, and state backends.

## Objective
The primary goals of the Flink Kerberos security infrastructure are:

1. to enable secure data access for jobs within a cluster via connectors (e.g. Kafka)
2. to authenticate to ZooKeeper (if configured to use SASL)
3. to authenticate to Hadoop components (e.g. HDFS, HBase) 

In a production deployment scenario, streaming jobs are understood to run for long periods of time (days/weeks/months) and be able to authenticate to secure 
data sources throughout the life of the job.  Kerberos keytabs do not expire in that timeframe, unlike a Hadoop delegation token
or ticket cache entry.

The current implementation supports running Flink clusters (JobManager / TaskManager / jobs) with either a configured keytab credential
or with Hadoop delegation tokens.   Keep in mind that all jobs share the credential configured for a given cluster.   To use a different keytab
for a certain job, simply launch a separate Flink cluster with a different configuration.   Numerous Flink clusters may run side-by-side in a Kubernetes, YARN
or Mesos environment.

## How Flink Security works
In concept, a Flink program may use first- or third-party connectors (Kafka, HDFS, Cassandra, Flume, Kinesis etc.) necessitating arbitrary authentication methods (Kerberos, SSL/TLS, username/password, etc.).  While satisfying the security requirements for all connectors is an ongoing effort,
Flink provides first-class support for Kerberos authentication only.  The following services and connectors are supported for Kerberos authentication:

- Kafka (0.9+)
- HDFS
- HBase
- ZooKeeper

Note that it is possible to enable the use of Kerberos independently for each service or connector.  For example, the user may enable 
Hadoop security without necessitating the use of Kerberos for ZooKeeper, or vice versa.    The shared element is the configuration of 
Kerberos credentials, which is then explicitly used by each component.

The internal architecture is based on security modules (implementing `org.apache.flink.runtime.security.modules.SecurityModule`) which
are installed at startup.  The following sections describes each security module.

### Hadoop Security Module
This module uses the Hadoop `UserGroupInformation` (UGI) class to establish a process-wide *login user* context.   The login user is
then used for all interactions with Hadoop, including HDFS, HBase, and YARN.

If Hadoop security is enabled (in `core-site.xml`), the login user will have whatever Kerberos credential is configured.  Otherwise,
the login user conveys only the user identity of the OS account that launched the cluster.

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

1. Add security-related configuration options to the Flink configuration file (on all cluster nodes) (see [here]({% link deployment/config.zh.md %}#auth-with-external-systems)).
2. Ensure that the keytab file exists at the path indicated by `security.kerberos.login.keytab` on all cluster nodes.
3. Deploy Flink cluster as normal.

### Native Kubernetes, YARN and Mesos Mode

Steps to run a secure Flink cluster in native Kubernetes, YARN and Mesos mode:

1. Add security-related configuration options to the Flink configuration file on the client (see [here]({% link deployment/config.zh.md %}#auth-with-external-systems)).
2. Ensure that the keytab file exists at the path as indicated by `security.kerberos.login.keytab` on the client node.
3. Deploy Flink cluster as normal.

In YARN, Mesos and native Kubernetes mode, the keytab is automatically copied from the client to the Flink containers.

To enable Kerberos authentication, the Kerberos configuration file is also required. This file can be either fetched from the cluster environment or uploaded by Flink. In the latter case, you need to configure the `security.kerberos.krb5-conf.path` to indicate the path of the Kerberos configuration file and Flink will copy this file to its containers/pods.

Note that the property `java.security.krb5.conf`, which was available in Mesos mode previously, has been deprecated. Despite it's still taking effect for backward compatibility, please be aware this property can be removed in future releases.

For more information, see <a href="https://github.com/apache/hadoop/blob/trunk/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-site/src/site/markdown/YarnApplicationSecurity.md">YARN security</a> documentation.

#### Using `kinit` (YARN only)

In YARN mode, it is possible to deploy a secure Flink cluster without a keytab, using only the ticket cache (as managed by `kinit`).
This avoids the complexity of generating a keytab and avoids entrusting the cluster manager with it.  In this scenario, the Flink CLI acquires Hadoop delegation tokens (for HDFS and for HBase).
The main drawback is that the cluster is necessarily short-lived since the generated delegation tokens will expire (typically within a week).

Steps to run a secure Flink cluster using `kinit`:

1. Add security-related configuration options to the Flink configuration file on the client (see [here]({% link deployment/config.zh.md %}#auth-with-external-systems)).
2. Login using the `kinit` command.
3. Deploy Flink cluster as normal.

## Further Details

### Ticket Renewal
Each component that uses Kerberos is independently responsible for renewing the Kerberos ticket-granting-ticket (TGT).
Hadoop, ZooKeeper, and Kafka all renew the TGT automatically when provided a keytab.  In the delegation token scenario,
YARN itself renews the token (up to its maximum lifespan).

{% top %}
