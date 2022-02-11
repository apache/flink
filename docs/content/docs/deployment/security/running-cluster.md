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
