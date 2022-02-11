---
title: "Overview"
weight: 1
type: docs
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

# Security Overview

Frameworks that process data are sensitive components; you must use authentication and encryption to 
secure your data and data sources. Apache Flink supports authentication with [Kerberos](https://web.mit.edu/kerberos/) 
and can be configured to encrypt all network communication with [SSL](https://www.ssl.com/faqs/faq-what-is-ssl/).

When we talk about security for Flink, we generally make a distinction between securing the internal 
communication within the Flink cluster (i.e. between the Task Managers, between the Task Managers and 
the Flink Master) and securing the external communication between the cluster and the outside world.

Internally, netty is used for the TCP connections used for data exchange among the task managers, 
and Akka is used for RPC between the Flink master and the task managers.

Externally, HTTP is used for pretty much everything, except that some external services used as sources 
or sinks may use some other network protocol.

## What is supported?

Security enhancement features by the Flink community make it easy to access secured data, protect 
associated credentials, and increase overall security in a Flink cluster. The following security 
measures are currently supported:

- Authentication of connections between Flink processes 
- Encryption of data transferred between Flink processes using SSL (Note that there is a performance 
  degradation when SSL is enabled, the magnitude of which depends on the CPU type and the JVM implementation.)
- Authorization of read / write operations by clients 
- Authorization is pluggable and integration with external authorization services is supported

It is worth noting that security is optional because the overall philosophy in Flink is to have defaults 
that work out-of-the-box, without any configuration. Thus, non-secured clusters are supported, as well 
as a mix of authenticated, unauthenticated, encrypted and unencrypted clients. 

## How to use this section 

The guides in this section explain how to configure and use these security features. 

We are going to look at the following topics:

- Using SSL to secure internal communication
- Using SSL to secure external communication
- Using Kerberos for authentication

We’ll describe the new features, including Kerberos-based access to HDFS and Kafka, transport security (TLS), 
and service-level authorization which protects your Flink cluster from unauthorized access.
