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
and can be configured to encrypt all network communication with [SSL/TLS](https://www.ssl.com/faqs/faq-what-is-ssl/).

{{< hint info >}}
SSL and TLS are both encryption protocols for the transport layer, used for encrypting data between
the client and server. The TLS protocol is the successor to SSL. Flink enforces TLS by default, but 
you can also switch back to SSL.
{{< /hint >}}

## What needs to be secured in Flink?

When we talk about security for Flink, we generally make a distinction between securing the internal 
communication within the Flink cluster (i.e. between the TaskManagers, between the TaskManagers and 
the JobManager) and securing the external communication between the cluster and the outside world.

Internally, [Netty](https://netty.io) is used for the TCP connections used for data exchange among 
the TaskManagers, and [Akka](https://akka.io) is used for RPC between the JobManager and the TaskManagers.

Externally, HTTP is used for pretty much everything, except that some external services used as sources 
or sinks may use some other network protocol.

## What is supported by Flink?

Security enhancement features by the Flink community make it easy to access secured data, protect 
associated credentials, and increase overall security in a Flink cluster. The following security 
measures are currently supported:

- Authentication of connections between Flink processes 
- Encryption of data transferred between Flink processes using SSL (Note that there is a performance 
  degradation when SSL is enabled, the magnitude of which depends on the CPU type, the JVM implementation, 
  and the key size.)
- Authorization of read / write operations by clients 
- Authorization is pluggable and integration with external authorization services is supported

It is worth noting that security needs to be manually configured because the overall philosophy in 
Flink is to have defaults that work out-of-the-box, without any configuration. Thus, non-secured 
clusters are supported, as well as a mix of authenticated, unauthenticated, encrypted and unencrypted 
clients. 

## Where are its flaws? How should they be addressed?

Web UI unsecure
not secure by default

## What needs to be solved outside of Flink?

connectors

## How to use this section 

The guides in this section explain how to configure and use these security features and will cover 
the following topics:

- Using Kerberos for authentication
- Using SSL to secure internal communication
- Using SSL to secure external communication

You will learn about the new features, including Kerberos-based access to HDFS and Kafka, transport 
security (SSL/TLS), and service-level authorization which protects your Flink cluster from unauthorized 
access.
