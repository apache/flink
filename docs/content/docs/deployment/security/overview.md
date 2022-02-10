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

In release X, the Flink community added a number of features that increases security in a Flink cluster. 
The following security measures are currently supported:

- Authentication of connections Flink processes 
- Encryption of data transferred between Flink processes using SSL (Note that there is a performance 
  degradation when SSL is enabled, the magnitude of which depends on the CPU type and the JVM implementation.)
- Authorization of read / write operations by clients 
- Authorization is pluggable and integration with external authorization services is supported

It is worth noting that security is optional - non-secured clusters are supported, as well as a mix 
of authenticated, unauthenticated, encrypted and unencrypted clients. The guides in this section explain 
how to configure and use these security features.


Recent security enhancements to Flink make it easy to access secure data and to protect the associated 
credentials. We’ll describe the new features, including Kerberos-based access to HDFS and Kafka, transport security (TLS), 
and service-level authorization which protects your Flink cluster from unauthorized access.
