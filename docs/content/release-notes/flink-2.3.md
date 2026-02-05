---
title: "Release Notes - Flink 2.3"
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

# Release notes - Flink 2.3

These release notes discuss important aspects, such as configuration, behavior or dependencies,
that changed between Flink 2.2 and Flink 2.3. Please read these notes carefully if you are
planning to upgrade your Flink version to 2.3.


### Core

#### Set security.ssl.algorithms default value to modern cipher suite

### [FLINK-39022](https://issues.apache.org/jira/browse/FLINK-39022)

A JDK update (affecting JDK 11.0.30+, 17.0.18+, 21.0.10+, and 24+) disabled `TLS_RSA_*` cipher suites.
This was done to support forward-secrecy (RFC 9325) and comply with the IETF Draft on *Deprecating Obsolete Key Exchange Methods in TLS*.

To support these and future JDK versions, the default value for the Flink configuration option `security.ssl.algorithms` has been changed to a modern, widely available cipher suite:

`TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384`

This default provides strong security and wide compatibility. You can customize the cipher suites using the `security.ssl.algorithms` configuration option if your environment has different requirements. 
If these cipher suites are not supported on your setup, you will see that Flink processes will not be able to connect to each other.
