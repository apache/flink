---
title: Frequently Asked Questions
weight: 5
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

# Frequently Asked Questions

### During a security analysis of Flink, I noticed that Flink allows for remote code execution. Is this an issue?

Apache Flink is a framework for executing user-supplied code in clusters and this is by design. This is
why we have had to reject numerous remote code execution vulnerability reports. 

By design, users can submit code to Flink processes, which will be executed unconditionally and without 
any attempts to limit what code can run. Starting other processes, establishing network connections, 
or accessing and modifying local files is possible.

**We strongly discourage users to expose Flink processes to the public Internet**. Within company 
networks or "cloud" accounts, you should restrict access to a Flink cluster via appropriate means.

### I found a vulnerability in Flink. How do I report it?

We appreciate any reports that help to improve the security of Flink!

Vulnerability reports are accepted through the [Apache Security Team](http://www.apache.org/security/), 
via [security@apache.org](mailto:security@apache.org).

If you want to discuss a potential security issue privately with the Flink PMC, you can reach us via 
[private@flink.apache.org](mailto:private@flink.apache.org).
