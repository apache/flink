---
title: "Customizable Features for Platform Users"
nav-title: platform
nav-parent_id: advanced
nav-pos: 3
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
Flink provides a set of customizable features for users to extend from the default behavior through the plugin framework.

## Customize Failure Listener
Flink provides a pluggable failure listener interface for users to register multiple instances, which are called each 
time an exception that is reported at runtime. The default failure listener only records the failure count and emit the metric
"numJobFailure" for the job. The purpose of these listeners is to build metrics based on the exceptions, make calls to external
systems or classify the exceptions otherwise. For example, it can distinguish whether it is a Flink runtime error or an 
application user logic error. With accurate metrics, you may have a better idea about the platform level metrics, 
for example failures due to network, platform reliability, etc.


### Implement a plugin for your custom failure listener

To implement a plugin for your custom resource type, you need to:

  - Add your own FailureListener by implementing the `org.apache.flink.core.failurelistener.FailureListener` interface.
  
  - Add your own FailureListenerFactory by implementing the `org.apache.flink.core.failurelistener.FailureListenerFactory` interface.

  - Add a service entry. Create a file `META-INF/services/org.apache.flink.core.failurelistener.FailureListenerFactory`
  which contains the class name of your failure listener factory class (see the [Java Service Loader](https://docs.oracle.com/javase/8/docs/api/java/util/ServiceLoader.html) docs for more details).


Then, create a jar which includes your `FailureListener`, `FailureListenerFactory`, `META-INF/services/` and all the external dependencies.
Make a directory in `plugins/` of your Flink distribution with an arbitrary name, e.g. "failure-listener", and put the jar into this directory.
See [Flink Plugin]({% link deployment/filesystems/plugins.md %}) for more details.

As a plugin will be loaded in different classloader, log4j is not able to initialized correctly in your plugin. In this case,
you should add the config below in `flink-conf.yaml`:
- `plugin.classloader.parent-first-patterns.additional: org.slf4j`

