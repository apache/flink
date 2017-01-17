---
title: "Debugging Classloading"
nav-parent_id: monitoring
nav-pos: 8
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

* ToC
{:toc}

## Overview of Classloading in Flink

  - What is in the Application Classloader for different deployment techs
  - What is in the user code classloader

  - Access to the user code classloader for applications

## Classpath Setups

  - Finding classpaths in logs
  - Moving libraries and/or user code to the Application Classpath 

## Unloading of Dynamically Loaded Classes

  - Checkpoint statistics overview
  - Interpret time until checkpoints
  - Synchronous vs. asynchronous checkpoint time

