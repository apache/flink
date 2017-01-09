---
title: "Debugging and Tuning Checkpoints and Large State"
nav-parent_id: monitoring
nav-pos: 5
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

This page gives a guide how to improve and tune applications that use large state.

* ToC
{:toc}

## Monitoring State and Checkpoints

  - Checkpoint statistics overview
  - Interpret time until checkpoints
  - Synchronous vs. asynchronous checkpoint time

## Tuning Checkpointing

  - Checkpoint interval
  - Getting work done between checkpoints (min time between checkpoints)

## Tuning Network Buffers

  - getting a good number of buffers to use
  - monitoring if too many buffers cause too much inflight data

## Make checkpointing asynchronous where possible

  - large state should be on keyed state, not operator state, because keyed state is managed, operator state not (subject to change in future versions)

  - asynchronous snapshots preferrable. long synchronous snapshot times can cause problems on large state and complex topogies. move to RocksDB for that

## Tuning RocksDB

  - Predefined options
  - Custom Options

## Capacity planning

  - Normal operation should not be constantly back pressured (link to back pressure monitor)
  - Allow for some excess capacity to support catch-up in case of failures and checkpoint alignment skew (due to data skew or bad nodes)


