/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.yarn

import org.apache.flink.runtime.instance.InstanceConnectionInfo
import org.apache.flink.runtime.io.disk.iomanager.IOManager
import org.apache.flink.runtime.io.network.NetworkEnvironment
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService
import org.apache.flink.runtime.memory.MemoryManager
import org.apache.flink.runtime.taskmanager.TaskManagerConfiguration
import org.apache.flink.runtime.testingUtils.TestingTaskManagerLike

/** [[YarnTaskManager]] implementation which mixes in the [[TestingTaskManagerLike]] mixin.
  *
  * This actor class is used for testing purposes on Yarn. Here we use an explicit class definition
  * instead of an anonymous class with the respective mixin to obtain a more readable logger name.
  *
  * @param config Configuration object for the actor
  * @param connectionInfo Connection information of this actor
  * @param memoryManager MemoryManager which is responsibel for Flink's managed memory allocation
  * @param ioManager IOManager responsible for I/O
  * @param network NetworkEnvironment for this actor
  * @param numberOfSlots Number of slots for this TaskManager
  * @param leaderRetrievalService [[LeaderRetrievalService]] to retrieve the current leading
  *                              JobManager
  */
class TestingYarnTaskManager(
    config: TaskManagerConfiguration,
    connectionInfo: InstanceConnectionInfo,
    memoryManager: MemoryManager,
    ioManager: IOManager,
    network: NetworkEnvironment,
    numberOfSlots: Int,
    leaderRetrievalService: LeaderRetrievalService)
  extends YarnTaskManager(
    config,
    connectionInfo,
    memoryManager,
    ioManager,
    network,
    numberOfSlots,
    leaderRetrievalService)
  with TestingTaskManagerLike {}
