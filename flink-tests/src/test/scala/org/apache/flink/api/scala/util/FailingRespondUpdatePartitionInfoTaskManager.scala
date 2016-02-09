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

package org.apache.flink.api.scala.util

import org.apache.flink.runtime.instance.InstanceConnectionInfo
import org.apache.flink.runtime.io.disk.iomanager.IOManager
import org.apache.flink.runtime.io.network.NetworkEnvironment
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService
import org.apache.flink.runtime.memory.MemoryManager
import org.apache.flink.runtime.messages.TaskMessages.UpdatePartitionInfo
import org.apache.flink.runtime.taskmanager.{TaskManagerConfiguration, TaskManager}

/**
  * Special TaskManager implementation used for the
  * [[org.apache.flink.test.recovery.TimeoutHandlingTest]]. This TM will skip the third
  * [[UpdatePartitionInfo]] message which is received. This skipping will trigger a timeout on the
  * [[org.apache.flink.runtime.jobmanager.JobManager]] side.
  *
  * @param config Configuration
  * @param connectionInfo ConnectionInfo of this TM
  * @param memoryManager MemoryManager to be used by the TM
  * @param ioManager IOManager to be used by the TM
  * @param network Network environment
  * @param numberOfSlots Number of slots to create
  * @param leaderRetrievalService Service to retrieve the current leader
  */
class FailingRespondUpdatePartitionInfoTaskManager(
  config: TaskManagerConfiguration,
  connectionInfo: InstanceConnectionInfo,
  memoryManager: MemoryManager,
  ioManager: IOManager,
  network: NetworkEnvironment,
  numberOfSlots: Int,
  leaderRetrievalService: LeaderRetrievalService)
  extends TaskManager(
    config,
    connectionInfo,
    memoryManager,
    ioManager,
    network,
    numberOfSlots,
    leaderRetrievalService) {

  var partitionInfoCounter = 0

  override def handleMessage: Receive = {
    handlePartitionInfo orElse super.handleMessage
  }

  def handlePartitionInfo: Receive = {
    case x: UpdatePartitionInfo =>
      partitionInfoCounter += 1

      // skip the 3rd UpdatePartitionInfo message
      if (partitionInfoCounter != 3) {
        super.handleMessage.apply(x)
      }
  }
}
