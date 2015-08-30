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
import org.apache.flink.runtime.taskmanager.{NetworkEnvironmentConfiguration, TaskManagerConfiguration, TaskManager}
import org.apache.flink.yarn.Messages.StopYarnSession

/** An extension of the TaskManager that listens for additional YARN related
  * messages.
  */
class YarnTaskManager(
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

  override def handleMessage: Receive = {
    handleYarnMessages orElse super.handleMessage
  }

  def handleYarnMessages: Receive = {
    case StopYarnSession(status, diagnostics) =>
      log.info(s"Stopping YARN TaskManager with final application status $status " +
        s"and diagnostics: $diagnostics")
      context.system.shutdown()
  }
}
