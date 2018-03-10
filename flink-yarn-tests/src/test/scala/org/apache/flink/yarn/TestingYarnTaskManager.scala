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

import org.apache.flink.runtime.clusterframework.types.ResourceID
import org.apache.flink.runtime.highavailability.HighAvailabilityServices
import org.apache.flink.runtime.io.disk.iomanager.IOManager
import org.apache.flink.runtime.io.network.NetworkEnvironment
import org.apache.flink.runtime.memory.MemoryManager
import org.apache.flink.runtime.metrics.groups.TaskManagerMetricGroup
import org.apache.flink.runtime.security.SecurityUtils
import org.apache.flink.runtime.state.TaskExecutorLocalStateStoresManager
import org.apache.flink.runtime.taskexecutor.TaskManagerConfiguration
import org.apache.flink.runtime.taskmanager.TaskManagerLocation
import org.apache.flink.runtime.testingUtils.TestingTaskManagerLike

/** [[YarnTaskManager]] implementation which mixes in the [[TestingTaskManagerLike]] mixin.
  *
  * This actor class is used for testing purposes on Yarn. Here we use an explicit class definition
  * instead of an anonymous class with the respective mixin to obtain a more readable logger name.
  *
  * @param config Configuration object for the actor
  * @param resourceID The Yarn container id
  * @param connectionInfo Connection information of this actor
  * @param memoryManager MemoryManager which is responsible for Flink's managed memory allocation
  * @param ioManager IOManager responsible for I/O
  * @param network NetworkEnvironment for this actor
  * @param taskManagerLocalStateStoresManager Task manager state store manager for this actor
  * @param numberOfSlots Number of slots for this TaskManager
  * @param highAvailabilityServices [[HighAvailabilityServices]] to create a leader retrieval
  *                                service for retrieving the leading JobManager
  */
class TestingYarnTaskManager(
    config: TaskManagerConfiguration,
    resourceID: ResourceID,
    connectionInfo: TaskManagerLocation,
    memoryManager: MemoryManager,
    ioManager: IOManager,
    network: NetworkEnvironment,
    taskManagerLocalStateStoresManager: TaskExecutorLocalStateStoresManager,
    numberOfSlots: Int,
    highAvailabilityServices: HighAvailabilityServices,
    taskManagerMetricGroup : TaskManagerMetricGroup)
  extends YarnTaskManager(
    config,
    resourceID,
    connectionInfo,
    memoryManager,
    ioManager,
    network,
    taskManagerLocalStateStoresManager,
    numberOfSlots,
    highAvailabilityServices,
    taskManagerMetricGroup)
  with TestingTaskManagerLike {

  object YarnTaskManager {

    /** Entry point (main method) to run the TaskManager on YARN.
      *
      * @param args The command line arguments.
      */
    def main(args: Array[String]): Unit = {
      val tmRunner = YarnTaskManagerRunnerFactory.create(
        args, classOf[TestingYarnTaskManager], System.getenv())

      try {
        SecurityUtils.getInstalledContext.runSecured(tmRunner)
      } catch {
        case e: Exception =>
          throw new RuntimeException(e)
      }
    }

  }
}

