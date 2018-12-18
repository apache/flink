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

package org.apache.flink.mesos.runtime.clusterframework

import org.apache.flink.runtime.clusterframework.types.ResourceID
import org.apache.flink.runtime.highavailability.HighAvailabilityServices
import org.apache.flink.runtime.io.disk.iomanager.IOManager
import org.apache.flink.runtime.io.network.NetworkEnvironment
import org.apache.flink.runtime.memory.MemoryManager
import org.apache.flink.runtime.metrics.groups.TaskManagerMetricGroup
import org.apache.flink.runtime.state.TaskExecutorLocalStateStoresManager
import org.apache.flink.runtime.taskexecutor.TaskManagerConfiguration
import org.apache.flink.runtime.taskmanager.{TaskManager, TaskManagerLocation}

/** An extension of the TaskManager that listens for additional Mesos-related
  * messages.
  */
class MesosTaskManager(
    config: TaskManagerConfiguration,
    resourceID: ResourceID,
    taskManagerLocation: TaskManagerLocation,
    memoryManager: MemoryManager,
    ioManager: IOManager,
    network: NetworkEnvironment,
    taskManagerLocalStateStoresManager: TaskExecutorLocalStateStoresManager,
    numberOfSlots: Int,
    highAvailabilityServices: HighAvailabilityServices,
    taskManagerMetricGroup : TaskManagerMetricGroup)
  extends TaskManager(
    config,
    resourceID,
    taskManagerLocation,
    memoryManager,
    ioManager,
    network,
    taskManagerLocalStateStoresManager,
    numberOfSlots,
    highAvailabilityServices,
    taskManagerMetricGroup) {

  override def handleMessage: Receive = {
    super.handleMessage
  }
}

object MesosTaskManager {
  /** Entry point (main method) to run the TaskManager on Mesos.
    *
    * @param args The command line arguments.
    */
  def main(args: Array[String]): Unit = {
    MesosTaskManagerRunner.runTaskManager(args, classOf[MesosTaskManager])
  }

}
