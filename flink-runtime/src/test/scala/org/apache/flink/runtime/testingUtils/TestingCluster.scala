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

package org.apache.flink.runtime.testingUtils

import akka.actor.{ActorSystem, Props}
import org.apache.flink.configuration.{ConfigConstants, Configuration}
import org.apache.flink.runtime.jobmanager.JobManager
import org.apache.flink.runtime.minicluster.FlinkMiniCluster
import org.apache.flink.runtime.net.NetUtils
import org.apache.flink.runtime.taskmanager.TaskManager

class TestingCluster(userConfiguration: Configuration) extends FlinkMiniCluster(userConfiguration,
  true) {

  override def generateConfiguration(userConfig: Configuration): Configuration = {
    val cfg = new Configuration()
    cfg.setString(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, "localhost")
    cfg.setInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY, NetUtils.getAvailablePort)
    cfg.setInteger(ConfigConstants.TASK_MANAGER_MEMORY_SIZE_KEY, 10)

    cfg.addAll(userConfig)
    cfg
  }

  override def startJobManager(implicit system: ActorSystem) = {
    system.actorOf(Props(new JobManager(configuration) with TestingJobManager),
      JobManager.JOB_MANAGER_NAME)
  }

  override def startTaskManager(index: Int)(implicit system: ActorSystem) = {
    val (connectionInfo, jobManagerURL, taskManagerConfig, networkConnectionConfig) =
      TaskManager.parseConfiguration(HOSTNAME, configuration, true)

    system.actorOf(Props(new TaskManager(connectionInfo, jobManagerURL, taskManagerConfig,
      networkConnectionConfig) with TestingTaskManager), TaskManager.TASK_MANAGER_NAME + index)
  }
}
