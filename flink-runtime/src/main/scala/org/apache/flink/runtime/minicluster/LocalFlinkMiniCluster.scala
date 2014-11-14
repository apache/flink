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

package org.apache.flink.runtime.minicluster

import akka.actor.{ActorRef, ActorSystem}
import org.apache.flink.configuration.{ConfigConstants, Configuration}
import org.apache.flink.runtime.akka.AkkaUtils
import org.apache.flink.runtime.client.JobClient
import org.apache.flink.runtime.jobmanager.JobManager
import org.apache.flink.runtime.taskmanager.TaskManager
import org.slf4j.LoggerFactory

class LocalFlinkMiniCluster(userConfiguration: Configuration) extends
FlinkMiniCluster(userConfiguration){

  override def generateConfiguration(userConfiguration: Configuration): Configuration = {
    val forNumberString = System.getProperty("forkNumber")

    val forkNumber = try {
      Integer.parseInt(forNumberString)
    }catch{
      case e: NumberFormatException => -1
    }

    val config = FlinkMiniCluster.getDefaultConfig

    config.addAll(userConfiguration)

    if(forkNumber != -1){
      val jobManagerRPC = 1024 + forkNumber*300
      val taskManagerRPC = 1024 + forkNumber*300 + 100
      val taskManagerData = 1024 + forkNumber*300 + 200

      config.setInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY, jobManagerRPC)
      config.setInteger(ConfigConstants.TASK_MANAGER_IPC_PORT_KEY, taskManagerRPC)
      config.setInteger(ConfigConstants.TASK_MANAGER_DATA_PORT_KEY, taskManagerData)

    }

    FlinkMiniCluster.initializeIOFormatClasses(config)

    config
  }

  override def startJobManager(implicit system: ActorSystem):
  ActorRef = {
    val config = configuration.clone()
    JobManager.startActor(config)
  }

  override def startTaskManager(index: Int)(implicit system: ActorSystem): ActorRef = {
    val config = configuration.clone()

    val rpcPort = config.getInteger(ConfigConstants.TASK_MANAGER_IPC_PORT_KEY, ConfigConstants
      .DEFAULT_TASK_MANAGER_IPC_PORT)
    val dataPort = config.getInteger(ConfigConstants.TASK_MANAGER_DATA_PORT_KEY, ConfigConstants
      .DEFAULT_TASK_MANAGER_DATA_PORT)

    if(rpcPort > 0){
      config.setInteger(ConfigConstants.TASK_MANAGER_IPC_PORT_KEY, rpcPort + index)
    }

    if(dataPort > 0){
      config.setInteger(ConfigConstants.TASK_MANAGER_DATA_PORT_KEY, dataPort + index)
    }

    TaskManager.startActorWithConfiguration(FlinkMiniCluster.HOSTNAME, config, false)(system)
  }
}

object LocalFlinkMiniCluster{
  val LOG = LoggerFactory.getLogger(classOf[LocalFlinkMiniCluster])
}
