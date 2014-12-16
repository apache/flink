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
import org.apache.flink.api.common.io.FileOutputFormat
import org.apache.flink.configuration.{ConfigConstants, Configuration}
import org.apache.flink.runtime.akka.AkkaUtils
import org.apache.flink.runtime.client.JobClient
import org.apache.flink.runtime.jobmanager.JobManager
import org.apache.flink.runtime.taskmanager.TaskManager
import org.apache.flink.runtime.util.EnvironmentInformation
import org.slf4j.LoggerFactory

class LocalFlinkMiniCluster(userConfiguration: Configuration, singleActorSystem: Boolean = true)
  extends FlinkMiniCluster(userConfiguration, singleActorSystem){
  import LocalFlinkMiniCluster._

  val jobClientActorSystem = if(singleActorSystem){
    jobManagerActorSystem
  }else{
    AkkaUtils.createActorSystem()
  }

  var jobClient: Option[ActorRef] = None

  override def generateConfiguration(userConfiguration: Configuration): Configuration = {
    val config = getDefaultConfig

    config.addAll(userConfiguration)

    setMemory(config)

    initializeIOFormatClasses(config)

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

    val localExecution = if(numTaskManagers == 1){
      true
    }else{
      false
    }

    TaskManager.startActorWithConfiguration(HOSTNAME, config, localExecution)(system)
  }

  def getJobClient(): ActorRef ={
    jobClient match {
      case Some(jc) => jc
      case None =>
        val config = new Configuration()

        config.setString(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, HOSTNAME)
        config.setInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY, getJobManagerRPCPort)

        if(singleActorSystem){
          config.setString(ConfigConstants.JOB_MANAGER_AKKA_URL, "akka://flink/user/jobmanager")
        }

        val jc = JobClient.startActorWithConfiguration(config)(jobClientActorSystem)
        jobClient = Some(jc)
        jc
    }
  }

  def getJobClientActorSystem: ActorSystem = jobClientActorSystem

  def getJobManagerRPCPort: Int = {
    configuration.getInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY, -1)
  }

  override def shutdown(): Unit = {
    super.shutdown()

    if(!singleActorSystem) {
      jobClientActorSystem.shutdown()
    }
  }

  override def awaitTermination(): Unit = {
    if(!singleActorSystem) {
      jobClientActorSystem.awaitTermination()
    }
    super.awaitTermination()
  }

  def initializeIOFormatClasses(configuration: Configuration): Unit = {
    try{
      val om = classOf[FileOutputFormat[_]].getDeclaredMethod("initDefaultsFromConfiguration",
        classOf[Configuration])
      om.setAccessible(true)
      om.invoke(null, configuration)
    }catch {
      case e: Exception =>
        LOG.error("Cannot (re) initialize the globally loaded defaults. Some classes might not " +
          "follow the specified default behaviour.")
    }
  }

  def setMemory(config: Configuration): Unit = {
    var memorySize: Long = EnvironmentInformation.getSizeOfFreeHeapMemoryWithDefrag
    val bufferMem: Long = ConfigConstants.DEFAULT_TASK_MANAGER_NETWORK_NUM_BUFFERS *
      ConfigConstants.DEFAULT_TASK_MANAGER_NETWORK_BUFFER_SIZE
    val numTaskManager = config.getInteger(ConfigConstants
      .LOCAL_INSTANCE_MANAGER_NUMBER_TASK_MANAGER, 1)
    val taskManagerNumSlots: Int = ConfigConstants.DEFAULT_TASK_MANAGER_NUM_TASK_SLOTS
    memorySize = memorySize - (bufferMem * numTaskManager)
    memorySize = (memorySize * ConfigConstants.DEFAULT_MEMORY_MANAGER_MEMORY_FRACTION).toLong
    memorySize >>>= 20
    memorySize /= numTaskManager
    config.setLong(ConfigConstants.TASK_MANAGER_MEMORY_SIZE_KEY, memorySize)
  }

  def getDefaultConfig: Configuration = {
    val config: Configuration = new Configuration
    config.setString(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, HOSTNAME)
    config.setInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY, ConfigConstants
      .DEFAULT_JOB_MANAGER_IPC_PORT)
    config.setInteger(ConfigConstants.TASK_MANAGER_IPC_PORT_KEY, ConfigConstants
      .DEFAULT_TASK_MANAGER_IPC_PORT)
    config.setInteger(ConfigConstants.TASK_MANAGER_DATA_PORT_KEY, ConfigConstants
      .DEFAULT_TASK_MANAGER_DATA_PORT)
    config.setBoolean(ConfigConstants.TASK_MANAGER_MEMORY_LAZY_ALLOCATION_KEY, ConfigConstants
      .DEFAULT_TASK_MANAGER_MEMORY_LAZY_ALLOCATION)
    config.setInteger(ConfigConstants.JOBCLIENT_POLLING_INTERVAL_KEY, ConfigConstants
      .DEFAULT_JOBCLIENT_POLLING_INTERVAL)
    config.setBoolean(ConfigConstants.FILESYSTEM_DEFAULT_OVERWRITE_KEY, ConfigConstants
      .DEFAULT_FILESYSTEM_OVERWRITE)
    config.setBoolean(ConfigConstants.FILESYSTEM_OUTPUT_ALWAYS_CREATE_DIRECTORY_KEY,
      ConfigConstants.DEFAULT_FILESYSTEM_ALWAYS_CREATE_DIRECTORY)
    config.setLong(ConfigConstants.TASK_MANAGER_MEMORY_SIZE_KEY, -1)
    config.setInteger(ConfigConstants.LOCAL_INSTANCE_MANAGER_NUMBER_TASK_MANAGER, 1)
    config.setInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, ConfigConstants
      .DEFAULT_TASK_MANAGER_NUM_TASK_SLOTS)
    config
  }
}

object LocalFlinkMiniCluster{
  val LOG = LoggerFactory.getLogger(classOf[LocalFlinkMiniCluster])
}
