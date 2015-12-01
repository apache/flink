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
import org.apache.flink.runtime.io.network.netty.NettyConfig
import org.apache.flink.runtime.jobmanager.{MemoryArchivist, JobManager}
import org.apache.flink.runtime.taskmanager.TaskManager
import org.apache.flink.runtime.util.EnvironmentInformation

/**
 * Local Flink mini cluster which executes all [[TaskManager]]s and the [[JobManager]] in the same
 * JVM. It extends the [[FlinkMiniCluster]] by having convenience functions to setup Flink's
 * configuration and implementations to create [[JobManager]] and [[TaskManager]].
 *
 * @param userConfiguration Configuration object with the user provided configuration values
 * @param singleActorSystem true if all actors (JobManager and TaskManager) shall be run in the same
 *                          [[ActorSystem]], otherwise false
 */
class LocalFlinkMiniCluster(
    userConfiguration: Configuration,
    singleActorSystem: Boolean) extends FlinkMiniCluster(userConfiguration, singleActorSystem) {

  def this(userConfiguration: Configuration) = this(userConfiguration, true)

  // --------------------------------------------------------------------------

  override def generateConfiguration(userConfiguration: Configuration): Configuration = {
    val config = getDefaultConfig

    config.addAll(userConfiguration)
    setMemory(config)
    initializeIOFormatClasses(config)

    config
  }

  override def startJobManager(index: Int, system: ActorSystem): ActorRef = {
    val config = configuration.clone()

    val jobManagerName = getJobManagerName(index)
    val archiveName = getArchiveName(index)

    val jobManagerPort = config.getInteger(
      ConfigConstants.JOB_MANAGER_IPC_PORT_KEY,
      ConfigConstants.DEFAULT_JOB_MANAGER_IPC_PORT)

    if(jobManagerPort > 0) {
      config.setInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY, jobManagerPort + index)
    }

    val (jobManager, _) = JobManager.startJobManagerActors(
      config,
      system,
      Some(jobManagerName),
      Some(archiveName),
      classOf[JobManager],
      classOf[MemoryArchivist])

    jobManager
  }

  override def startTaskManager(index: Int, system: ActorSystem): ActorRef = {
    val config = configuration.clone()

    val rpcPort = config.getInteger(
      ConfigConstants.TASK_MANAGER_IPC_PORT_KEY,
      ConfigConstants.DEFAULT_TASK_MANAGER_IPC_PORT)

    val dataPort = config.getInteger(
      ConfigConstants.TASK_MANAGER_DATA_PORT_KEY,
      ConfigConstants.DEFAULT_TASK_MANAGER_DATA_PORT)

    if (rpcPort > 0) {
      config.setInteger(ConfigConstants.TASK_MANAGER_IPC_PORT_KEY, rpcPort + index)
    }
    if (dataPort > 0) {
      config.setInteger(ConfigConstants.TASK_MANAGER_DATA_PORT_KEY, dataPort + index)
    }

    val localExecution = numTaskManagers == 1

    val taskManagerActorName = if (singleActorSystem) {
      TaskManager.TASK_MANAGER_NAME + "_" + (index + 1)
    } else {
      TaskManager.TASK_MANAGER_NAME
    }
    
    TaskManager.startTaskManagerComponentsAndActor(
      config,
      system,
      hostname, // network interface to bind to
      Some(taskManagerActorName), // actor name
      Some(createLeaderRetrievalService), // job manager leader retrieval service
      localExecution, // start network stack?
      classOf[TaskManager])
  }

  def getLeaderRPCPort: Int = {
    val index = getLeaderIndex(timeout)

    jobManagerActorSystems match {
      case Some(jmActorSystems) =>
        AkkaUtils.getAddress(jmActorSystems(index)).port match {
          case Some(p) => p
          case None => -1
        }

      case None => throw new Exception("The JobManager of the LocalFlinkMiniCluster has not been " +
        "started properly.")
    }

  }

  def initializeIOFormatClasses(configuration: Configuration): Unit = {
    try {
      val om = classOf[FileOutputFormat[_]].getDeclaredMethod("initDefaultsFromConfiguration",
        classOf[Configuration])
      om.setAccessible(true)
      om.invoke(null, configuration)
    } catch {
      case e: Exception =>
        LOG.error("Cannot (re) initialize the globally loaded defaults. Some classes might not " +
          "follow the specified default behaviour.")
    }
  }

  def setMemory(config: Configuration): Unit = {
    // set this only if no memory was pre-configured
    if (config.getInteger(ConfigConstants.TASK_MANAGER_MEMORY_SIZE_KEY, -1) == -1) {

      val bufferSize: Int = config.getInteger(
        ConfigConstants.TASK_MANAGER_MEMORY_SEGMENT_SIZE_KEY,
        ConfigConstants.DEFAULT_TASK_MANAGER_MEMORY_SEGMENT_SIZE)
      
      val bufferMem: Long = config.getLong(
        ConfigConstants.TASK_MANAGER_NETWORK_NUM_BUFFERS_KEY,
        ConfigConstants.DEFAULT_TASK_MANAGER_NETWORK_NUM_BUFFERS) * bufferSize.toLong

      val numTaskManager = config.getInteger(
        ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, 1)

      val memoryFraction = config.getFloat(
        ConfigConstants.TASK_MANAGER_MEMORY_FRACTION_KEY,
        ConfigConstants.DEFAULT_MEMORY_MANAGER_MEMORY_FRACTION)

      // full memory size
      var memorySize: Long = EnvironmentInformation.getSizeOfFreeHeapMemoryWithDefrag

      // compute the memory size per task manager. we assume equally much memory for
      // each TaskManagers and each JobManager
      memorySize /= numTaskManager + 1 // the +1 is the job manager

      // for each TaskManager, subtract the memory needed for memory buffers
      memorySize -= bufferMem
      memorySize = (memorySize * memoryFraction).toLong
      memorySize >>>= 20 // bytes to megabytes
      config.setLong(ConfigConstants.TASK_MANAGER_MEMORY_SIZE_KEY, memorySize)
    }
  }

  def getDefaultConfig: Configuration = {
    val config: Configuration = new Configuration()

    config.setString(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, hostname)

    config.setInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, 1)

    // Reduce number of threads for local execution
    config.setInteger(NettyConfig.NUM_THREADS_CLIENT, 1)
    config.setInteger(NettyConfig.NUM_THREADS_SERVER, 2)

    config
  }

  protected def getJobManagerName(index: Int): String = {
    if(singleActorSystem) {
      JobManager.JOB_MANAGER_NAME + "_" + (index + 1)
    } else {
      JobManager.JOB_MANAGER_NAME
    }
  }
  protected def getArchiveName(index: Int): String = {
    if(singleActorSystem) {
      JobManager.ARCHIVE_NAME + "_" + (index + 1)
    } else {
      JobManager.ARCHIVE_NAME
    }
  }
}
