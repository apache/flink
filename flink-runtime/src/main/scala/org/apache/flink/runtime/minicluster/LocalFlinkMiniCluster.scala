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
import org.apache.flink.runtime.io.network.netty.NettyConfig
import org.apache.flink.runtime.jobmanager.JobManager
import org.apache.flink.runtime.taskmanager.TaskManager
import org.apache.flink.runtime.util.EnvironmentInformation
import org.slf4j.LoggerFactory

/**
 * Local Flink mini cluster which executes all [[TaskManager]]s and the [[JobManager]] in the same
 * JVM. It extends the [[FlinkMiniCluster]] by providing a [[JobClient]], having convenience
 * functions to setup Flink's configuration and implementations to create [[JobManager]] and
 * [[TaskManager]].
 *
 * @param userConfiguration Configuration object with the user provided configuration values
 * @param singleActorSystem true if all actors (JobManager and TaskManager) shall be run in the same
 *                          [[ActorSystem]], otherwise false
 */
class LocalFlinkMiniCluster(userConfiguration: Configuration, singleActorSystem: Boolean = true)
  extends FlinkMiniCluster(userConfiguration, singleActorSystem) {

  val jobClientActorSystem = if (singleActorSystem) {
    jobManagerActorSystem
  } else {
    // create an actor system listening on a random port
    AkkaUtils.createDefaultActorSystem()
  }

  var jobClient: Option[ActorRef] = None



  override def generateConfiguration(userConfiguration: Configuration): Configuration = {
    val config = getDefaultConfig

    config.addAll(userConfiguration)
    setMemory(config)
    initializeIOFormatClasses(config)

    config
  }

  override def startJobManager(system: ActorSystem): ActorRef = {
    val (jobManager, _) = JobManager.startJobManagerActors(configuration, system)
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

    TaskManager.startTaskManagerActor(config, system, HOSTNAME, taskManagerActorName,
                                      singleActorSystem, localExecution, classOf[TaskManager])
  }

  def getJobClient(): ActorRef ={
    jobClient match {
      case Some(jc) => jc
      case None =>
        val config = new Configuration()

        config.setString(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, HOSTNAME)
        config.setInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY, getJobManagerRPCPort)

        val jc = JobClient.createJobClientFromConfig(config, singleActorSystem,
                                                            jobClientActorSystem)
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
    // set this only if no memory was preconfigured
    if (config.getInteger(ConfigConstants.TASK_MANAGER_MEMORY_SIZE_KEY, -1) == -1) {

      val bufferMem: Long =
            config.getLong(ConfigConstants.TASK_MANAGER_NETWORK_NUM_BUFFERS_KEY,
                           ConfigConstants.DEFAULT_TASK_MANAGER_NETWORK_NUM_BUFFERS) *
            config.getLong(ConfigConstants.TASK_MANAGER_NETWORK_BUFFER_SIZE_KEY,
                           ConfigConstants.DEFAULT_TASK_MANAGER_NETWORK_BUFFER_SIZE)

      val numTaskManager = config.getInteger(
        ConfigConstants.LOCAL_INSTANCE_MANAGER_NUMBER_TASK_MANAGER, 1)

      val memoryFraction = config.getFloat(ConfigConstants.TASK_MANAGER_MEMORY_FRACTION_KEY,
                                           ConfigConstants.DEFAULT_MEMORY_MANAGER_MEMORY_FRACTION)

      // full memory size
      var memorySize: Long = EnvironmentInformation.getSizeOfFreeHeapMemoryWithDefrag

      // compute the memory size per task manager. we assume equally much memory for
      // each TaskManagers and each JobManager
      memorySize /= numTaskManager + 1 // the +1 is the job manager

      // for each TaskManager, subtract the memory needed for memory buffers
      memorySize -= bufferMem
      memorySize = (memorySize * memoryFraction).toLong
      memorySize >>>= 20  // bytes to megabytes
      config.setLong(ConfigConstants.TASK_MANAGER_MEMORY_SIZE_KEY, memorySize)
    }
  }

  def getDefaultConfig: Configuration = {
    val config: Configuration = new Configuration()

    config.setString(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, HOSTNAME)

    config.setInteger(ConfigConstants.LOCAL_INSTANCE_MANAGER_NUMBER_TASK_MANAGER, 1)

    // Reduce number of threads for local execution
    config.setInteger(NettyConfig.NUM_THREADS_CLIENT, 1)
    config.setInteger(NettyConfig.NUM_THREADS_SERVER, 2)

    config
  }
}
