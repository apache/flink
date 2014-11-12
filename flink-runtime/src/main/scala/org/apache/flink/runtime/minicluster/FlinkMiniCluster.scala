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

import java.util.concurrent.TimeUnit

import akka.pattern.ask
import akka.actor.{ActorRef, ActorSystem}
import org.apache.flink.api.common.io.FileOutputFormat
import org.apache.flink.configuration.{ConfigConstants, Configuration}
import org.apache.flink.runtime.akka.AkkaUtils
import org.apache.flink.runtime.messages.TaskManagerMessages.NotifyWhenRegisteredAtJobManager
import org.apache.flink.runtime.util.EnvironmentInformation
import org.slf4j.LoggerFactory

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{Future, Await}

abstract class FlinkMiniCluster(userConfiguration: Configuration) {
  import FlinkMiniCluster._

  implicit val timeout = FiniteDuration(userConfiguration.getInteger(ConfigConstants
    .AKKA_ASK_TIMEOUT, ConfigConstants.DEFAULT_AKKA_ASK_TIMEOUT), TimeUnit.SECONDS)

  val configuration = generateConfiguration(userConfiguration)

  val jobManagerActorSystem = startJobManagerActorSystem()
  val jobManagerActor = startJobManager(jobManagerActorSystem)

  val numTaskManagers = configuration.getInteger(ConfigConstants
    .LOCAL_INSTANCE_MANAGER_NUMBER_TASK_MANAGER, 1)

  val actorSystemsTaskManagers = for(i <- 0 until numTaskManagers) yield {
    val actorSystem = startTaskManagerActorSystem(i)
    (actorSystem, startTaskManager(i)(actorSystem))
  }

  val (taskManagerActorSystems, taskManagerActors) = actorSystemsTaskManagers.unzip

  waitForTaskManagersToBeRegistered()

  def generateConfiguration(userConfiguration: Configuration): Configuration

  def startJobManager(implicit system: ActorSystem): ActorRef
  def startTaskManager(index: Int)(implicit system: ActorSystem):
  ActorRef

  def startJobManagerActorSystem(): ActorSystem = {
    val port = configuration.getInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY,
      ConfigConstants.DEFAULT_JOB_MANAGER_IPC_PORT)

    AkkaUtils.createActorSystem(HOSTNAME, port, configuration)
  }

  def startTaskManagerActorSystem(index: Int): ActorSystem = {
    val port = configuration.getInteger(ConfigConstants.TASK_MANAGER_IPC_PORT_KEY,
      ConfigConstants.DEFAULT_TASK_MANAGER_IPC_PORT)

    AkkaUtils.createActorSystem(HOSTNAME, if(port != 0) port + index else port,
      configuration)
  }

  def getJobManager: ActorRef = {
    jobManagerActor
  }



  def getTaskManagers = {
    taskManagerActors
  }

  def stop(): Unit = {
    LOG.info("Stopping FlinkMiniCluster.")
    shutdown()
    awaitTermination();
  }

  def shutdown(): Unit = {
    taskManagerActorSystems foreach { _.shutdown() }
    jobManagerActorSystem.shutdown()
  }

  def awaitTermination(): Unit = {
    taskManagerActorSystems foreach { _.awaitTermination()}
    jobManagerActorSystem.awaitTermination()
  }

  def waitForTaskManagersToBeRegistered(): Unit = {
    implicit val executionContext = AkkaUtils.globalExecutionContext

    val futures = taskManagerActors map {
      taskManager => (taskManager ? NotifyWhenRegisteredAtJobManager)(timeout)
    }

    Await.ready(Future.sequence(futures), timeout)
  }
}

object FlinkMiniCluster{
  val LOG = LoggerFactory.getLogger(classOf[FlinkMiniCluster])
  val HOSTNAME = "localhost"

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
    var memorySize: Long = EnvironmentInformation.getSizeOfFreeHeapMemoryWithDefrag
    val bufferMem: Long = ConfigConstants.DEFAULT_TASK_MANAGER_NETWORK_NUM_BUFFERS *
      ConfigConstants.DEFAULT_TASK_MANAGER_NETWORK_BUFFER_SIZE
    val numTaskManager = 1
    val taskManagerNumSlots: Int = ConfigConstants.DEFAULT_TASK_MANAGER_NUM_TASK_SLOTS
    memorySize = memorySize - (bufferMem * numTaskManager)
    memorySize = (memorySize * ConfigConstants.DEFAULT_MEMORY_MANAGER_MEMORY_FRACTION).toLong
    memorySize >>>= 20
    memorySize /= numTaskManager
    config.setLong(ConfigConstants.TASK_MANAGER_MEMORY_SIZE_KEY, memorySize)
    config.setInteger(ConfigConstants.LOCAL_INSTANCE_MANAGER_NUMBER_TASK_MANAGER, numTaskManager)
    config.setInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, taskManagerNumSlots)
    config
  }

}
