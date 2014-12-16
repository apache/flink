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
import com.typesafe.config.{ConfigFactory}
import org.apache.flink.configuration.{ConfigConstants, Configuration}
import org.apache.flink.runtime.akka.AkkaUtils
import org.apache.flink.runtime.messages.TaskManagerMessages.NotifyWhenRegisteredAtJobManager
import org.slf4j.LoggerFactory

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{Future, Await}

abstract class FlinkMiniCluster(userConfiguration: Configuration,
                                val singleActorSystem: Boolean) {
  import FlinkMiniCluster._

  val HOSTNAME = "localhost"

  implicit val timeout = FiniteDuration(userConfiguration.getInteger(ConfigConstants
    .AKKA_ASK_TIMEOUT, ConfigConstants.DEFAULT_AKKA_ASK_TIMEOUT), TimeUnit.SECONDS)

  val configuration = generateConfiguration(userConfiguration)

  if(singleActorSystem){
    configuration.setString(ConfigConstants.JOB_MANAGER_AKKA_URL, "akka://flink/user/jobmanager")
  }

  val jobManagerActorSystem = startJobManagerActorSystem()
  val jobManagerActor = startJobManager(jobManagerActorSystem)

  val numTaskManagers = configuration.getInteger(ConfigConstants
    .LOCAL_INSTANCE_MANAGER_NUMBER_TASK_MANAGER, 1)

  val actorSystemsTaskManagers = for(i <- 0 until numTaskManagers) yield {
    val actorSystem = if(singleActorSystem) {
      jobManagerActorSystem
    }
    else{
      startTaskManagerActorSystem(i)
    }

    (actorSystem, startTaskManager(i)(actorSystem))
  }

  val (taskManagerActorSystems, taskManagerActors) = actorSystemsTaskManagers.unzip

  waitForTaskManagersToBeRegistered()

  def generateConfiguration(userConfiguration: Configuration): Configuration

  def startJobManager(implicit system: ActorSystem): ActorRef
  def startTaskManager(index: Int)(implicit system: ActorSystem):
  ActorRef

  def getJobManagerAkkaConfigString(): String = {
    val port = configuration.getInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY, ConfigConstants
      .DEFAULT_JOB_MANAGER_IPC_PORT)

    if(singleActorSystem){
      AkkaUtils.getLocalConfigString(configuration)
    }else{
      AkkaUtils.getConfigString(HOSTNAME, port, configuration)
    }

  }

  def startJobManagerActorSystem(): ActorSystem = {
    val configString = getJobManagerAkkaConfigString()

    val config = ConfigFactory.parseString(getJobManagerAkkaConfigString())

    AkkaUtils.createActorSystem(config)
  }

  def getTaskManagerAkkaConfigString(index: Int): String = {
    val port = configuration.getInteger(ConfigConstants.TASK_MANAGER_IPC_PORT_KEY,
      ConfigConstants.DEFAULT_TASK_MANAGER_IPC_PORT)

    AkkaUtils.getConfigString(HOSTNAME, if(port != 0) port + index else port,
      configuration)
  }

  def startTaskManagerActorSystem(index: Int): ActorSystem = {
    val config = ConfigFactory.parseString(getTaskManagerAkkaConfigString(index))

    AkkaUtils.createActorSystem(config)
  }

  def getJobManager: ActorRef = {
    jobManagerActor
  }

  def getTaskManagers = {
    taskManagerActors
  }

  def getTaskManagersAsJava = {
    import collection.JavaConverters._
    taskManagerActors.asJava
  }

  def stop(): Unit = {
    LOG.info("Stopping FlinkMiniCluster.")
    shutdown()
    awaitTermination();
  }

  def shutdown(): Unit = {
    if(!singleActorSystem){
      taskManagerActorSystems foreach {
        _.shutdown()
      }
    }

    jobManagerActorSystem.shutdown()
  }

  def awaitTermination(): Unit = {
    jobManagerActorSystem.awaitTermination()

    if(!singleActorSystem) {
      taskManagerActorSystems foreach {
        _.awaitTermination()
      }
    }
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
}
