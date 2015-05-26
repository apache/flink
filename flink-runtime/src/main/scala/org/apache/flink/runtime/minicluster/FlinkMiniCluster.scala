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

import java.net.InetAddress
import akka.pattern.Patterns.gracefulStop

import akka.pattern.ask
import akka.actor.{ActorRef, ActorSystem}
import com.typesafe.config.Config
import org.apache.flink.api.common.JobSubmissionResult
import org.apache.flink.configuration.{ConfigConstants, Configuration}
import org.apache.flink.runtime.StreamingMode
import org.apache.flink.runtime.akka.AkkaUtils
import org.apache.flink.runtime.client.{JobExecutionException, JobClient, SerializedJobExecutionResult}
import org.apache.flink.runtime.jobgraph.JobGraph
import org.apache.flink.runtime.messages.TaskManagerMessages.NotifyWhenRegisteredAtJobManager
import org.slf4j.LoggerFactory

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{Future, Await}

/**
 * Abstract base class for Flink's mini cluster. The mini cluster starts a
 * [[org.apache.flink.runtime.jobmanager.JobManager]] and one or multiple
 * [[org.apache.flink.runtime.taskmanager.TaskManager]]. Depending on the settings, the different
 * actors can all be run in the same [[ActorSystem]] or each one in its own.
 *
 * @param userConfiguration Configuration object with the user provided configuration values
 * @param singleActorSystem true if all actors (JobManager and TaskManager) shall be run in the same
 *                          [[ActorSystem]], otherwise false
 * @param streamingMode True, if the system should be started in streaming mode, false if
 *                      in pure batch mode.
 */
abstract class FlinkMiniCluster(val userConfiguration: Configuration,
                                val singleActorSystem: Boolean,
                                val streamingMode: StreamingMode) {

  def this(userConfiguration: Configuration, singleActorSystem: Boolean) 
         = this(userConfiguration, singleActorSystem, StreamingMode.BATCH_ONLY)
  
  protected val LOG = LoggerFactory.getLogger(classOf[FlinkMiniCluster])

  // --------------------------------------------------------------------------
  //                           Construction
  // --------------------------------------------------------------------------

  // NOTE: THIS MUST BE getByName("localhost"), which is 127.0.0.1 and
  // not getLocalHost(), which may be 127.0.1.1
  val hostname = InetAddress.getByName("localhost").getHostAddress()

  val timeout = AkkaUtils.getTimeout(userConfiguration)

  val configuration = generateConfiguration(userConfiguration)

  var jobManagerActorSystem = startJobManagerActorSystem()
  var jobManagerActor = startJobManager(jobManagerActorSystem)

  val numTaskManagers = configuration.getInteger(
     ConfigConstants.LOCAL_INSTANCE_MANAGER_NUMBER_TASK_MANAGER, 1)

  var (taskManagerActorSystems, taskManagerActors) =
    (for(i <- 0 until numTaskManagers) yield {
      val actorSystem = if(singleActorSystem) {
        jobManagerActorSystem
      } else {
        startTaskManagerActorSystem(i)
      }

      (actorSystem, startTaskManager(i, actorSystem))
    }).unzip

  waitForTaskManagersToBeRegistered()


  // --------------------------------------------------------------------------
  //                           Construction
  // --------------------------------------------------------------------------

  def generateConfiguration(userConfiguration: Configuration): Configuration

  def startJobManager(system: ActorSystem): ActorRef

  def startTaskManager(index: Int, system: ActorSystem): ActorRef

  def getJobManagerAkkaConfig: Config = {
    if (singleActorSystem) {
      AkkaUtils.getAkkaConfig(configuration, None)
    }
    else {
      val port = configuration.getInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY,
        ConfigConstants.DEFAULT_JOB_MANAGER_IPC_PORT)

      AkkaUtils.getAkkaConfig(configuration, Some((hostname, port)))
    }
  }

  def startJobManagerActorSystem(): ActorSystem = {
    val config = getJobManagerAkkaConfig
    AkkaUtils.createActorSystem(config)
  }

  def getTaskManagerAkkaConfig(index: Int): Config = {
    val port = configuration.getInteger(ConfigConstants.TASK_MANAGER_IPC_PORT_KEY,
      ConfigConstants.DEFAULT_TASK_MANAGER_IPC_PORT)

    val resolvedPort = if(port != 0) port + index else port

    AkkaUtils.getAkkaConfig(configuration, Some((hostname, resolvedPort)))
  }

  def startTaskManagerActorSystem(index: Int): ActorSystem = {
    val config = getTaskManagerAkkaConfig(index)

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
    awaitTermination()
  }

  def shutdown(): Unit = {
    val futures = taskManagerActors map {
        gracefulStop(_, timeout)
    }

    val future = gracefulStop(jobManagerActor, timeout)

    implicit val executionContext = AkkaUtils.globalExecutionContext

    Await.ready(Future.sequence(future +: futures), timeout)

    if(!singleActorSystem){
      taskManagerActorSystems foreach {
        _.shutdown()
      }
    }

    jobManagerActorSystem.shutdown()
  }

  def awaitTermination(): Unit = {
    jobManagerActorSystem.awaitTermination()

    taskManagerActorSystems foreach {
      _.awaitTermination()
    }
  }

  def waitForTaskManagersToBeRegistered(): Unit = {
    implicit val executionContext = AkkaUtils.globalExecutionContext

    val futures = taskManagerActors map {
      taskManager => (taskManager ? NotifyWhenRegisteredAtJobManager)(timeout)
    }

    Await.ready(Future.sequence(futures), timeout)
  }

  @throws(classOf[JobExecutionException])
  def submitJobAndWait(jobGraph: JobGraph, printUpdates: Boolean)
                                                                : SerializedJobExecutionResult = {

    submitJobAndWait(jobGraph, printUpdates, timeout)
  }
  
  @throws(classOf[JobExecutionException])
  def submitJobAndWait(jobGraph: JobGraph, printUpdates: Boolean, timeout: FiniteDuration)
                                                                 : SerializedJobExecutionResult = {

    val clientActorSystem = if (singleActorSystem) jobManagerActorSystem
    else JobClient.startJobClientActorSystem(configuration)

    JobClient.submitJobAndWait(clientActorSystem, jobManagerActor, jobGraph, timeout, printUpdates)
  }

  @throws(classOf[JobExecutionException])
  def submitJobDetached(jobGraph: JobGraph) : JobSubmissionResult = {
    JobClient.submitJobDetached(jobManagerActor, jobGraph, timeout)
    new JobSubmissionResult(jobGraph.getJobID)
  }
}
