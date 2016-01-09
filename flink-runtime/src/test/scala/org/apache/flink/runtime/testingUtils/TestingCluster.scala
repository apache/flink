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

import java.util.concurrent.TimeoutException

import akka.pattern.ask
import akka.actor.{ActorRef, Props, ActorSystem}
import akka.testkit.CallingThreadDispatcher
import org.apache.flink.configuration.{ConfigConstants, Configuration}
import org.apache.flink.runtime.jobmanager.JobManager
import org.apache.flink.runtime.leaderelection.LeaderElectionService
import org.apache.flink.runtime.minicluster.FlinkMiniCluster
import org.apache.flink.util.NetUtils
import org.apache.flink.runtime.taskmanager.TaskManager
import org.apache.flink.runtime.testingUtils.TestingMessages.Alive

import scala.concurrent.{Await, Future}

/**
 * Testing cluster which starts the [[JobManager]] and [[TaskManager]] actors with testing support
 * in the same [[ActorSystem]].
 *
 * @param userConfiguration Configuration object with the user provided configuration values
 * @param singleActorSystem true if all actors shall be running in the same [[ActorSystem]],
 *                          otherwise false
 */
class TestingCluster(
                      userConfiguration: Configuration,
                      singleActorSystem: Boolean,
                      synchronousDispatcher: Boolean)
  extends FlinkMiniCluster(
    userConfiguration,
    singleActorSystem) {

  def this(userConfiguration: Configuration, singleActorSystem: Boolean) =
    this(userConfiguration, singleActorSystem, false)

  def this(userConfiguration: Configuration) = this(userConfiguration, true, false)

  // --------------------------------------------------------------------------

  override def generateConfiguration(userConfig: Configuration): Configuration = {
    val cfg = new Configuration()
    cfg.setString(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, "localhost")
    cfg.setInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY, NetUtils.getAvailablePort())
    cfg.setInteger(ConfigConstants.TASK_MANAGER_MEMORY_SIZE_KEY, 10)
    cfg.setInteger(ConfigConstants.JOB_MANAGER_WEB_PORT_KEY, -1)

    cfg.addAll(userConfig)
    cfg
  }

  override def startJobManager(index: Int, actorSystem: ActorSystem): ActorRef = {
    val config = configuration.clone()

    val jobManagerName = if(singleActorSystem) {
      JobManager.JOB_MANAGER_NAME + "_" + (index + 1)
    } else {
      JobManager.JOB_MANAGER_NAME
    }

    val archiveName = if(singleActorSystem) {
      JobManager.ARCHIVE_NAME + "_" + (index + 1)
    } else {
      JobManager.ARCHIVE_NAME
    }

    val jobManagerPort = config.getInteger(
      ConfigConstants.JOB_MANAGER_IPC_PORT_KEY,
      ConfigConstants.DEFAULT_JOB_MANAGER_IPC_PORT)

    if(jobManagerPort > 0) {
      config.setInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY, jobManagerPort + index)
    }

    val (executionContext,
    instanceManager,
    scheduler,
    libraryCacheManager,
    executionRetries,
    delayBetweenRetries,
    timeout,
    archiveCount,
    leaderElectionService,
    submittedJobsGraphs,
    checkpointRecoveryFactory) = JobManager.createJobManagerComponents(
      config,
      createLeaderElectionService())

    val testArchiveProps = Props(new TestingMemoryArchivist(archiveCount))
    val archive = actorSystem.actorOf(testArchiveProps, archiveName)

    val jobManagerProps = Props(
      new TestingJobManager(
        configuration,
        executionContext,
        instanceManager,
        scheduler,
        libraryCacheManager,
        archive,
        executionRetries,
        delayBetweenRetries,
        timeout,
        leaderElectionService,
        submittedJobsGraphs,
        checkpointRecoveryFactory))

    val dispatcherJobManagerProps = if (synchronousDispatcher) {
      // disable asynchronous futures (e.g. accumulator update in Heartbeat)
      jobManagerProps.withDispatcher(CallingThreadDispatcher.Id)
    } else {
      jobManagerProps
    }

    actorSystem.actorOf(dispatcherJobManagerProps, jobManagerName)
  }

  override def startTaskManager(index: Int, system: ActorSystem) = {

    val tmActorName = TaskManager.TASK_MANAGER_NAME + "_" + (index + 1)

    TaskManager.startTaskManagerComponentsAndActor(
      configuration,
      system,
      hostname,
      Some(tmActorName),
      Some(createLeaderRetrievalService),
      numTaskManagers == 1,
      classOf[TestingTaskManager])
  }


  def createLeaderElectionService(): Option[LeaderElectionService] = {
    None
  }

  @throws(classOf[TimeoutException])
  @throws(classOf[InterruptedException])
  def waitForTaskManagersToBeAlive(): Unit = {
    val aliveFutures = taskManagerActors map {
      _ map {
        tm => (tm ? Alive)(timeout)
      }
    } getOrElse(Seq())

    val combinedFuture = Future.sequence(aliveFutures)

    Await.ready(combinedFuture, timeout)
  }

  @throws(classOf[TimeoutException])
  @throws(classOf[InterruptedException])
  def waitForActorsToBeAlive(): Unit = {
    val tmsAliveFutures = taskManagerActors map {
      _ map {
        tm => (tm ? Alive)(timeout)
      }
    } getOrElse(Seq())

    val jmsAliveFutures = jobManagerActors map {
      _ map {
        tm => (tm ? Alive)(timeout)
      }
    } getOrElse(Seq())

    val combinedFuture = Future.sequence(tmsAliveFutures ++ jmsAliveFutures)

    Await.ready(combinedFuture, timeout)
  }
}
