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

package org.apache.flink.test.util

import java.util.concurrent.TimeoutException

import akka.pattern.ask
import akka.actor.{Props, ActorRef, ActorSystem}
import akka.pattern.Patterns._
import org.apache.curator.test.TestingCluster
import org.apache.flink.configuration.{ConfigConstants, Configuration}
import org.apache.flink.runtime.StreamingMode
import org.apache.flink.runtime.akka.AkkaUtils
import org.apache.flink.runtime.jobmanager.{RecoveryMode, JobManager}
import org.apache.flink.runtime.minicluster.LocalFlinkMiniCluster
import org.apache.flink.runtime.taskmanager.TaskManager
import org.apache.flink.runtime.testingUtils.TestingTaskManagerMessages
.NotifyWhenRegisteredAtJobManager
import org.apache.flink.runtime.testingUtils.{TestingUtils, TestingTaskManager,
TestingJobManager, TestingMemoryArchivist}
import org.apache.flink.runtime.webmonitor.WebMonitor

import scala.concurrent.{Future, Promise, Await}

/**
 * A forkable mini cluster is a special case of the mini cluster, used for parallel test execution
 * on build servers. If multiple tests run in parallel, the cluster picks up the fork number and
 * uses it to avoid port conflicts.
 *
 * @param userConfiguration Configuration object with the user provided configuration values
 * @param singleActorSystem true, if all actors (JobManager and TaskManager) shall be run in the
 *                          same [[ActorSystem]], otherwise false.
 */
class ForkableFlinkMiniCluster(
    userConfiguration: Configuration,
    singleActorSystem: Boolean,
    streamingMode: StreamingMode)
  extends LocalFlinkMiniCluster(userConfiguration, singleActorSystem, streamingMode) {

  def this(userConfiguration: Configuration, singleActorSystem: Boolean) 
       = this(userConfiguration, singleActorSystem, StreamingMode.BATCH_ONLY)

  def this(userConfiguration: Configuration) = this(userConfiguration, true)
  
  // --------------------------------------------------------------------------

  var zookeeperCluster: Option[TestingCluster] = None
  
  override def generateConfiguration(userConfiguration: Configuration): Configuration = {
    val forNumberString = System.getProperty("forkNumber")

    val forkNumber = try {
      Integer.parseInt(forNumberString)
    }
    catch {
      case e: NumberFormatException => -1
    }

    val config = userConfiguration.clone()

    if (forkNumber != -1) {
      val jobManagerRPC = 1024 + forkNumber*300
      val taskManagerRPC = 1024 + forkNumber*300 + 100
      val taskManagerData = 1024 + forkNumber*300 + 200

      config.setInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY, jobManagerRPC)
      config.setInteger(ConfigConstants.TASK_MANAGER_IPC_PORT_KEY, taskManagerRPC)
      config.setInteger(ConfigConstants.TASK_MANAGER_DATA_PORT_KEY, taskManagerData)
    }

    super.generateConfiguration(config)
  }

  override def startJobManager(index: Int, actorSystem: ActorSystem): ActorRef = {
    val config = configuration.clone()

    val jobManagerName = getJobManagerName(index)
    val archiveName = getArchiveName(index)

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
      _,
      executionRetries,
      delayBetweenRetries,
      timeout,
      archiveCount,
      leaderElectionService) = JobManager.createJobManagerComponents(config)
      val testArchiveProps = Props(
        new TestingMemoryArchivist(archiveCount))

    val archiver = actorSystem.actorOf(testArchiveProps, archiveName)

    val jobManagerProps = Props(
      new TestingJobManager(
        configuration,
        executionContext,
        instanceManager,
        scheduler,
        libraryCacheManager,
        archiver,
        executionRetries,
        delayBetweenRetries,
        timeout,
        streamingMode,
        leaderElectionService))

    val jobManager = actorSystem.actorOf(jobManagerProps, jobManagerName)

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

    TaskManager.startTaskManagerComponentsAndActor(
      config,
      system,
      hostname,
      Some(TaskManager.TASK_MANAGER_NAME + index),
      Some(createLeaderRetrievalService),
      localExecution,
      streamingMode,
      classOf[TestingTaskManager])
  }

  def restartLeadingJobManager(): Unit = {
    this.synchronized {
      (jobManagerActorSystems, jobManagerActors) match {
        case (Some(jmActorSystems), Some(jmActors)) =>
          val leader = getLeaderGateway(AkkaUtils.getTimeout(configuration))
          val index = getLeaderIndex(AkkaUtils.getTimeout(configuration))

          clearLeader()

          val stopped = gracefulStop(leader.actor(), TestingUtils.TESTING_DURATION)
          Await.result(stopped, TestingUtils.TESTING_DURATION)

          if(!singleActorSystem) {
            jmActorSystems(index).shutdown()
            jmActorSystems(index).awaitTermination()
          }

          val newJobManagerActorSystem = if(!singleActorSystem) {
            startJobManagerActorSystem(index)
          } else {
            jmActorSystems(0)
          }

          val newJobManagerActor = startJobManager(index, newJobManagerActorSystem)

          jobManagerActors = Some(jmActors.patch(index, Seq(newJobManagerActor), 1))
          jobManagerActorSystems = Some(jmActorSystems.patch(
            index,
            Seq(newJobManagerActorSystem),
            1))

          val lrs = createLeaderRetrievalService

          leaderRetrievalService = Some(lrs)
          lrs.start(this)

        case _ => throw new Exception("The JobManager of the ForkableFlinkMiniCluster have not " +
          "been started properly.")
      }
    }
  }

  def restartTaskManager(index: Int): Unit = {
    (taskManagerActorSystems, taskManagerActors) match {
      case (Some(tmActorSystems), Some(tmActors)) =>
        val stopped = gracefulStop(tmActors(index), TestingUtils.TESTING_DURATION)
        Await.result(stopped, TestingUtils.TESTING_DURATION)

        if(!singleActorSystem) {
          tmActorSystems(index).shutdown()
          tmActorSystems(index).awaitTermination()
        }

        val taskManagerActorSystem  = if(!singleActorSystem) {
          startTaskManagerActorSystem(index)
        } else {
          tmActorSystems(0)
        }

        val taskManagerActor = startTaskManager(index, taskManagerActorSystem)

        taskManagerActors = Some(tmActors.patch(index, Seq(taskManagerActor), 1))
        taskManagerActorSystems = Some(tmActorSystems.patch(index, Seq(taskManagerActorSystem), 1))

      case _ => throw new Exception("The TaskManager of the ForkableFlinkMiniCluster have not " +
        "been started properly.")
    }
  }

  override def start(): Unit = {
    val zookeeperURL = configuration.getString(ConfigConstants.ZOOKEEPER_QUORUM_KEY, "")

    zookeeperCluster = if(recoveryMode == RecoveryMode.ZOOKEEPER && zookeeperURL.equals("")) {
      LOG.info("Starting ZooKeeper cluster.")

      val testingCluster = new TestingCluster(numJobManagers)

      configuration.setString(ConfigConstants.ZOOKEEPER_QUORUM_KEY, testingCluster.getConnectString)

      testingCluster.start()

      Some(testingCluster)
    } else {
      None
    }

    super.start()
  }

  override def stop(): Unit = {
    super.stop()

    zookeeperCluster.foreach{
      LOG.info("Stopping ZooKeeper cluster.")
      _.close()
    }
  }

  def waitForTaskManagersToBeRegisteredAtJobManager(jobManager: ActorRef): Unit = {
    val futures = taskManagerActors.map {
      _.map {
        tm => (tm ? NotifyWhenRegisteredAtJobManager(jobManager))(timeout)
      }
    }.getOrElse(Seq())

    try {
      Await.ready(Future.sequence(futures), timeout)
    } catch {
      case t: TimeoutException =>
        throw new Exception("Timeout while waiting for TaskManagers to register at " +
          s"${jobManager.path}")
    }

  }
}

object ForkableFlinkMiniCluster {

  import org.apache.flink.runtime.testingUtils.TestingUtils.DEFAULT_AKKA_ASK_TIMEOUT

  def startCluster(
      numSlots: Int,
      numTaskManagers: Int,
      timeout: String = DEFAULT_AKKA_ASK_TIMEOUT)
    : ForkableFlinkMiniCluster = {

    val config = new Configuration()
    config.setInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, numSlots)
    config.setInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, numTaskManagers)
    config.setString(ConfigConstants.AKKA_ASK_TIMEOUT, timeout)

    val cluster = new ForkableFlinkMiniCluster(config)

    cluster.start()

    cluster
  }
}
