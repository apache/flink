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

import java.util.concurrent.{ExecutorService, TimeUnit, TimeoutException}

import akka.pattern.ask
import akka.actor.{ActorRef, ActorSystem, Props}
import akka.pattern.Patterns._
import akka.testkit.CallingThreadDispatcher
import org.apache.flink.configuration.{ConfigConstants, Configuration}
import org.apache.flink.runtime.akka.AkkaUtils
import org.apache.flink.runtime.checkpoint.CheckpointRecoveryFactory
import org.apache.flink.runtime.checkpoint.savepoint.SavepointStore
import org.apache.flink.runtime.clusterframework.FlinkResourceManager
import org.apache.flink.runtime.clusterframework.types.ResourceIDRetrievable
import org.apache.flink.runtime.execution.librarycache.BlobLibraryCacheManager
import org.apache.flink.runtime.executiongraph.restart.RestartStrategyFactory
import org.apache.flink.runtime.instance.InstanceManager
import org.apache.flink.runtime.jobmanager.scheduler.Scheduler
import org.apache.flink.runtime.jobmanager.{JobManager, MemoryArchivist, SubmittedJobGraphStore}
import org.apache.flink.runtime.leaderelection.LeaderElectionService
import org.apache.flink.runtime.metrics.MetricRegistry
import org.apache.flink.runtime.minicluster.LocalFlinkMiniCluster
import org.apache.flink.runtime.testutils.TestingResourceManager
import org.apache.flink.runtime.taskmanager.TaskManager
import org.apache.flink.runtime.testingUtils.TestingMessages.Alive
import org.apache.flink.runtime.testingUtils.TestingTaskManagerMessages.NotifyWhenRegisteredAtJobManager

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{Await, Future}

/**
 * Testing cluster which starts the [[JobManager]] and [[TaskManager]] actors with testing support
 * in the same or separate [[ActorSystem]]s.
 *
 * @param userConfiguration Configuration object with the user provided configuration values
 * @param singleActorSystem true if all actors shall be running in the same [[ActorSystem]],
 *                          otherwise false
 */
class TestingCluster(
    userConfiguration: Configuration,
    singleActorSystem: Boolean,
    synchronousDispatcher: Boolean)
  extends LocalFlinkMiniCluster(
    userConfiguration,
    singleActorSystem) {

  def this(userConfiguration: Configuration, singleActorSystem: Boolean) =
    this(userConfiguration, singleActorSystem, false)

  def this(userConfiguration: Configuration) = this(userConfiguration, true, false)

  // --------------------------------------------------------------------------

  override val jobManagerClass: Class[_ <: JobManager] = classOf[TestingJobManager]

  override val resourceManagerClass: Class[_ <: FlinkResourceManager[_ <: ResourceIDRetrievable]] =
    classOf[TestingResourceManager]

  override val taskManagerClass: Class[_ <: TaskManager] = classOf[TestingTaskManager]

  override val memoryArchivistClass: Class[_ <: MemoryArchivist] = classOf[TestingMemoryArchivist]

  override def getJobManagerProps(
    jobManagerClass: Class[_ <: JobManager],
    configuration: Configuration,
    executorService: ExecutorService,
    instanceManager: InstanceManager,
    scheduler: Scheduler,
    libraryCacheManager: BlobLibraryCacheManager,
    archive: ActorRef,
    restartStrategyFactory: RestartStrategyFactory,
    timeout: FiniteDuration,
    leaderElectionService: LeaderElectionService,
    submittedJobGraphStore: SubmittedJobGraphStore,
    checkpointRecoveryFactory: CheckpointRecoveryFactory,
    savepointStore: SavepointStore,
    jobRecoveryTimeout: FiniteDuration,
    metricsRegistry: Option[MetricRegistry]): Props = {

    val props = super.getJobManagerProps(
      jobManagerClass,
      configuration,
      executorService,
      instanceManager,
      scheduler,
      libraryCacheManager,
      archive,
      restartStrategyFactory,
      timeout,
      leaderElectionService,
      submittedJobGraphStore,
      checkpointRecoveryFactory,
      savepointStore,
      jobRecoveryTimeout,
      metricsRegistry)

    if (synchronousDispatcher) {
      props.withDispatcher(CallingThreadDispatcher.Id)
    } else {
      props
    }
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
        jm => (jm ? Alive)(timeout)
      }
    } getOrElse(Seq())

    val resourceManagersAliveFutures = resourceManagerActors map {
      _ map {
        rm => (rm ? Alive)(timeout)
      }
    } getOrElse(Seq())

    val combinedFuture = Future.sequence(tmsAliveFutures ++ jmsAliveFutures ++
                                           resourceManagersAliveFutures)

    Await.ready(combinedFuture, timeout)
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

  def restartLeadingJobManager(): Unit = {
    this.synchronized {
      (jobManagerActorSystems, jobManagerActors) match {
        case (Some(jmActorSystems), Some(jmActors)) =>
          val leader = getLeaderGateway(AkkaUtils.getTimeout(originalConfiguration))
          val index = getLeaderIndex(AkkaUtils.getTimeout(originalConfiguration))

          // restart the leading job manager with the same port
          val port = getLeaderRPCPort
          val oldPort = originalConfiguration.getInteger(
            ConfigConstants.JOB_MANAGER_IPC_PORT_KEY,
            0)

          // we have to set the old port in the configuration file because this is used for startup
          originalConfiguration.setInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY, port)

          clearLeader()

          val stopped = gracefulStop(leader.actor(), TestingCluster.MAX_RESTART_DURATION)
          Await.result(stopped, TestingCluster.MAX_RESTART_DURATION)

          if(!singleActorSystem) {
            jmActorSystems(index).shutdown()
            jmActorSystems(index).awaitTermination()
          }

          val newJobManagerActorSystem = if(!singleActorSystem) {
            startJobManagerActorSystem(index)
          } else {
            jmActorSystems.head
          }

          // reset the original configuration
          originalConfiguration.setInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY, oldPort)

          val newJobManagerActor = startJobManager(index, newJobManagerActorSystem)

          jobManagerActors = Some(jmActors.patch(index, Seq(newJobManagerActor), 1))
          jobManagerActorSystems = Some(jmActorSystems.patch(
            index,
            Seq(newJobManagerActorSystem),
            1))

          val lrs = createLeaderRetrievalService()

          jobManagerLeaderRetrievalService = Some(lrs)
          lrs.start(this)

        case _ => throw new Exception("The JobManager of the TestingCluster have not " +
                                        "been started properly.")
      }
    }
  }

  def restartTaskManager(index: Int): Unit = {
    (taskManagerActorSystems, taskManagerActors) match {
      case (Some(tmActorSystems), Some(tmActors)) =>
        val stopped = gracefulStop(tmActors(index), TestingCluster.MAX_RESTART_DURATION)
        Await.result(stopped, TestingCluster.MAX_RESTART_DURATION)

        if(!singleActorSystem) {
          tmActorSystems(index).shutdown()
          tmActorSystems(index).awaitTermination()
        }

        val taskManagerActorSystem  = if(!singleActorSystem) {
          startTaskManagerActorSystem(index)
        } else {
          tmActorSystems.head
        }

        val taskManagerActor = startTaskManager(index, taskManagerActorSystem)

        taskManagerActors = Some(tmActors.patch(index, Seq(taskManagerActor), 1))
        taskManagerActorSystems = Some(tmActorSystems.patch(index, Seq(taskManagerActorSystem), 1))

      case _ => throw new Exception("The TaskManager of the TestingCluster have not " +
                                      "been started properly.")
    }
  }

  def addTaskManager(): Unit = {
    if (useSingleActorSystem) {
      (jobManagerActorSystems, taskManagerActors) match {
        case (Some(jmSystems), Some(tmActors)) =>
          val index = numTaskManagers
          taskManagerActors = Some(tmActors :+ startTaskManager(index, jmSystems(0)))
          numTaskManagers += 1
        case _ => throw new IllegalStateException("Cluster has not been started properly.")
      }
    } else {
      (taskManagerActorSystems, taskManagerActors) match {
        case (Some(tmSystems), Some(tmActors)) =>
          val index = numTaskManagers
          val newTmSystem = startTaskManagerActorSystem(index)
          val newTmActor = startTaskManager(index, newTmSystem)

          taskManagerActorSystems = Some(tmSystems :+ newTmSystem)
          taskManagerActors = Some(tmActors :+ newTmActor)

          numTaskManagers += 1
        case _ => throw new IllegalStateException("Cluster has not been started properly.")
      }
    }
  }
}

object TestingCluster {
  val MAX_RESTART_DURATION = new FiniteDuration(2, TimeUnit.MINUTES)
}
