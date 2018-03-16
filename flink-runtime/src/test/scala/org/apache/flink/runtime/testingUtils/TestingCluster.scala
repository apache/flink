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

import java.io.IOException
import java.util.concurrent.{Executor, ScheduledExecutorService, TimeUnit, TimeoutException}

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.pattern.Patterns._
import akka.pattern.ask
import akka.testkit.CallingThreadDispatcher
import org.apache.flink.api.common.JobID
import org.apache.flink.configuration.{Configuration, JobManagerOptions}
import org.apache.flink.runtime.akka.AkkaUtils
import org.apache.flink.runtime.blob.BlobServer
import org.apache.flink.runtime.checkpoint.savepoint.Savepoint
import org.apache.flink.runtime.checkpoint.{CheckpointOptions, CheckpointRecoveryFactory, CheckpointRetentionPolicy}
import org.apache.flink.runtime.clusterframework.FlinkResourceManager
import org.apache.flink.runtime.clusterframework.types.ResourceIDRetrievable
import org.apache.flink.runtime.execution.librarycache.BlobLibraryCacheManager
import org.apache.flink.runtime.executiongraph.restart.RestartStrategyFactory
import org.apache.flink.runtime.highavailability.{HighAvailabilityServices, HighAvailabilityServicesUtils}
import org.apache.flink.runtime.instance.{ActorGateway, InstanceManager}
import org.apache.flink.runtime.jobmanager.scheduler.Scheduler
import org.apache.flink.runtime.jobmanager.{JobManager, MemoryArchivist, SubmittedJobGraphStore}
import org.apache.flink.runtime.leaderelection.LeaderElectionService
import org.apache.flink.runtime.messages.JobManagerMessages
import org.apache.flink.runtime.messages.JobManagerMessages._
import org.apache.flink.runtime.metrics.groups.JobManagerMetricGroup
import org.apache.flink.runtime.minicluster.LocalFlinkMiniCluster
import org.apache.flink.runtime.taskmanager.TaskManager
import org.apache.flink.runtime.testingUtils.TestingJobManagerMessages._
import org.apache.flink.runtime.testingUtils.TestingMessages.Alive
import org.apache.flink.runtime.testingUtils.TestingTaskManagerMessages.NotifyWhenRegisteredAtJobManager
import org.apache.flink.runtime.testutils.TestingResourceManager

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{Await, ExecutionContext, Future}

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
    highAvailabilityServices: HighAvailabilityServices,
    singleActorSystem: Boolean,
    synchronousDispatcher: Boolean)
  extends LocalFlinkMiniCluster(
    userConfiguration,
    highAvailabilityServices,
    singleActorSystem) {

  def this(
      userConfiguration: Configuration,
      singleActorSystem: Boolean,
      synchronousDispatcher: Boolean) = {
    this(
      userConfiguration,
      HighAvailabilityServicesUtils.createAvailableOrEmbeddedServices(
        userConfiguration,
        ExecutionContext.global),
      singleActorSystem,
      synchronousDispatcher)
  }

  def this(userConfiguration: Configuration, singleActorSystem: Boolean) = {
    this(
      userConfiguration,
      singleActorSystem,
      false)
  }

  def this(userConfiguration: Configuration) = this(userConfiguration, true)

  // --------------------------------------------------------------------------

  override val jobManagerClass: Class[_ <: JobManager] = classOf[TestingJobManager]

  override val resourceManagerClass: Class[_ <: FlinkResourceManager[_ <: ResourceIDRetrievable]] =
    classOf[TestingResourceManager]

  override val taskManagerClass: Class[_ <: TaskManager] = classOf[TestingTaskManager]

  override val memoryArchivistClass: Class[_ <: MemoryArchivist] = classOf[TestingMemoryArchivist]

  override def getJobManagerProps(
    jobManagerClass: Class[_ <: JobManager],
    configuration: Configuration,
    futureExecutor: ScheduledExecutorService,
    ioExecutor: Executor,
    instanceManager: InstanceManager,
    scheduler: Scheduler,
    blobServer: BlobServer,
    libraryCacheManager: BlobLibraryCacheManager,
    archive: ActorRef,
    restartStrategyFactory: RestartStrategyFactory,
    timeout: FiniteDuration,
    leaderElectionService: LeaderElectionService,
    submittedJobGraphStore: SubmittedJobGraphStore,
    checkpointRecoveryFactory: CheckpointRecoveryFactory,
    jobRecoveryTimeout: FiniteDuration,
    jobManagerMetricGroup: JobManagerMetricGroup,
    optRestAddress: Option[String]): Props = {

    val props = super.getJobManagerProps(
      jobManagerClass,
      configuration,
      futureExecutor,
      ioExecutor,
      instanceManager,
      scheduler,
      blobServer,
      libraryCacheManager,
      archive,
      restartStrategyFactory,
      timeout,
      leaderElectionService,
      submittedJobGraphStore,
      checkpointRecoveryFactory,
      jobRecoveryTimeout,
      jobManagerMetricGroup,
      optRestAddress)

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
            JobManagerOptions.PORT,
            0)

          // we have to set the old port in the configuration file because this is used for startup
          originalConfiguration.setInteger(JobManagerOptions.PORT, port)

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
          originalConfiguration.setInteger(JobManagerOptions.PORT, oldPort)

          val newJobManagerActor = startJobManager(
            index,
            newJobManagerActorSystem,
            webMonitor.map(_.getRestAddress))

          jobManagerActors = Some(jmActors.patch(index, Seq(newJobManagerActor), 1))
          jobManagerActorSystems = Some(jmActorSystems.patch(
            index,
            Seq(newJobManagerActorSystem),
            1))

          jobManagerLeaderRetrievalService.foreach(_.stop())

          jobManagerLeaderRetrievalService = Option(
            highAvailabilityServices.getJobManagerLeaderRetriever(
              HighAvailabilityServices.DEFAULT_JOB_ID))

          jobManagerLeaderRetrievalService.foreach(_.start(this))

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

  @throws(classOf[IOException])
  def triggerSavepoint(jobId: JobID): String = {
    val timeout = AkkaUtils.getTimeout(configuration)
    triggerSavepoint(jobId, getLeaderGateway(timeout), timeout)
  }

  @throws(classOf[IOException])
  def requestSavepoint(savepointPath: String): Savepoint = {
    val timeout = AkkaUtils.getTimeout(configuration)
    requestSavepoint(savepointPath, getLeaderGateway(timeout), timeout)
  }

  @throws(classOf[IOException])
  def disposeSavepoint(savepointPath: String): Unit = {
    val timeout = AkkaUtils.getTimeout(configuration)
    disposeSavepoint(savepointPath, getLeaderGateway(timeout), timeout)
  }

  @throws(classOf[IOException])
  def triggerSavepoint(
      jobId: JobID,
      jobManager: ActorGateway,
      timeout: FiniteDuration): String = {
    val result = Await.result(
      jobManager.ask(
        TriggerSavepoint(jobId), timeout), timeout)

    result match {
      case success: TriggerSavepointSuccess => success.savepointPath
      case fail: TriggerSavepointFailure => throw new IOException(fail.cause)
      case _ => throw new IllegalStateException("Trigger savepoint failed")
    }
  }

  @throws(classOf[IOException])
  def requestSavepoint(
      savepointPath: String,
      jobManager: ActorGateway,
      timeout: FiniteDuration): Savepoint = {
    val result = Await.result(
      jobManager.ask(
        TestingJobManagerMessages.RequestSavepoint(savepointPath), timeout), timeout)

    result match {
      case success: ResponseSavepoint => success.savepoint
      case _ => throw new IOException("Request savepoint failed")
    }
  }

  @throws(classOf[IOException])
  def disposeSavepoint(
      savepointPath: String,
      jobManager: ActorGateway,
      timeout: FiniteDuration): Unit = {
    val timeout = AkkaUtils.getTimeout(originalConfiguration)
    val jobManager = getLeaderGateway(timeout)
    val result = Await.result(jobManager.ask(DisposeSavepoint(savepointPath), timeout), timeout)
    result match {
      case DisposeSavepointSuccess =>
      case _ => throw new IOException("Dispose savepoint failed")
    }
  }

  @throws(classOf[IOException])
  def requestCheckpoint(
      jobId: JobID,
      checkpointRetentionPolicy: CheckpointRetentionPolicy): String = {

    val jobManagerGateway = getLeaderGateway(timeout)

    // wait until the cluster is ready to take a checkpoint.
    val allRunning = jobManagerGateway.ask(
      TestingJobManagerMessages.WaitForAllVerticesToBeRunning(jobId), timeout)

    Await.ready(allRunning, timeout)

    // trigger checkpoint
    val result = Await.result(
      jobManagerGateway.ask(CheckpointRequest(jobId, checkpointRetentionPolicy), timeout), timeout)

    result match {
      case success: CheckpointRequestSuccess => success.path
      case fail: CheckpointRequestFailure => throw fail.cause
      case _ => throw new IllegalStateException("Trigger checkpoint failed")
    }
  }

  /**
    * This cancels the given job and waits until it has been completely removed from
    * the cluster.
    *
    * @param jobId identifying the job to cancel
    * @throws Exception if something goes wrong
    */
  @throws[Exception]
  def cancelJob(jobId: JobID): Unit = {
    if (getCurrentlyRunningJobsJava.contains(jobId)) {
      val jobManagerGateway = getLeaderGateway(timeout)
      val jobRemoved = jobManagerGateway.ask(NotifyWhenJobRemoved(jobId), timeout)
      val cancelFuture = jobManagerGateway.ask(new JobManagerMessages.CancelJob(jobId), timeout)
      val result = Await.result(cancelFuture, timeout)

      result match {
        case CancellationFailure(_, cause) =>
          throw new Exception("Cancellation failed", cause)
        case _ => // noop
      }

      // wait until the job has been removed
      Await.result(jobRemoved, timeout)
    }
    else throw new IllegalStateException("Job is not running")
  }
 }

object TestingCluster {
  val MAX_RESTART_DURATION = new FiniteDuration(2, TimeUnit.MINUTES)
}
