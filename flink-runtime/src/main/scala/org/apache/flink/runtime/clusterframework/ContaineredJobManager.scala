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

package org.apache.flink.runtime.clusterframework

import java.util.concurrent.{Executor, ScheduledExecutorService}

import akka.actor.ActorRef
import org.apache.flink.api.common.JobID
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.blob.BlobServer
import org.apache.flink.runtime.checkpoint.CheckpointRecoveryFactory
import org.apache.flink.runtime.clusterframework.messages._
import org.apache.flink.runtime.execution.librarycache.BlobLibraryCacheManager
import org.apache.flink.runtime.executiongraph.restart.RestartStrategyFactory
import org.apache.flink.runtime.instance.InstanceManager
import org.apache.flink.runtime.jobgraph.JobStatus
import org.apache.flink.runtime.jobmanager.scheduler.{Scheduler => FlinkScheduler}
import org.apache.flink.runtime.jobmanager.{JobManager, SubmittedJobGraphStore}
import org.apache.flink.runtime.leaderelection.LeaderElectionService
import org.apache.flink.runtime.messages.Acknowledge
import org.apache.flink.runtime.messages.JobManagerMessages.{CurrentJobStatus, JobNotFound, RequestJobStatus}
import org.apache.flink.runtime.metrics.groups.JobManagerMetricGroup
import org.apache.flink.runtime.metrics.{MetricRegistryImpl => FlinkMetricRegistry}

import scala.concurrent.duration._
import scala.language.postfixOps


/** JobManager actor for execution on Yarn or Mesos.
  * It enriches the [[JobManager]] with additional messages
  * to start/administer/stop the session.
  *
  * @param flinkConfiguration Configuration object for the actor
  * @param futureExecutor Execution context which is used to execute concurrent tasks in the
  *                         [[org.apache.flink.runtime.executiongraph.ExecutionGraph]]
  * @param ioExecutor to execute blocking io operations
  * @param instanceManager Instance manager to manage the registered
  *                        [[org.apache.flink.runtime.taskmanager.TaskManager]]
  * @param scheduler Scheduler to schedule Flink jobs
  * @param blobServer Server instance to store BLOBs for the individual tasks
  * @param libraryCacheManager Manager to manage uploaded jar files
  * @param archive Archive for finished Flink jobs
  * @param restartStrategyFactory Restart strategy to be used in case of a job recovery
  * @param timeout Timeout for futures
  * @param leaderElectionService LeaderElectionService to participate in the leader election
  */
abstract class ContaineredJobManager(
    flinkConfiguration: Configuration,
    futureExecutor: ScheduledExecutorService,
    ioExecutor: Executor,
    instanceManager: InstanceManager,
    scheduler: FlinkScheduler,
    blobServer: BlobServer,
    libraryCacheManager: BlobLibraryCacheManager,
    archive: ActorRef,
    restartStrategyFactory: RestartStrategyFactory,
    timeout: FiniteDuration,
    leaderElectionService: LeaderElectionService,
    submittedJobGraphs : SubmittedJobGraphStore,
    checkpointRecoveryFactory : CheckpointRecoveryFactory,
    jobRecoveryTimeout: FiniteDuration,
    jobManagerMetricGroup: JobManagerMetricGroup,
    optRestAddress: Option[String])
  extends JobManager(
    flinkConfiguration,
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
    submittedJobGraphs,
    checkpointRecoveryFactory,
    jobRecoveryTimeout,
    jobManagerMetricGroup,
    optRestAddress) {

  val jobPollingInterval: FiniteDuration

  // indicates if this JM has been started in a dedicated (per-job) mode.
  var stopWhenJobFinished: JobID = null

  override def handleMessage: Receive = {
    handleContainerMessage orElse super.handleMessage
  }

  def handleContainerMessage: Receive = {

    case msg @ (_: RegisterInfoMessageListener | _: UnRegisterInfoMessageListener) =>
      // forward to ResourceManager
      currentResourceManager match {
        case Some(rm) =>
          // we forward the message
          rm.forward(decorateMessage(msg))
        case None =>
          // client has to try again
      }

    case msg: ShutdownClusterAfterJob =>
      val jobId = msg.jobId()
      log.info(s"ApplicationMaster will shut down session when job $jobId has finished.")
      stopWhenJobFinished = jobId
      // trigger regular job status messages (if this is a dedicated/per-job cluster)
      if (stopWhenJobFinished != null) {
        context.system.scheduler.schedule(0 seconds,
          jobPollingInterval,
          new Runnable {
            override def run(): Unit = {
              self ! decorateMessage(RequestJobStatus(stopWhenJobFinished))
            }
          }
        )(context.dispatcher)
      }

      sender() ! decorateMessage(Acknowledge.get())

    case msg: GetClusterStatus =>
      sender() ! decorateMessage(
        new GetClusterStatusResponse(
          instanceManager.getNumberOfRegisteredTaskManagers,
          instanceManager.getTotalNumberOfSlots)
      )

    case jnf: JobNotFound =>
      log.debug(s"Job with ID ${jnf.jobID} not found in JobManager")
      if (stopWhenJobFinished == null) {
        log.warn("The ApplicationMaster didn't expect to receive this message")
      }

    case jobStatus: CurrentJobStatus =>
      if (stopWhenJobFinished == null) {
        log.warn(s"Received job status $jobStatus which wasn't requested.")
      } else {
        if (stopWhenJobFinished != jobStatus.jobID) {
          log.warn(s"Received job status for job ${jobStatus.jobID} but expected status for " +
            s"job $stopWhenJobFinished")
        } else {
          if (jobStatus.status.isGloballyTerminalState) {
            log.info(s"Job with ID ${jobStatus.jobID} is in terminal state ${jobStatus.status}. " +
              s"Shutting down session")
            if (jobStatus.status == JobStatus.FINISHED) {
              self ! decorateMessage(
                new StopCluster(
                  ApplicationStatus.SUCCEEDED,
                  s"The monitored job with ID ${jobStatus.jobID} has finished.")
              )
            } else {
              self ! decorateMessage(
                new StopCluster(
                  ApplicationStatus.FAILED,
                  s"The monitored job with ID ${jobStatus.jobID} has failed to complete.")
              )
            }
          } else {
            log.debug(s"Monitored job with ID ${jobStatus.jobID} is in state ${jobStatus.status}")
          }
        }
      }
  }
}
