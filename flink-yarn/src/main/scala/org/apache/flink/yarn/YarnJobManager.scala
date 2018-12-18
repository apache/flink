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

package org.apache.flink.yarn

import java.io.IOException
import java.util.concurrent.{Executor, ScheduledExecutorService, TimeUnit}

import akka.actor.ActorRef
import org.apache.flink.configuration.{Configuration => FlinkConfiguration}
import org.apache.flink.core.fs.Path
import org.apache.flink.runtime.blob.BlobServer
import org.apache.flink.runtime.checkpoint.CheckpointRecoveryFactory
import org.apache.flink.runtime.clusterframework.ContaineredJobManager
import org.apache.flink.runtime.clusterframework.messages.StopCluster
import org.apache.flink.runtime.execution.librarycache.BlobLibraryCacheManager
import org.apache.flink.runtime.executiongraph.restart.RestartStrategyFactory
import org.apache.flink.runtime.instance.InstanceManager
import org.apache.flink.runtime.jobmanager.scheduler.{Scheduler => FlinkScheduler}
import org.apache.flink.runtime.jobmanager.{JobManager, SubmittedJobGraphStore}
import org.apache.flink.runtime.leaderelection.LeaderElectionService
import org.apache.flink.runtime.metrics.MetricRegistryImpl
import org.apache.flink.runtime.metrics.groups.JobManagerMetricGroup
import org.apache.flink.yarn.configuration.YarnConfigOptions

import scala.concurrent.duration._
import scala.language.postfixOps

/** JobManager actor for execution on Yarn. It enriches the [[JobManager]] with additional messages
  * to start/administer/stop the Yarn session.
  *
  * @param flinkConfiguration Configuration object for the actor
  * @param futureExecutor Execution context which is used to execute concurrent tasks in the
  *                         [[org.apache.flink.runtime.executiongraph.ExecutionGraph]]
  * @param ioExecutor for blocking io operations
  * @param instanceManager Instance manager to manage the registered
  *                        [[org.apache.flink.runtime.taskmanager.TaskManager]]
  * @param scheduler Scheduler to schedule Flink jobs
  * @param blobServer BLOB store for file uploads
  * @param libraryCacheManager manages uploaded jar files and class paths
  * @param archive Archive for finished Flink jobs
  * @param restartStrategyFactory Restart strategy to be used in case of a job recovery
  * @param timeout Timeout for futures
  * @param leaderElectionService LeaderElectionService to participate in the leader election
  */
class YarnJobManager(
    flinkConfiguration: FlinkConfiguration,
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
  extends ContaineredJobManager(
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

  val DEFAULT_YARN_HEARTBEAT_DELAY: FiniteDuration = 5 seconds
  val YARN_HEARTBEAT_DELAY: FiniteDuration =
    FiniteDuration(
      flinkConfiguration.getInteger(YarnConfigOptions.HEARTBEAT_DELAY_SECONDS),
      TimeUnit.SECONDS)

  val yarnFilesPath: Option[String] = Option(System.getenv().get(YarnConfigKeys.FLINK_YARN_FILES))

  override val jobPollingInterval = YARN_HEARTBEAT_DELAY

  override def handleMessage: Receive = {
    handleYarnShutdown orElse super.handleMessage
  }

  private def handleYarnShutdown: Receive = {
    case msg: StopCluster =>
      // do global cleanup if the yarn files path has been set
      yarnFilesPath match {
        case Some(filePath) =>
          log.info(s"Deleting yarn application files under $filePath.")

          val path = new Path(filePath)

          try {
            val fs = path.getFileSystem

            if (!fs.delete(path, true)) {
              throw new IOException(s"Deleting yarn application files under $filePath " +
                s"was unsuccessful.")
            }
          } catch {
            case ioe: IOException =>
              log.warn(
                s"Could not properly delete yarn application files directory $filePath.",
                ioe)
          }
        case None =>
          log.debug("No yarn application files directory set. Therefore, cannot clean up " +
            "the data.")
      }

      super.handleMessage(msg)
  }
}
