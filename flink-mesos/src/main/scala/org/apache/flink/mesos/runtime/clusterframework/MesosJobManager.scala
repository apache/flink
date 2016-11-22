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

package org.apache.flink.mesos.runtime.clusterframework

import java.util.concurrent.Executor

import akka.actor.ActorRef
import org.apache.flink.configuration.{Configuration => FlinkConfiguration}
import org.apache.flink.runtime.checkpoint.CheckpointRecoveryFactory
import org.apache.flink.runtime.clusterframework.ContaineredJobManager
import org.apache.flink.runtime.execution.librarycache.BlobLibraryCacheManager
import org.apache.flink.runtime.executiongraph.restart.RestartStrategyFactory
import org.apache.flink.runtime.instance.InstanceManager
import org.apache.flink.runtime.jobmanager.SubmittedJobGraphStore
import org.apache.flink.runtime.jobmanager.scheduler.{Scheduler => FlinkScheduler}
import org.apache.flink.runtime.leaderelection.LeaderElectionService
import org.apache.flink.runtime.metrics.{MetricRegistry => FlinkMetricRegistry}

import scala.concurrent.duration._

/** JobManager actor for execution on Mesos. .
  *
  * @param flinkConfiguration Configuration object for the actor
  * @param executor Execution context which is used to execute concurrent tasks in the
  *                         [[org.apache.flink.runtime.executiongraph.ExecutionGraph]]
  * @param instanceManager Instance manager to manage the registered
  *                        [[org.apache.flink.runtime.taskmanager.TaskManager]]
  * @param scheduler Scheduler to schedule Flink jobs
  * @param libraryCacheManager Manager to manage uploaded jar files
  * @param archive Archive for finished Flink jobs
  * @param restartStrategyFactory Restart strategy to be used in case of a job recovery
  * @param timeout Timeout for futures
  * @param leaderElectionService LeaderElectionService to participate in the leader election
  */
class MesosJobManager(flinkConfiguration: FlinkConfiguration,
                      executor: Executor,
                      instanceManager: InstanceManager,
                      scheduler: FlinkScheduler,
                      libraryCacheManager: BlobLibraryCacheManager,
                      archive: ActorRef,
                      restartStrategyFactory: RestartStrategyFactory,
                      timeout: FiniteDuration,
                      leaderElectionService: LeaderElectionService,
                      submittedJobGraphs : SubmittedJobGraphStore,
                      checkpointRecoveryFactory : CheckpointRecoveryFactory,
                      jobRecoveryTimeout: FiniteDuration,
                      metricsRegistry: Option[FlinkMetricRegistry])
  extends ContaineredJobManager(
    flinkConfiguration,
    executor,
    instanceManager,
    scheduler,
    libraryCacheManager,
    archive,
    restartStrategyFactory,
    timeout,
    leaderElectionService,
    submittedJobGraphs,
    checkpointRecoveryFactory,
    jobRecoveryTimeout,
    metricsRegistry) {

  val jobPollingInterval: FiniteDuration = 5 seconds

}
