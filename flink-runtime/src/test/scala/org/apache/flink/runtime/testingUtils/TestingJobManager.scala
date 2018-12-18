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

import java.util.concurrent.{Executor, ScheduledExecutorService}

import akka.actor.ActorRef
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.blob.BlobServer
import org.apache.flink.runtime.checkpoint.CheckpointRecoveryFactory
import org.apache.flink.runtime.execution.librarycache.BlobLibraryCacheManager
import org.apache.flink.runtime.executiongraph.restart.RestartStrategyFactory
import org.apache.flink.runtime.instance.InstanceManager
import org.apache.flink.runtime.jobmanager.scheduler.Scheduler
import org.apache.flink.runtime.jobmanager.{JobManager, SubmittedJobGraphStore}
import org.apache.flink.runtime.leaderelection.LeaderElectionService
import org.apache.flink.runtime.metrics.groups.JobManagerMetricGroup

import scala.concurrent.duration._
import scala.language.postfixOps

/**
 * JobManager implementation extended by testing messages
 */
class TestingJobManager(
    flinkConfiguration: Configuration,
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
    submittedJobGraphs : SubmittedJobGraphStore,
    checkpointRecoveryFactory : CheckpointRecoveryFactory,
    jobRecoveryTimeout : FiniteDuration,
    jobManagerMetricGroup : JobManagerMetricGroup,
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
    optRestAddress)
  with TestingJobManagerLike {}
