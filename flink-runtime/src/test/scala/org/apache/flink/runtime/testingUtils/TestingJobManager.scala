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

import akka.actor.{Cancellable, Terminated, ActorRef}
import akka.pattern.pipe
import akka.pattern.ask
import org.apache.flink.api.common.JobID
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.leaderelection.LeaderElectionService
import org.apache.flink.runtime.{StreamingMode, FlinkActor}
import org.apache.flink.runtime.StreamingMode
import org.apache.flink.runtime.execution.ExecutionState
import org.apache.flink.runtime.execution.librarycache.BlobLibraryCacheManager
import org.apache.flink.runtime.instance.InstanceManager
import org.apache.flink.runtime.jobgraph.JobStatus
import org.apache.flink.runtime.jobmanager.JobManager
import org.apache.flink.runtime.jobmanager.scheduler.Scheduler
import org.apache.flink.runtime.messages.ExecutionGraphMessages.JobStatusChanged
import org.apache.flink.runtime.messages.JobManagerMessages.GrantLeadership
import org.apache.flink.runtime.messages.Messages.{Acknowledge, Disconnect}
import org.apache.flink.runtime.messages.TaskManagerMessages.Heartbeat
import org.apache.flink.runtime.testingUtils.TestingJobManagerMessages._
import org.apache.flink.runtime.testingUtils.TestingMessages.{CheckIfJobRemoved, Alive,
DisableDisconnect}
import org.apache.flink.runtime.testingUtils.TestingTaskManagerMessages.AccumulatorsChanged

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._

import scala.language.postfixOps

/** JobManager implementation extended by testing messages
  *
  * @param flinkConfiguration
  * @param executionContext
  * @param instanceManager
  * @param scheduler
  * @param libraryCacheManager
  * @param archive
  * @param defaultExecutionRetries
  * @param delayBetweenRetries
  * @param timeout
  * @param mode
  */
class TestingJobManager(
    flinkConfiguration: Configuration,
    executionContext: ExecutionContext,
    instanceManager: InstanceManager,
    scheduler: Scheduler,
    libraryCacheManager: BlobLibraryCacheManager,
    archive: ActorRef,
    defaultExecutionRetries: Int,
    delayBetweenRetries: Long,
    timeout: FiniteDuration,
    mode: StreamingMode,
    leaderElectionService: LeaderElectionService)
  extends JobManager(
    flinkConfiguration,
    executionContext,
    instanceManager,
    scheduler,
    libraryCacheManager,
    archive,
    defaultExecutionRetries,
    delayBetweenRetries,
    timeout,
    mode,
    leaderElectionService)
  with TestingJobManagerLike {}
