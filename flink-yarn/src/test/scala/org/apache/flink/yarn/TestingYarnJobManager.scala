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

import akka.actor.ActorRef
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.StreamingMode
import org.apache.flink.runtime.execution.librarycache.BlobLibraryCacheManager
import org.apache.flink.runtime.instance.InstanceManager
import org.apache.flink.runtime.jobmanager.scheduler.Scheduler
import org.apache.flink.runtime.leaderelection.LeaderElectionService

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration

class TestingYarnJobManager(
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
  extends YarnJobManager(
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
    leaderElectionService) {
}
