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

import java.util.{UUID, List => JavaList}

import org.apache.flink.runtime.clusterframework.ApplicationStatus
import org.apache.flink.runtime.messages.RequiresLeaderSessionID
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.yarn.api.records.{Container, ContainerStatus}

import scala.concurrent.duration.{Deadline, FiniteDuration}

object YarnMessages {

  case class ApplicationMasterStatus(numTaskManagers: Int, numSlots: Int)

  case class LocalStopYarnSession(status: ApplicationStatus, diagnostics: String)

  /**
    * Entry point to start a new YarnSession.
    * @param config The configuration to start the YarnSession with.
    * @param webServerPort The port of the web server to bind to.
    */
  case class StartYarnSession(config: Configuration, webServerPort: Int)

  /**
    * Callback from the async ResourceManager client when containers were allocated.
    * @param containers List of containers which were allocated.
    */
  case class YarnContainersAllocated(containers: JavaList[Container])

  /**
    * Callback from the async ResourceManager client when containers were completed.
    * @param statuses List of the completed containers' status.
    */
  case class YarnContainersCompleted(statuses: JavaList[ContainerStatus])

  /** Triggers the registration of the ApplicationClient to the YarnJobManager
    *
    * @param jobManagerAkkaURL JobManager's Akka URL
    * @param currentTimeout Timeout for next [[TriggerApplicationClientRegistration]] message
    * @param deadline Deadline for registration process to finish
    */
  case class TriggerApplicationClientRegistration(
      jobManagerAkkaURL: String,
      currentTimeout: FiniteDuration,
      deadline: Option[Deadline]) extends RequiresLeaderSessionID

  /** Registration message sent from the [[ApplicationClient]] to the [[YarnFlinkResourceManager]].
    * A successful registration is acknowledged with a [[AcknowledgeApplicationClientRegistration]]
    * message.
    */
  case object RegisterApplicationClient extends RequiresLeaderSessionID

  /** Response to a [[RegisterApplicationClient]] message which led to a successful registration
    * of the [[ApplicationClient]]
    */
  case object AcknowledgeApplicationClientRegistration extends RequiresLeaderSessionID

  /** Notification message that a new leader has been found. This message is sent from the
    * [[org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService]]
    *
    * @param jobManagerAkkaURL New leader's Akka URL
    * @param leaderSessionID New leader's session ID
    */
  case class JobManagerLeaderAddress(jobManagerAkkaURL: String, leaderSessionID: UUID)

  case object HeartbeatWithYarn
  case object CheckForUserCommand

  case object LocalGetYarnMessage // request new message

  def getLocalGetYarnMessage(): AnyRef = {
    LocalGetYarnMessage
  }

}
