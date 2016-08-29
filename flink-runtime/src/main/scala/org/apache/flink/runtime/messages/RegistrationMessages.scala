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

package org.apache.flink.runtime.messages

import java.util.UUID

import org.apache.flink.runtime.clusterframework.types.ResourceID
import org.apache.flink.runtime.instance.{HardwareDescription, InstanceID}
import org.apache.flink.runtime.taskmanager.TaskManagerLocation

import scala.concurrent.duration.{Deadline, FiniteDuration}

/**
 * A set of messages between TaskManager and JobManager to handle the
 * registration of the TaskManager at the JobManager.
 */
object RegistrationMessages {

  /**
   * Marker trait for registration messages.
   */
  trait RegistrationMessage extends RequiresLeaderSessionID {}

  /**
   * Triggers the TaskManager to attempt a registration at the JobManager.
   *
   * @param jobManagerURL Akka URL to the JobManager
   * @param timeout The timeout for the message. The next retry will double this timeout.
   * @param deadline Optional deadline until when the registration must be completed.
   * @param attempt The attempt number, for logging.
   * @param registrationRun UUID of the current registration run to filter out outdated runs
   */
  case class TriggerTaskManagerRegistration(
      jobManagerURL: String,
      timeout: FiniteDuration,
      deadline: Option[Deadline],
      attempt: Int,
      registrationRun: UUID)
    extends RegistrationMessage

  /**
   * Registers a task manager at the JobManager. A successful registration is acknowledged by
   * [[AcknowledgeRegistration]].
   *
   * @param connectionInfo The TaskManagers connection information.
   * @param resources The TaskManagers resources.
   * @param numberOfSlots The number of processing slots offered by the TaskManager.
   */
  case class RegisterTaskManager(
                                  resourceId: ResourceID,
                                  connectionInfo: TaskManagerLocation,
                                  resources: HardwareDescription,
                                  numberOfSlots: Int)
    extends RegistrationMessage

  /**
   * Denotes the successful registration of a task manager at the JobManager. This is the
   * response triggered by the [[RegisterTaskManager]] message when the JobManager has registered
   * the task manager with the resource manager.
   *
   * @param instanceID The instance ID under which the TaskManager is registered at the
   *                   JobManager.
   * @param blobPort The server port where the JobManager's BLOB service runs.
   */
  case class AcknowledgeRegistration(
      instanceID: InstanceID,
      blobPort: Int)
    extends RegistrationMessage

  /**
   * Denotes that the TaskManager has already been registered at the JobManager.
   *
   * @param instanceID The instance ID under which the TaskManager is registered.
   * @param blobPort The server port where the JobManager's BLOB service runs.
   */
  case class AlreadyRegistered(
      instanceID: InstanceID,
      blobPort: Int)
    extends RegistrationMessage

  /**
   * Denotes the unsuccessful registration of a task manager at the JobManager. This is the
   * response triggered by the [[RegisterTaskManager]] message.
   *
   * @param reason Reason why the task manager registration was refused
   */
  case class RefuseRegistration(reason: Throwable) extends RegistrationMessage

}
