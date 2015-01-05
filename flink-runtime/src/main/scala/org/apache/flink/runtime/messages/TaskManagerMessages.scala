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

import org.apache.flink.core.io.InputSplit
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID
import org.apache.flink.runtime.instance.InstanceID 

object TaskManagerMessages {

  
  /**
   * Cancels the task associated with [[attemptID]]. The result is sent back to the sender as a
   * [[TaskOperationResult]] message.
   *
   * @param attemptID
   */
  case class CancelTask(attemptID: ExecutionAttemptID)

  /**
   * Submits a task to the task manager. The submission result is sent back to the sender as a
   * [[TaskOperationResult]] message.
   *
   * @param tasks task deployment descriptor which contains the task relevant information
   */
  case class SubmitTask(tasks: TaskDeploymentDescriptor)

  /**
   * Contains the next input split for a task. This message is a response to
   * [[org.apache.flink.runtime.messages.JobManagerMessages.RequestNextInputSplit]].
   *
   * @param inputSplit
   */
  case class NextInputSplit(inputSplit: InputSplit)

  /**
   * Unregisters a task identified by [[executionID]] from the task manager.
   *
   * @param executionID
   */
  case class UnregisterTask(executionID: ExecutionAttemptID)

  /**
   * Reports whether a task manager operation has been successful or not. This message will be
   * sent to the sender as a response to [[SubmitTask]] and [[CancelTask]].
   *
   * @param executionID identifying the respective task
   * @param success indicating whether the operation has been successful
   * @param description
   */
  case class TaskOperationResult(executionID: ExecutionAttemptID, success: Boolean,
                                 description: String = ""){
    def this(executionID: ExecutionAttemptID, success: Boolean) = this(executionID, success, "")
  }

  /**
   * Reports liveliness of an instance with [[instanceID]] to the
   * [[org.apache.flink.runtime.instance.InstanceManager]]. This message is sent to the job
   * manager which forwards it to the InstanceManager.
   *
   * @param instanceID
   */
  case class Heartbeat(instanceID: InstanceID)

  /**
   * Requests a notification from the task manager as soon as the task manager has been
   * registered at the job manager. Once the task manager is registered at the job manager a
   * [[RegisteredAtJobManager]] message will be sent to the sender.
   */
  case object NotifyWhenRegisteredAtJobManager

  /**
   * Acknowledges that the task manager has been successfully registered at the job manager. This
   * message is a response to [[NotifyWhenRegisteredAtJobManager]].
   */
  case object RegisteredAtJobManager

  /**
   * Registers the sender as task manager at the job manager.
   */
  case object RegisterAtJobManager

  /**
   * Makes the task manager sending a heartbeat message to the job manager.
   */
  case object SendHeartbeat

  /**
   * Logs the current memory usage as debug level output.
   */
  case object LogMemoryUsage

  /**
   * Fail the specified task externally
   *
   * @param executionID identifying the task to fail
   * @param cause reason for the external failure
   */
  case class FailTask(executionID: ExecutionAttemptID, cause: Throwable)
  
    // --------------------------------------------------------------------------
  // Utility methods to allow simpler case object access from Java
  // --------------------------------------------------------------------------
  
  def getNotifyWhenRegisteredAtJobManagerMessage() : AnyRef = {
    NotifyWhenRegisteredAtJobManager
  }
  
  def getRegisteredAtJobManagerMessage() : AnyRef = {
    RegisteredAtJobManager
  }
  
  def getRegisterAtJobManagerMessage() : AnyRef = {
    RegisterAtJobManager
  }

  def getSendHeartbeatMessage() : AnyRef = {
    SendHeartbeat
  }

  def getLogMemoryUsageMessage() : AnyRef = {
    RegisteredAtJobManager
  }
}
