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

import java.util.Map

import akka.actor.ActorRef
import org.apache.flink.api.common.JobID
import org.apache.flink.api.common.accumulators.Accumulator
import org.apache.flink.runtime.checkpoint.CheckpointRetentionPolicy
import org.apache.flink.runtime.checkpoint.savepoint.Savepoint
import org.apache.flink.runtime.executiongraph.AccessExecutionGraph
import org.apache.flink.runtime.instance.ActorGateway
import org.apache.flink.runtime.jobgraph.JobStatus
import org.apache.flink.runtime.messages.RequiresLeaderSessionID
import org.apache.flink.runtime.messages.checkpoint.AbstractCheckpointMessage
import org.apache.flink.util.OptionalFailure

object TestingJobManagerMessages {

  case class RequestExecutionGraph(jobID: JobID)

  sealed trait ResponseExecutionGraph {
    def jobID: JobID
  }

  case class ExecutionGraphFound(jobID: JobID, executionGraph: AccessExecutionGraph) extends
  ResponseExecutionGraph

  case class ExecutionGraphNotFound(jobID: JobID) extends ResponseExecutionGraph

  case class WaitForAllVerticesToBeRunning(jobID: JobID)
  case class WaitForAllVerticesToBeRunningOrFinished(jobID: JobID)
  case class AllVerticesRunning(jobID: JobID)

  case class NotifyWhenJobRemoved(jobID: JobID)

  case class RequestWorkingTaskManager(jobID: JobID)
  case class WorkingTaskManager(gatewayOption: Option[ActorGateway])

  case class NotifyWhenJobStatus(jobID: JobID, state: JobStatus)
  case class JobStatusIs(jobID: JobID, state: JobStatus)

  case object NotifyListeners

  case object WaitForBackgroundTasksToFinish

  case class NotifyWhenTaskManagerTerminated(taskManager: ActorRef)
  case class TaskManagerTerminated(taskManager: ActorRef)

  /**
    * Triggers a checkpoint for the specified job.
    *
    * This is not a subtype of [[AbstractCheckpointMessage]], because it is a
    * control-flow message, which is *not* part of the checkpointing mechanism
    * of triggering and acknowledging checkpoints.
    *
    * @param jobId The JobID of the job to trigger the savepoint for.
    */
  case class CheckpointRequest(
    jobId: JobID,
    retentionPolicy: CheckpointRetentionPolicy) extends RequiresLeaderSessionID

  /**
    * Response after a successful checkpoint trigger containing the savepoint path.
    *
    * @param jobId The job ID for which the savepoint was triggered.
    * @param path  The path of the savepoint.
    */
  case class CheckpointRequestSuccess(
    jobId: JobID,
    checkpointId: Long,
    path: String,
    triggerTime: Long)

  /**
    * Response after a failed checkpoint trigger containing the failure cause.
    *
    * @param jobId The job ID for which the savepoint was triggered.
    * @param cause The cause of the failure.
    */
  case class CheckpointRequestFailure(jobId: JobID, cause: Throwable)

  /**
   * Registers a listener to receive a message when accumulators changed.
   * The change must be explicitly triggered by the TestingTaskManager which can receive an
   * [[org.apache.flink.runtime.testingUtils.TestingTaskManagerMessages.AccumulatorsChanged]]
   * message by a task that changed the accumulators. This message is then
   * forwarded to the JobManager which will send the accumulators in the [[UpdatedAccumulators]]
   * message when the next Heartbeat occurs.
   */
  case class NotifyWhenAccumulatorChange(jobID: JobID)

  /**
   * Reports updated accumulators back to the listener.
   */
  case class UpdatedAccumulators(jobID: JobID,
    userAccumulators: Map[String, OptionalFailure[Accumulator[_,_]]])

  /** Notifies the sender when the [[TestingJobManager]] has been elected as the leader
   *
   */
  case object NotifyWhenLeader

  /**
    * Notifies the sender when the [[TestingJobManager]] receives new clients for jobs
    */
  case object NotifyWhenClientConnects
  /**
    * Notifies of client connect
    */
  case object ClientConnected
  /**
    * Notifies when the client has requested class loading information
    */
  case object ClassLoadingPropsDelivered

  /**
   * Registers to be notified by an [[org.apache.flink.runtime.messages.Messages.Acknowledge]]
   * message when at least numRegisteredTaskManager have registered at the JobManager.
   *
   * @param numRegisteredTaskManager minimum number of registered TMs before the sender is notified
   */
  case class NotifyWhenAtLeastNumTaskManagerAreRegistered(numRegisteredTaskManager: Int)

  /** Disables the post stop method of the [[TestingJobManager]].
    *
    * Only the leaderElectionService is stopped in the postStop method call to revoke the leadership
    */
  case object DisablePostStop

  /**
    * Requests a savepoint from the job manager.
    *
    * @param savepointPath The path of the savepoint to request.
    */
  case class RequestSavepoint(savepointPath: String)

  /**
    * Response to a savepoint request.
    *
    * @param savepoint The requested savepoint or null if none available.
    */
  case class ResponseSavepoint(savepoint: Savepoint)

  def getNotifyWhenLeader(): AnyRef = NotifyWhenLeader
  def getNotifyWhenClientConnects(): AnyRef = NotifyWhenClientConnects
  def getDisablePostStop(): AnyRef = DisablePostStop

  def getClientConnected(): AnyRef = ClientConnected
  def getClassLoadingPropsDelivered(): AnyRef = ClassLoadingPropsDelivered

  def getWaitForBackgroundTasksToFinish(): AnyRef = WaitForBackgroundTasksToFinish
}
