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
import org.apache.flink.runtime.accumulators.AccumulatorRegistry
import org.apache.flink.runtime.checkpoint.CompletedCheckpoint
import org.apache.flink.runtime.executiongraph.{ExecutionAttemptID, ExecutionGraph}
import org.apache.flink.runtime.instance.ActorGateway
import org.apache.flink.runtime.jobgraph.JobStatus
import org.apache.flink.runtime.checkpoint.savepoint.Savepoint

object TestingJobManagerMessages {

  case class RequestExecutionGraph(jobID: JobID)

  sealed trait ResponseExecutionGraph {
    def jobID: JobID
  }

  case class ExecutionGraphFound(jobID: JobID, executionGraph: ExecutionGraph) extends
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

  case class NotifyWhenTaskManagerTerminated(taskManager: ActorRef)
  case class TaskManagerTerminated(taskManager: ActorRef)

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
    flinkAccumulators: Map[ExecutionAttemptID, Map[AccumulatorRegistry.Metric, Accumulator[_,_]]],
    userAccumulators: Map[String, Accumulator[_,_]])

  /** Notifies the sender when the [[TestingJobManager]] has been elected as the leader
   *
   */
  case object NotifyWhenLeader

  /**
    * Notifies the sender when the [[TestingJobManager]] receives new clients for jobs
    */
  case object NotifyWhenClientConnects

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

}
