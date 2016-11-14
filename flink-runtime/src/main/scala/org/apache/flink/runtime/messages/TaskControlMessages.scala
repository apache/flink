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

import java.util

import org.apache.flink.runtime.deployment.{InputChannelDeploymentDescriptor, TaskDeploymentDescriptor}
import org.apache.flink.runtime.executiongraph.{ExecutionAttemptID, PartitionInfo}
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID
import org.apache.flink.runtime.taskmanager.TaskExecutionState

/**
 * A set of messages that control the deployment and the state of Tasks executed
 * on the TaskManager.
 */
object TaskMessages {

  /**
   * Marker trait for task messages.
   */
  trait TaskMessage

  // --------------------------------------------------------------------------
  //  Starting and stopping Tasks
  // --------------------------------------------------------------------------

  /**
   * Submits a task to the task manager. The result is to this message is a
   * [[TaskOperationResult]] message.
   *
   * @param tasks Descriptor which contains the information to start the task.
   */
  case class SubmitTask(tasks: TaskDeploymentDescriptor)
    extends TaskMessage with RequiresLeaderSessionID

  /**
   * Cancels the task associated with [[attemptID]]. The result is sent back to the sender as a
   * [[TaskOperationResult]] message.
   *
   * @param attemptID The task's execution attempt ID.
   */
  case class CancelTask(attemptID: ExecutionAttemptID)
    extends TaskMessage with RequiresLeaderSessionID

  /**
   * Stops the task associated with [[attemptID]]. The result is sent back to the sender as a
   * [[TaskOperationResult]] message.
   *
   * @param attemptID The task's execution attempt ID.
   */
  case class StopTask(attemptID: ExecutionAttemptID)
    extends TaskMessage with RequiresLeaderSessionID

  /**
   * Triggers a fail of specified task from the outside (as opposed to the task throwing
   * an exception itself) with the given exception as the cause.
   *
   * @param executionID The task's execution attempt ID.
   * @param cause The reason for the external failure.
   */
  case class FailTask(executionID: ExecutionAttemptID, cause: Throwable)
    extends TaskMessage

  /**
   * Notifies the TaskManager that the task has reached its final state,
   * either FINISHED, CANCELED, or FAILED.
   *
   * @param executionID The task's execution attempt ID.
   */
  case class TaskInFinalState(executionID: ExecutionAttemptID)
    extends TaskMessage


  // --------------------------------------------------------------------------
  //  Updates to Intermediate Results
  // --------------------------------------------------------------------------

  /**
   * Base class for messages that update the information about location of input partitions
   */
  abstract sealed class UpdatePartitionInfo extends TaskMessage with RequiresLeaderSessionID {
    def executionID: ExecutionAttemptID
  }

  /**
   *
   * @param executionID The task's execution attempt ID.
   * @param resultId The input reader to update.
   * @param partitionInfo The partition info update.
   */
  case class UpdateTaskSinglePartitionInfo(
      executionID: ExecutionAttemptID,
      resultId: IntermediateDataSetID,
      partitionInfo: InputChannelDeploymentDescriptor)
    extends UpdatePartitionInfo

  /**
   *
   * @param executionID The task's execution attempt ID.
   * @param partitionInfos List of input gates with channel descriptors to update.
   */
  case class UpdateTaskMultiplePartitionInfos(
      executionID: ExecutionAttemptID,
      partitionInfos: java.lang.Iterable[PartitionInfo])
    extends UpdatePartitionInfo

  /**
   * Fails (and releases) all intermediate result partitions identified by
   * [[executionID]] from the task manager.
   *
   * @param executionID The task's execution attempt ID.
   */
  case class FailIntermediateResultPartitions(executionID: ExecutionAttemptID)
    extends TaskMessage with RequiresLeaderSessionID


  // --------------------------------------------------------------------------
  //  Report Messages
  // --------------------------------------------------------------------------

  /**
   * Denotes a state change of a task at the JobManager. The update success is acknowledged by a
   * boolean value which is sent back to the sender.
   *
   * @param taskExecutionState The changed task state
   */
  case class UpdateTaskExecutionState(taskExecutionState: TaskExecutionState)
    extends TaskMessage with RequiresLeaderSessionID

  // --------------------------------------------------------------------------
  //  Utility Functions
  // --------------------------------------------------------------------------

  def createUpdateTaskMultiplePartitionInfos(
      executionID: ExecutionAttemptID,
      resultIDs: java.util.List[IntermediateDataSetID],
      partitionInfos: java.util.List[InputChannelDeploymentDescriptor])
    : UpdateTaskMultiplePartitionInfos = {

    require(resultIDs.size() == partitionInfos.size(),
      "ResultIDs must have the same length as partitionInfos.")

    val partitionInfoList = new util.ArrayList[PartitionInfo](resultIDs.size())

    for (i <- 0 until resultIDs.size()) {
      partitionInfoList.add(new PartitionInfo(resultIDs.get(i), partitionInfos.get(i)))
    }

    new UpdateTaskMultiplePartitionInfos(
      executionID,
      partitionInfoList)
  }
}
