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

import org.apache.flink.api.common.JobID
import org.apache.flink.runtime.client.{SerializedJobExecutionResult, JobStatusMessage}
import org.apache.flink.runtime.executiongraph.{ExecutionAttemptID, ExecutionGraph}
import org.apache.flink.runtime.instance.{InstanceID, Instance}
import org.apache.flink.runtime.io.network.partition.ResultPartitionID
import org.apache.flink.runtime.jobgraph.{IntermediateDataSetID, JobGraph, JobStatus, JobVertexID}

import scala.collection.JavaConverters._

/**
 * The job manager specific actor messages
 */
object JobManagerMessages {

  /**
   * Submits a job to the job manager. If [[registerForEvents]] is true,
   * then the sender will be registered as listener for the state change messages.
   * The submission result will be sent back to the sender as a success message.
   *
   * @param jobGraph The job to be submitted to the JobManager
   * @param registerForEvents if true, then register for state change events
   */
  case class SubmitJob(jobGraph: JobGraph, registerForEvents: Boolean)

  /**
   * Cancels a job with the given [[jobID]] at the JobManager. The result of the cancellation is
   * sent back to the sender as a [[CancellationResponse]] message.
   *
   * @param jobID
   */
  case class CancelJob(jobID: JobID)

  /**
   * Requesting next input split for the
   * [[org.apache.flink.runtime.executiongraph.ExecutionJobVertex]]
   * of the job specified by [[jobID]]. The next input split is sent back to the sender as a
   * [[org.apache.flink.runtime.messages.TaskManagerMessages.NextInputSplit]] message.
   *
   * @param jobID
   * @param vertexID
   */
  case class RequestNextInputSplit(jobID: JobID, vertexID: JobVertexID, executionAttempt:
  ExecutionAttemptID)

  /**
   * Contains the next input split for a task. This message is a response to
   * [[org.apache.flink.runtime.messages.JobManagerMessages.RequestNextInputSplit]].
   *
   * @param splitData
   */
  case class NextInputSplit(splitData: Array[Byte])

  /**
   * Requests the current state of the partition.
   *
   * The state of a partition is currently bound to the state of the producing execution.
   * 
   * @param jobId The job ID of the job, which produces the partition.
   * @param partitionId The partition ID of the partition to request the state of.
   * @param taskExecutionId The execution attempt ID of the task requesting the partition state.
   * @param taskResultId The input gate ID of the task requesting the partition state.
   */
  case class RequestPartitionState(jobId: JobID,
                                   partitionId: ResultPartitionID,
                                   taskExecutionId: ExecutionAttemptID,
                                   taskResultId: IntermediateDataSetID)

  /**
   * Notifies the [[org.apache.flink.runtime.jobmanager.JobManager]] about available data for a
   * produced partition.
   * <p>
   * There is a call to this method for each
   * [[org.apache.flink.runtime.executiongraph.ExecutionVertex]] instance once per produced
   * [[org.apache.flink.runtime.io.network.partition.ResultPartition]] instance,
   * either when first producing data (for pipelined executions) or when all data has been produced
   * (for staged executions).
   * <p>
   * The [[org.apache.flink.runtime.jobmanager.JobManager]] then can decide when to schedule the
   * partition consumers of the given session.
   *
   * @see [[org.apache.flink.runtime.io.network.partition.ResultPartition]]
   */
  case class ScheduleOrUpdateConsumers(jobId: JobID, partitionId: ResultPartitionID)

  case class ConsumerNotificationResult(success: Boolean, error: Option[Throwable] = None)

  /**
   * Requests the current [[JobStatus]] of the job identified by [[jobID]]. This message triggers
   * as response a [[JobStatusResponse]] message.
   *
   * @param jobID
   */
  case class RequestJobStatus(jobID: JobID)

  sealed trait JobStatusResponse {
    def jobID: JobID
  }

  /**
   * Denotes the current [[JobStatus]] of the job with [[jobID]].
   *
   * @param jobID
   * @param status
   */
  case class CurrentJobStatus(jobID: JobID, status: JobStatus) extends JobStatusResponse

  /**
   * Requests the number of currently registered task manager at the job manager. The result is
   * sent back to the sender as an [[Int]].
   */
  case object RequestNumberRegisteredTaskManager

  /**
   * Requests the maximum number of slots available to the job manager. The result is sent back
   * to the sender as an [[Int]].
   */
  case object RequestTotalNumberOfSlots

  /**
   * Requests the port of the blob manager from the job manager. The result is sent back to the
   * sender as an [[Int]].
   */
  case object RequestBlobManagerPort

  /**
   * Denotes a successful job execution.
   */
  case class JobResultSuccess(result: SerializedJobExecutionResult)


  sealed trait CancellationResponse{
    def jobID: JobID
  }

  /**
   * Denotes a successful job cancellation
   * @param jobID
   */
  case class CancellationSuccess(jobID: JobID) extends CancellationResponse

  /**
   * Denotes a failed job cancellation
   * @param jobID
   * @param cause
   */
  case class CancellationFailure(jobID: JobID, cause: Throwable) extends CancellationResponse

  /**
   * Requests all currently running jobs from the job manager. This message triggers a
   * [[RunningJobs]] response.
   */
  case object RequestRunningJobs

  /**
   * This message is the response to the [[RequestRunningJobs]] message. It contains all
   * execution graphs of the currently running jobs.
   */
  case class RunningJobs(runningJobs: Iterable[ExecutionGraph]) {
    def this() = this(Seq())
    def asJavaIterable: java.lang.Iterable[ExecutionGraph] = {
      runningJobs.asJava
    }
  }

  /**
   * Requests the status of all currently running jobs from the job manager.
   * This message triggers a [[RunningJobsStatus]] response.
   */
  case object RequestRunningJobsStatus

  case class RunningJobsStatus(runningJobs: Iterable[JobStatusMessage]) {
    def this() = this(Seq())

    def getStatusMessages(): java.util.List[JobStatusMessage] = {
      new java.util.ArrayList[JobStatusMessage](runningJobs.asJavaCollection)
    }
  }

  /**
   * Requests the execution graph of a specific job identified by [[jobID]].
   * The result is sent back to the sender as a [[JobResponse]].
   */
  case class RequestJob(jobID: JobID)

  sealed trait JobResponse{
    def jobID: JobID
  }

  /**
   * Contains the [[executionGraph]] of a job with [[jobID]]. This is the response to
   * [[RequestJob]] if the job runs or is archived.
   *
   * @param jobID
   * @param executionGraph
   */
  case class JobFound(jobID: JobID, executionGraph: ExecutionGraph) extends JobResponse

  /**
   * Denotes that there is no job with [[jobID]] retrievable. This message can be the response of
   * [[RequestJob]] or [[RequestJobStatus]].
   *
   * @param jobID
   */
  case class JobNotFound(jobID: JobID) extends JobResponse with JobStatusResponse

  /**
   * Requests the instances of all registered task managers.
   */
  case object RequestRegisteredTaskManagers

  /**
   * Contains the [[Instance]] objects of all registered task managers. It is the response to the
   * message [[RequestRegisteredTaskManagers]].
   *
   * @param taskManagers
   */
  case class RegisteredTaskManagers(taskManagers: Iterable[Instance]){
    def asJavaIterable: java.lang.Iterable[Instance] = {
      import scala.collection.JavaConverters._
      taskManagers.asJava
    }

    def asJavaCollection: java.util.Collection[Instance] = {
      import scala.collection.JavaConverters._
      taskManagers.asJavaCollection
    }
  }

  /**
   * Requests stack trace messages of the task manager
   *
   * @param instanceID Instance ID of the task manager
   */
  case class RequestStackTrace(instanceID: InstanceID)

  /**
   * Requests the current state of the job manager
   */
  case object RequestJobManagerStatus

  /**
   * Response to RequestJobManagerStatus
   */
  sealed trait JobManagerStatus

  case object JobManagerStatusAlive extends JobManagerStatus

  // --------------------------------------------------------------------------
  // Utility methods to allow simpler case object access from Java
  // --------------------------------------------------------------------------
  
  def getRequestNumberRegisteredTaskManager : AnyRef = {
    RequestNumberRegisteredTaskManager
  }
  
  def getRequestTotalNumberOfSlots : AnyRef = {
    RequestTotalNumberOfSlots
  }
  
  def getRequestBlobManagerPort : AnyRef = {
    RequestBlobManagerPort
  }
  
  def getRequestRunningJobs : AnyRef = {
    RequestRunningJobs
  }

  def getRequestRunningJobsStatus : AnyRef = {
    RequestRunningJobsStatus
  }

  def getRequestRegisteredTaskManagers : AnyRef = {
    RequestRegisteredTaskManagers
  }
  
  def getRequestJobManagerStatus : AnyRef = {
    RequestJobManagerStatus
  }
  
  def getJobManagerStatusAlive : AnyRef = {
    JobManagerStatusAlive
  }
}
