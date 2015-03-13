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

import org.apache.flink.runtime.accumulators.AccumulatorEvent
import org.apache.flink.runtime.client.JobStatusMessage
import org.apache.flink.runtime.executiongraph.{ExecutionAttemptID, ExecutionGraph}
import org.apache.flink.runtime.instance.{InstanceID, Instance}
import org.apache.flink.runtime.jobgraph.{JobGraph, JobID, JobStatus, JobVertexID}
import org.apache.flink.runtime.taskmanager.TaskExecutionState

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
   * @param jobGraph
   * @param registerForEvents if true, then register for state change events
   */
  case class SubmitJob(jobGraph: JobGraph, registerForEvents: Boolean = false)

  /**
   * Cancels a job with the given [[jobID]] at the JobManager. The result of the cancellation is
   * sent back to the sender as a [[CancellationResponse]] message.
   *
   * @param jobID
   */
  case class CancelJob(jobID: JobID)

  /**
   * Denotes a state change of a task at the JobManager. The update success is acknowledged by a
   * boolean value which is sent back to the sender.
   *
   * @param taskExecutionState
   */
  case class UpdateTaskExecutionState(taskExecutionState: TaskExecutionState)

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
   * Notifies the [[org.apache.flink.runtime.jobmanager.JobManager]] about available data for a
   * produced partition.
   * <p>
   * There is a call to this method for each
   * [[org.apache.flink.runtime.executiongraph.ExecutionVertex]] instance once per produced
   * [[org.apache.flink.runtime.io.network.partition.IntermediateResultPartition]] instance,
   * either when first producing data (for pipelined executions) or when all data has been produced
   * (for staged executions).
   * <p>
   * The [[org.apache.flink.runtime.jobmanager.JobManager]] then can decide when to schedule the
   * partition consumers of the given session.
   *
   * @see [[org.apache.flink.runtime.io.network.partition.IntermediateResultPartition]]
   */
  case class ScheduleOrUpdateConsumers(jobId: JobID,
                                       executionId: ExecutionAttemptID,
                                       partitionIndex: Int)

  case class ConsumerNotificationResult(success: Boolean, error: Option[Throwable] = None)

  /**
   * Reports the accumulator results of the individual tasks to the job manager.
   *
   * @param accumulatorEvent
   */
  case class ReportAccumulatorResult(accumulatorEvent: AccumulatorEvent)

  /**
   * Requests the accumulator results of the job identified by [[jobID]] from the job manager.
   * The result is sent back to the sender as a [[AccumulatorResultsResponse]] message.
   *
   * @param jobID
   */
  case class RequestAccumulatorResults(jobID: JobID)

  sealed trait AccumulatorResultsResponse{
    val jobID: JobID
  }

  /**
   * Contains the retrieved accumulator results from the job manager. This response is triggered
   * by [[RequestAccumulatorResults]].
   *
   * @param jobID
   * @param results
   */
  case class AccumulatorResultsFound(jobID: JobID, results: Map[String,
    Object]) extends AccumulatorResultsResponse{
    def asJavaMap: java.util.Map[String, Object] = {
      import scala.collection.JavaConverters._
      results.asJava
    }
  }

  /**
   * Denotes that no accumulator results for [[jobID]] could be found at the job manager.
   * @param jobID
   */
  case class AccumulatorResultsNotFound(jobID: JobID) extends AccumulatorResultsResponse

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
   *
   * @param jobID
   * @param runtime
   * @param accumulatorResults
   */
  case class JobResultSuccess(jobID: JobID, runtime: Long,
                              accumulatorResults: java.util.Map[String, AnyRef]) {}

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
