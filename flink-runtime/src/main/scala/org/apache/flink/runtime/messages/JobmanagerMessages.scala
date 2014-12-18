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
import org.apache.flink.runtime.executiongraph.{ExecutionAttemptID, ExecutionGraph}
import org.apache.flink.runtime.instance.{Instance, InstanceConnectionInfo}
import org.apache.flink.runtime.io.network.ConnectionInfoLookupResponse
import org.apache.flink.runtime.io.network.channels.ChannelID
import org.apache.flink.runtime.jobgraph.{JobStatus, JobVertexID, JobID, JobGraph}
import org.apache.flink.runtime.taskmanager.TaskExecutionState

/**
 * The job manager specific messages
 */
object JobManagerMessages {

  /**
   * Submits a job to the job manager. If [[registerForEvents]] is true,
   * then the sender will be registered as listener for the state change messages. If [[detach]]
   * is set to true, then the sender will detach from the job execution. Consequently,
   * he will not receive the job execution result [[JobResult]]. The submission result will be sent
   * back to the
   * sender as a [[SubmissionResponse]] message.
   *
   * @param jobGraph
   * @param registerForEvents if true, then register for state change events
   * @param detach if true, then detach from the job execution
   */
  case class SubmitJob(jobGraph: JobGraph, registerForEvents: Boolean = false,
                       detach: Boolean = false)

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
   * Looks up the connection information of a task being the source of a channel specified by
   * [[sourceChannelID]]. The caller denotes the instance information of the task requesting the
   * lookup information. The connection information is sent back to the sender as a
   * [[ConnectionInformation]] message.
   *
   * @param caller instance on which the task requesting the connection information runs
   * @param jobID
   * @param sourceChannelID denoting the channel whose producer shall be found
   */
  case class LookupConnectionInformation(caller: InstanceConnectionInfo, jobID: JobID,
                                         sourceChannelID: ChannelID)

  /**
   * Contains the connection lookup information of a lookup request triggered by
   * [[LookupConnectionInformation]].
   *
   * @param response
   */
  case class ConnectionInformation(response: ConnectionInfoLookupResponse)

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
   * Requests the final job status of the job with [[jobID]]. If the job has not been terminated
   * then the result is sent back upon termination of the job. The result is a
   * [[JobStatusResponse]] message.
   *
   * @param jobID
   */
  case class RequestFinalJobStatus(jobID: JobID)

  sealed trait JobResult{
    def jobID: JobID
  }

  /**
   * Denotes a successful job execution.
   *
   * @param jobID
   * @param runtime
   * @param accumulatorResults
   */
  case class JobResultSuccess(jobID: JobID, runtime: Long, accumulatorResults: java.util.Map[String,
    AnyRef]) extends JobResult {}

  /**
   * Denotes a cancellation of the job.
   * @param jobID
   * @param msg
   */
  case class JobResultCanceled(jobID: JobID, msg: String) extends JobResult

  /**
   * Denotes a failed job execution.
   * @param jobID
   * @param msg
   */
  case class JobResultFailed(jobID: JobID, msg:String) extends JobResult

  sealed trait SubmissionResponse{
    def jobID: JobID
  }

  /**
   * Denotes a successful job submission.
   * @param jobID
   */
  case class SubmissionSuccess(jobID: JobID) extends SubmissionResponse

  /**
   * Denotes a failed job submission. The cause of the failure is denoted by [[cause]].
   *
   * @param jobID
   * @param cause of the submission failure
   */
  case class SubmissionFailure(jobID: JobID, cause: Throwable) extends SubmissionResponse

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
   *
   * @param runningJobs
   */
  case class RunningJobs(runningJobs: Iterable[ExecutionGraph]) {
    def this() = this(Seq())
    def asJavaIterable: java.lang.Iterable[ExecutionGraph] = {
      import scala.collection.JavaConverters._
      runningJobs.asJava
    }
  }

  /**
   * Requests the execution graph of a specific job identified by [[jobID]]. The result is sent
   * back to the sender as a [[JobResponse]].
   * @param jobID
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
   * Requests the current state of the job manager
   */
  case object RequestJobManagerStatus

  /**
   * Response to RequestJobManagerStatus
   */
  sealed trait JobManagerStatus

  case object JobManagerStatusAlive extends JobManagerStatus

}
