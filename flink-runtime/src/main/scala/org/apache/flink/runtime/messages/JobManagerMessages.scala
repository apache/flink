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

import java.net.URL
import java.util.UUID

import akka.actor.ActorRef
import org.apache.flink.api.common.JobID
import org.apache.flink.runtime.akka.ListeningBehaviour
import org.apache.flink.runtime.blob.PermanentBlobKey
import org.apache.flink.runtime.client.{JobStatusMessage, SerializedJobExecutionResult}
import org.apache.flink.runtime.clusterframework.types.ResourceID
import org.apache.flink.runtime.executiongraph.{AccessExecutionGraph, ExecutionAttemptID, ExecutionGraph}
import org.apache.flink.runtime.instance.{Instance, InstanceID}
import org.apache.flink.runtime.io.network.partition.ResultPartitionID
import org.apache.flink.runtime.jobgraph.{IntermediateDataSetID, JobGraph, JobStatus, JobVertexID}
import org.apache.flink.runtime.jobmanager.SubmittedJobGraph
import org.apache.flink.runtime.messages.checkpoint.AbstractCheckpointMessage
import org.apache.flink.util.SerializedThrowable

import scala.collection.JavaConverters._

/**
 * The job manager specific actor messages
 */
object JobManagerMessages {

  /** Wrapper class for leader session messages. Leader session messages implement the
    * [[RequiresLeaderSessionID]] interface and have to be wrapped in a [[LeaderSessionMessage]],
    * which also contains the current leader session ID.
    *
    * @param leaderSessionID Current leader session ID
    * @param message [[RequiresLeaderSessionID]] message to be wrapped in a [[LeaderSessionMessage]]
    */
  case class LeaderSessionMessage(leaderSessionID: UUID, message: Any)

  /**
   * Submits a job to the job manager. Depending on the [[listeningBehaviour]],
   * the sender registers for different messages. If [[ListeningBehaviour.DETACHED]], then
   * it will only be informed whether the submission was successful or not. If
   * [[ListeningBehaviour.EXECUTION_RESULT]], then it will additionally receive the execution
   * result. If [[ListeningBehaviour.EXECUTION_RESULT_AND_STATE_CHANGES]], then it will additionally
   * receive the job status change notifications.
   *
   * The submission result will be sent back to the sender as a success message.
   *
   * @param jobGraph The job to be submitted to the JobManager
   * @param listeningBehaviour Specifies to what the sender wants to listen (detached, execution
   *                           result, execution result and state changes)
   */
  case class SubmitJob(
      jobGraph: JobGraph,
      listeningBehaviour: ListeningBehaviour)
    extends RequiresLeaderSessionID

  /**
    * Registers the sender of the message as the client for the provided job identifier.
    * This message is acknowledged by the JobManager with [[RegisterJobClientSuccess]]
    * or [[JobNotFound]] if the job was not running.
    * @param jobID The job id of the job
    * @param listeningBehaviour The types of updates which will be sent to the sender
    * after registration
    */
  case class RegisterJobClient(
      jobID: JobID,
      listeningBehaviour: ListeningBehaviour)
    extends RequiresLeaderSessionID

  /**
   * Triggers the recovery of the job with the given ID.
   *
   * @param jobId ID of the job to recover
   */
  case class RecoverJob(jobId: JobID) extends RequiresLeaderSessionID

  /**
   * Triggers the submission of the recovered job
   *
   * @param submittedJobGraph Contains the submitted JobGraph
   */
  case class RecoverSubmittedJob(submittedJobGraph: SubmittedJobGraph)
    extends RequiresLeaderSessionID

  /**
   * Triggers recovery of all available jobs.
   */
  case object RecoverAllJobs extends RequiresLeaderSessionID

  /**
   * Cancels a job with the given [[jobID]] at the JobManager. The result of the cancellation is
   * sent back to the sender as a [[CancellationResponse]] message.
   *
   * @param jobID
   */
  case class CancelJob(jobID: JobID) extends RequiresLeaderSessionID

  /**
    * Cancels the job with the given [[jobID]] at the JobManager. Before cancellation a savepoint
    * is triggered without any other checkpoints in between. The result of the cancellation is
    * the path of the triggered savepoint on success or an exception.
    *
    * @param jobID ID of the job to cancel
    * @param savepointDirectory Optional target directory for the savepoint.
    *                           If no target directory is specified here, the
    *                           cluster default is used.
    */
  case class CancelJobWithSavepoint(
      jobID: JobID,
      savepointDirectory: String = null)
    extends RequiresLeaderSessionID

  /**
   * Stops a (streaming) job with the given [[jobID]] at the JobManager. The result of
   * stopping is sent back to the sender as a [[StoppingResponse]] message.
   *
   * @param jobID
   */
  case class StopJob(jobID: JobID) extends RequiresLeaderSessionID

  /**
   * Requesting next input split for the
   * [[org.apache.flink.runtime.executiongraph.ExecutionJobVertex]]
   * of the job specified by [[jobID]]. The next input split is sent back to the sender as a
   * [[NextInputSplit]] message.
   *
   * @param jobID
   * @param vertexID
   */
  case class RequestNextInputSplit(
      jobID: JobID,
      vertexID: JobVertexID,
      executionAttempt: ExecutionAttemptID)
    extends RequiresLeaderSessionID

  /**
   * Contains the next input split for a task. This message is a response to
   * [[org.apache.flink.runtime.messages.JobManagerMessages.RequestNextInputSplit]].
   *
   * @param splitData
   */
  case class NextInputSplit(splitData: Array[Byte])

  /**
    * Requests the execution state of the execution producing a result partition.
    *
    * @param jobId                 ID of the job the partition belongs to.
    * @param intermediateDataSetId ID of the parent intermediate data set.
    * @param resultPartitionId     ID of the result partition to check. This
    *                              identifies the producing execution and
    *                              partition.
    */
  case class RequestPartitionProducerState(
      jobId: JobID,
      intermediateDataSetId: IntermediateDataSetID,
      resultPartitionId: ResultPartitionID)
    extends RequiresLeaderSessionID

  /**
   * Notifies the org.apache.flink.runtime.jobmanager.JobManager about available data for a
   * produced partition.
   * <p>
   * There is a call to this method for each
   * [[org.apache.flink.runtime.executiongraph.ExecutionVertex]] instance once per produced
   * [[org.apache.flink.runtime.io.network.partition.ResultPartition]] instance,
   * either when first producing data (for pipelined executions) or when all data has been produced
   * (for staged executions).
   * <p>
   * The org.apache.flink.runtime.jobmanager.JobManager then can decide when to schedule the
   * partition consumers of the given session.
   *
   * @see [[org.apache.flink.runtime.io.network.partition.ResultPartition]]
   */
  case class ScheduleOrUpdateConsumers(jobId: JobID, partitionId: ResultPartitionID)
    extends RequiresLeaderSessionID

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
    * Requests all entities necessary for reconstructing a job class loader
    * May respond with [[ClassloadingProps]] or [[JobNotFound]]
    * @param jobId The job id of the registered job
    */
  case class RequestClassloadingProps(jobId: JobID)

  /**
    * Response to [[RequestClassloadingProps]]
    * @param blobManagerPort The port of the blobManager
    * @param requiredJarFiles The blob keys of the required jar files
    * @param requiredClasspaths The urls of the required classpaths
    */
  case class ClassloadingProps(blobManagerPort: Integer,
                               requiredJarFiles: java.util.Collection[PermanentBlobKey],
                               requiredClasspaths: java.util.Collection[URL])

  /**
   * Requests the port of the blob manager from the job manager. The result is sent back to the
   * sender as an [[Int]].
   */
  case object RequestBlobManagerPort

  /** Requests the current leader session ID of the job manager. The result is sent back to the
    * sender as an [[ResponseLeaderSessionID]]
    */
  case object RequestLeaderSessionID

  /** Response to the [[RequestLeaderSessionID]] message.
    *
    * @param leaderSessionID
    */
  case class ResponseLeaderSessionID(leaderSessionID: UUID)

  /**
   * Denotes a successful job submission.
   * @param jobId Ths job's ID.
   */
  case class JobSubmitSuccess(jobId: JobID)

  /**
    * Denotes a successful registration of a JobClientActor for a running job
    * @param jobId The job id of the registered job
    */
  case class RegisterJobClientSuccess(jobId: JobID)

  /**
    * Denotes messages which contain the result of a completed job execution
    */
  sealed trait JobResultMessage

  /**
   * Denotes a successful job execution.
   * @param result The result of the job execution, in serialized form.
   */
  case class JobResultSuccess(result: SerializedJobExecutionResult) extends JobResultMessage

  /**
   * Denotes an unsuccessful job execution.
   * @param cause The exception that caused the job to fail, in serialized form.
   */
  case class JobResultFailure(cause: SerializedThrowable) extends JobResultMessage


  sealed trait CancellationResponse{
    def jobID: JobID
  }

  /**
   * Denotes a successful job cancellation
   * @param jobID
   */
  case class CancellationSuccess(
    jobID: JobID,
    savepointPath: String = null) extends CancellationResponse

  /**
   * Denotes a failed job cancellation
   * @param jobID
   * @param cause
   */
  case class CancellationFailure(jobID: JobID, cause: Throwable) extends CancellationResponse

  sealed trait StoppingResponse {
    def jobID: JobID
  }

  /**
   * Denotes a successful (streaming) job stopping
   * @param jobID
   */
  case class StoppingSuccess(jobID: JobID) extends StoppingResponse

  /**
   * Denotes a failed (streaming) job stopping
   * @param jobID
   * @param cause
   */
  case class StoppingFailure(jobID: JobID, cause: Throwable) extends StoppingResponse

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
  case class JobFound(jobID: JobID, executionGraph: AccessExecutionGraph) extends JobResponse

  /**
   * Denotes that there is no job with [[jobID]] retrievable. This message can be the response of
   * [[RequestJob]], [[RequestJobStatus]] or [[RegisterJobClient]].
   *
   * @param jobID
   */
  case class JobNotFound(jobID: JobID) extends JobResponse with JobStatusResponse

  /** Triggers the removal of the job with the given job ID
    *
    * @param jobID
    * @param removeJobFromStateBackend true if the job has properly finished
    */
  case class RemoveJob(jobID: JobID, removeJobFromStateBackend: Boolean = true)
    extends RequiresLeaderSessionID

  /**
   * Removes the job belonging to the job identifier from the job manager and archives it.
   * @param jobID The job identifier
   */
  case class RemoveCachedJob(jobID: JobID)

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
   * Requests the [[Instance]] object of the task manager with the given instance ID
   *
   * @param resourceId identifying the TaskManager which shall be retrieved
   */
  case class RequestTaskManagerInstance(resourceId: ResourceID)

  /**
   * Returns the [[Instance]] object of the requested task manager. This is in response to
   * [[RequestTaskManagerInstance]]
   */
  case class TaskManagerInstance(instance: Option[Instance])

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

  /** Grants leadership to the receiver. The message contains the new leader session id.
    *
     * @param leaderSessionID
    */
  case class GrantLeadership(leaderSessionID: Option[UUID])

  /** Revokes leadership of the receiver.
    */
  case object RevokeLeadership

  /** Requests the ActorRef of the archiver */
  case object RequestArchive

  /** Response containing the ActorRef of the archiver */
  case class ResponseArchive(actor: ActorRef)

  /** Request for the JobManager's REST endpoint address */
  case object RequestRestAddress

  /**
    * Triggers a savepoint for the specified job.
    *
    * This is not a subtype of [[AbstractCheckpointMessage]], because it is a
    * control-flow message, which is *not* part of the checkpointing mechanism
    * of triggering and acknowledging checkpoints.
    *
    * @param jobId The JobID of the job to trigger the savepoint for.
    * @param savepointDirectory Optional target directory
    */
  case class TriggerSavepoint(
      jobId: JobID,
      savepointDirectory : Option[String] = Option.empty) extends RequiresLeaderSessionID

  /**
    * Response after a successful savepoint trigger containing the savepoint path.
    *
    * @param jobId The job ID for which the savepoint was triggered.
    * @param savepointPath The path of the savepoint.
    */
  case class TriggerSavepointSuccess(
    jobId: JobID,
    checkpointId: Long,
    savepointPath: String,
    triggerTime: Long
  )

  /**
    * Response after a failed savepoint trigger containing the failure cause.
    *
    * @param jobId The job ID for which the savepoint was triggered.
    * @param cause The cause of the failure.
    */
  case class TriggerSavepointFailure(jobId: JobID, cause: Throwable)

  /**
    * Disposes a savepoint.
    *
    * @param savepointPath The path of the savepoint to dispose.
    */
  case class DisposeSavepoint(
      savepointPath: String)
    extends RequiresLeaderSessionID

  /** Response after a successful savepoint dispose. */
  case object DisposeSavepointSuccess

  /**
    * Response after a failed savepoint dispose containing the failure cause.
    *
    * @param cause The cause of the failure.
    */
  case class DisposeSavepointFailure(cause: Throwable)

  // --------------------------------------------------------------------------
  // Utility methods to allow simpler case object access from Java
  // --------------------------------------------------------------------------

  def getRequestJobStatus(jobId : JobID) : AnyRef = {
    RequestJobStatus(jobId)
  }

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

  def getRequestLeaderSessionID: AnyRef = {
    RequestLeaderSessionID
  }

  def getRequestArchive: AnyRef = {
    RequestArchive
  }

  def getRecoverAllJobs: AnyRef = {
    RecoverAllJobs
  }

  def getRequestRestAddress: AnyRef = {
    RequestRestAddress
  }

  def getDisposeSavepointSuccess: AnyRef = {
    DisposeSavepointSuccess
  }
}
