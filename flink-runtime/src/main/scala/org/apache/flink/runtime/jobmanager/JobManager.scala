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

package org.apache.flink.runtime.jobmanager

import java.io.{File, IOException}
import java.lang.reflect.{InvocationTargetException, Constructor}
import java.net.InetSocketAddress
import java.util.{UUID, Collections}

import akka.actor.Status.{Failure, Success}
import akka.actor._
import _root_.akka.pattern.ask

import grizzled.slf4j.Logger

import org.apache.flink.api.common.{ExecutionConfig, JobID}
import org.apache.flink.configuration.{ConfigConstants, Configuration, GlobalConfiguration}
import org.apache.flink.core.io.InputSplitAssigner
import org.apache.flink.runtime.accumulators.AccumulatorSnapshot
import org.apache.flink.runtime.blob.BlobServer
import org.apache.flink.runtime.client._
import org.apache.flink.runtime.executiongraph.{ExecutionGraph, ExecutionJobVertex}
import org.apache.flink.runtime.jobmanager.web.WebInfoServer
import org.apache.flink.runtime.messages.ArchiveMessages.ArchiveExecutionGraph
import org.apache.flink.runtime.messages.ExecutionGraphMessages.JobStatusChanged
import org.apache.flink.runtime.messages.Messages.{Acknowledge, Disconnect}
import org.apache.flink.runtime.messages.TaskMessages.{PartitionState, UpdateTaskExecutionState}
import org.apache.flink.runtime.messages.accumulators._
import org.apache.flink.runtime.messages.checkpoint.{AbstractCheckpointMessage, AcknowledgeCheckpoint}
import org.apache.flink.runtime.messages.webmonitor._
import org.apache.flink.runtime.process.ProcessReaper
import org.apache.flink.runtime.security.SecurityUtils
import org.apache.flink.runtime.security.SecurityUtils.FlinkSecuredRunner
import org.apache.flink.runtime.taskmanager.TaskManager
import org.apache.flink.runtime.util.ZooKeeperUtil
import org.apache.flink.runtime.util.{SerializedValue, EnvironmentInformation}
import org.apache.flink.runtime.webmonitor.WebMonitor
import org.apache.flink.runtime.{FlinkActor, StreamingMode, LeaderSessionMessages}
import org.apache.flink.runtime.{LogMessages}
import org.apache.flink.runtime.akka.AkkaUtils
import org.apache.flink.runtime.execution.librarycache.BlobLibraryCacheManager
import org.apache.flink.runtime.instance.{ActorGateway, AkkaActorGateway, InstanceManager}
import org.apache.flink.runtime.jobgraph.{JobVertexID, JobGraph, JobStatus}
import org.apache.flink.runtime.jobmanager.scheduler.{Scheduler => FlinkScheduler}
import org.apache.flink.runtime.messages.JobManagerMessages._
import org.apache.flink.runtime.messages.RegistrationMessages._
import org.apache.flink.runtime.messages.TaskManagerMessages.{SendStackTrace, Heartbeat}
import org.apache.flink.util.{ExceptionUtils, InstantiationUtil}

import _root_.akka.pattern.ask
import scala.concurrent._
import scala.concurrent.duration._
import scala.concurrent.forkjoin.ForkJoinPool
import scala.language.postfixOps
import scala.collection.JavaConverters._
import scala.collection.JavaConversions._

/**
 * The job manager is responsible for receiving Flink jobs, scheduling the tasks, gathering the
 * job status and managing the task managers. It is realized as an actor and receives amongst others
 * the following messages:
 *
 *  - [[RegisterTaskManager]] is sent by a TaskManager which wants to register at the job manager.
 *  A successful registration at the instance manager is acknowledged by [[AcknowledgeRegistration]]
 *
 *  - [[SubmitJob]] is sent by a client which wants to submit a job to the system. The submit
 *  message contains the job description in the form of the JobGraph. The JobGraph is appended to
 *  the ExecutionGraph and the corresponding JobExecutionVertices are scheduled for execution on
 *  the TaskManagers.
 *
 *  - [[CancelJob]] requests to cancel the job with the specified jobID. A successful cancellation
 *  is indicated by [[CancellationSuccess]] and a failure by [[CancellationFailure]]
 *
 * - [[UpdateTaskExecutionState]] is sent by a TaskManager to update the state of an
     ExecutionVertex contained in the [[ExecutionGraph]].
 * A successful update is acknowledged by true and otherwise false.
 *
 * - [[RequestNextInputSplit]] requests the next input split for a running task on a
 * [[TaskManager]]. The assigned input split or null is sent to the sender in the form of the
 * message [[NextInputSplit]].
 *
 * - [[JobStatusChanged]] indicates that the status of job (RUNNING, CANCELING, FINISHED, etc.) has
 * changed. This message is sent by the ExecutionGraph.
 */
class JobManager(
    protected val flinkConfiguration: Configuration,
    protected val executionContext: ExecutionContext,
    protected val instanceManager: InstanceManager,
    protected val scheduler: FlinkScheduler,
    protected val libraryCacheManager: BlobLibraryCacheManager,
    protected val archive: ActorRef,
    protected val defaultExecutionRetries: Int,
    protected val delayBetweenRetries: Long,
    protected val timeout: FiniteDuration,
    protected val mode: StreamingMode)
  extends FlinkActor
  with LeaderSessionMessages // order of the mixin is important, we want filtering after logging
  with LogMessages // order of the mixin is important, we want first logging
  {

  override val log = Logger(getClass)

  /** List of current jobs running jobs */
  protected val currentJobs = scala.collection.mutable.HashMap[JobID, (ExecutionGraph, JobInfo)]()

  override val leaderSessionID = Some(UUID.randomUUID())

  /**
   * Run when the job manager is started. Simply logs an informational message.
   */
  override def preStart(): Unit = {
    log.info(s"Starting JobManager at ${self.path.toSerializationFormat}.")
  }

  override def postStop(): Unit = {
    log.info(s"Stopping JobManager ${self.path.toSerializationFormat}.")

    // disconnect the registered task managers
    instanceManager.getAllRegisteredInstances.asScala.foreach {
      _.getActorGateway().tell(Disconnect("JobManager is shutting down"))
    }

    archive ! decorateMessage(PoisonPill)

    for((e,_) <- currentJobs.values) {
      e.fail(new Exception("The JobManager is shutting down."))
    }

    instanceManager.shutdown()
    scheduler.shutdown()

    try {
      libraryCacheManager.shutdown()
    } catch {
      case e: IOException => log.error("Could not properly shutdown the library cache manager.", e)
    }

    log.debug(s"Job manager ${self.path} is completely stopped.")
  }

  /**
   * Central work method of the JobManager actor. Receives messages and reacts to them.
   *
   * @return
   */
  override def handleMessage: Receive = {

    case RegisterTaskManager(
      registrationSessionID,
      taskManager,
      connectionInfo,
      hardwareInformation,
      numberOfSlots) =>

      if (instanceManager.isRegistered(taskManager)) {
        val instanceID = instanceManager.getRegisteredInstance(taskManager).getId

        // IMPORTANT: Send the response to the "sender", which is not the
        //            TaskManager actor, but the ask future!
        sender() ! decorateMessage(
          AlreadyRegistered(
            registrationSessionID,
            leaderSessionID.get,
            self,
            instanceID,
            libraryCacheManager.getBlobServerPort)
        )
      }
      else {
        try {
          val instanceID = instanceManager.registerTaskManager(
            taskManager,
            connectionInfo,
            hardwareInformation,
            numberOfSlots,
            leaderSessionID)

          // IMPORTANT: Send the response to the "sender", which is not the
          //            TaskManager actor, but the ask future!
          sender() ! decorateMessage(
            AcknowledgeRegistration(
              registrationSessionID,
              leaderSessionID.get,
              self,
              instanceID,
              libraryCacheManager.getBlobServerPort)
          )

          // to be notified when the taskManager is no longer reachable
          context.watch(taskManager)
        }
        catch {
          // registerTaskManager throws an IllegalStateException if it is already shut down
          // let the actor crash and restart itself in this case
          case e: Exception =>
            log.error("Failed to register TaskManager at instance manager", e)

            // IMPORTANT: Send the response to the "sender", which is not the
            //            TaskManager actor, but the ask future!
            sender() ! decorateMessage(
              RefuseRegistration(
                registrationSessionID,
                ExceptionUtils.stringifyException(e))
            )
        }
      }

    case RequestNumberRegisteredTaskManager =>
      sender ! decorateMessage(instanceManager.getNumberOfRegisteredTaskManagers)

    case RequestTotalNumberOfSlots =>
      sender ! decorateMessage(instanceManager.getTotalNumberOfSlots)

    case SubmitJob(jobGraph, listen) =>
      submitJob(jobGraph, listenToEvents = listen)

    case CancelJob(jobID) =>
      log.info(s"Trying to cancel job with ID $jobID.")

      currentJobs.get(jobID) match {
        case Some((executionGraph, _)) =>
          // execute the cancellation asynchronously
          Future {
            executionGraph.cancel()
          }(context.dispatcher)

          sender ! decorateMessage(CancellationSuccess(jobID))
        case None =>
          log.info(s"No job found with ID $jobID.")
          sender ! decorateMessage(
            CancellationFailure(
              jobID,
              new IllegalArgumentException(s"No job found with ID $jobID."))
          )
      }

    case UpdateTaskExecutionState(taskExecutionState) =>
      if (taskExecutionState == null) {
        sender ! decorateMessage(false)
      } else {
        currentJobs.get(taskExecutionState.getJobID) match {
          case Some((executionGraph, _)) =>
            val originalSender = sender()

            Future {
              val result = executionGraph.updateState(taskExecutionState)
              originalSender ! decorateMessage(result)
            }(context.dispatcher)

          case None => log.error("Cannot find execution graph for ID " +
            s"${taskExecutionState.getJobID} to change state to " +
            s"${taskExecutionState.getExecutionState}.")
            sender ! decorateMessage(false)
        }
      }

    case RequestNextInputSplit(jobID, vertexID, executionAttempt) =>
      val serializedInputSplit = currentJobs.get(jobID) match {
        case Some((executionGraph,_)) =>
          val execution = executionGraph.getRegisteredExecutions.get(executionAttempt)

          if (execution == null) {
            log.error(s"Can not find Execution for attempt $executionAttempt.")
            null
          } else {
            val slot = execution.getAssignedResource
            val taskId = execution.getVertex.getParallelSubtaskIndex

            val host = if (slot != null) {
              slot.getInstance().getInstanceConnectionInfo.getHostname
            } else {
              null
            }

            executionGraph.getJobVertex(vertexID) match {
              case vertex: ExecutionJobVertex => vertex.getSplitAssigner match {
                case splitAssigner: InputSplitAssigner =>
                  val nextInputSplit = splitAssigner.getNextInputSplit(host, taskId)

                  log.debug(s"Send next input split $nextInputSplit.")

                  try {
                    InstantiationUtil.serializeObject(nextInputSplit)
                  } catch {
                    case ex: Exception =>
                      log.error(s"Could not serialize the next input split of " +
                        s"class ${nextInputSplit.getClass}.", ex)
                      vertex.fail(new RuntimeException("Could not serialize the next input split " +
                        "of class " + nextInputSplit.getClass + ".", ex))
                      null
                  }

                case _ =>
                  log.error(s"No InputSplitAssigner for vertex ID $vertexID.")
                  null
              }
              case _ =>
                log.error(s"Cannot find execution vertex for vertex ID $vertexID.")
                null
          }
        }
        case None =>
          log.error(s"Cannot find execution graph for job ID $jobID.")
          null
      }

      sender ! decorateMessage(NextInputSplit(serializedInputSplit))

    case checkpointMessage : AbstractCheckpointMessage =>
      handleCheckpointMessage(checkpointMessage)

    case JobStatusChanged(jobID, newJobStatus, timeStamp, error) =>
      currentJobs.get(jobID) match {
        case Some((executionGraph, jobInfo)) => executionGraph.getJobName

          log.info(s"Status of job $jobID (${executionGraph.getJobName}) changed to $newJobStatus.",
            error)

          if (newJobStatus.isTerminalState) {
            jobInfo.end = timeStamp

            // is the client waiting for the job result?
            newJobStatus match {
              case JobStatus.FINISHED =>
                val accumulatorResults: java.util.Map[String, SerializedValue[AnyRef]] = try {
                  executionGraph.getAccumulatorsSerialized
                } catch {
                  case e: Exception =>
                    log.error(s"Cannot fetch serialized accumulators for job $jobID", e)
                    Collections.emptyMap()
                }
                val result = new SerializedJobExecutionResult(jobID, jobInfo.duration,
                                                              accumulatorResults)
                jobInfo.client ! decorateMessage(JobResultSuccess(result))

              case JobStatus.CANCELED =>
                jobInfo.client ! decorateMessage(
                  Failure(
                    new JobCancellationException(
                      jobID,
                    "Job was cancelled.", error)
                  )
                )

              case JobStatus.FAILED =>
                jobInfo.client ! decorateMessage(
                  Failure(
                    new JobExecutionException(
                      jobID,
                      "Job execution failed.",
                      error)
                  )
                )

              case x =>
                val exception = new JobExecutionException(jobID, s"$x is not a " +
                  "terminal state.")
                jobInfo.client ! decorateMessage(Failure(exception))
                throw exception
            }

            removeJob(jobID)

          }
        case None =>
          removeJob(jobID)
      }

    case ScheduleOrUpdateConsumers(jobId, partitionId) =>
      currentJobs.get(jobId) match {
        case Some((executionGraph, _)) =>
          sender ! decorateMessage(Acknowledge)
          executionGraph.scheduleOrUpdateConsumers(partitionId)
        case None =>
          log.error(s"Cannot find execution graph for job ID $jobId to schedule or update " +
            s"consumers.")
          sender ! decorateMessage(
            Failure(
              new IllegalStateException("Cannot find execution graph for job ID " +
                s"$jobId to schedule or update consumers.")
            )
          )
      }

    case RequestPartitionState(jobId, partitionId, taskExecutionId, taskResultId) =>
      val state = currentJobs.get(jobId) match {
        case Some((executionGraph, _)) =>
          val execution = executionGraph.getRegisteredExecutions.get(partitionId.getProducerId)

          if (execution != null) execution.getState else null
        case None =>
          // Nothing to do. This is not an error, because the request is received when a sending
          // task fails during a remote partition request.
          log.debug(s"Cannot find execution graph for job $jobId.")

          null
      }

      sender ! decorateMessage(
        PartitionState(
          taskExecutionId,
          taskResultId,
          partitionId.getPartitionId,
          state)
      )

    case RequestJobStatus(jobID) =>
      currentJobs.get(jobID) match {
        case Some((executionGraph,_)) =>
          sender ! decorateMessage(CurrentJobStatus(jobID, executionGraph.getState))
        case None =>
          // check the archive
          archive forward decorateMessage(RequestJobStatus(jobID))
      }

    case RequestRunningJobs =>
      val executionGraphs = currentJobs map {
        case (_, (eg, jobInfo)) => eg
      }

      sender ! decorateMessage(RunningJobs(executionGraphs))

    case RequestRunningJobsStatus =>
      try {
        val jobs = currentJobs map {
          case (_, (eg, _)) =>
            new JobStatusMessage(
              eg.getJobID,
              eg.getJobName,
              eg.getState,
              eg.getStatusTimestamp(JobStatus.CREATED)
            )
        }

        sender ! decorateMessage(RunningJobsStatus(jobs))
      }
      catch {
        case t: Throwable => log.error("Exception while responding to RequestRunningJobsStatus", t)
      }

    case RequestJob(jobID) =>
      currentJobs.get(jobID) match {
        case Some((eg, _)) => sender ! decorateMessage(JobFound(jobID, eg))
        case None =>
          // check the archive
          archive forward decorateMessage(RequestJob(jobID))
      }

    case RequestBlobManagerPort =>
      sender ! decorateMessage(libraryCacheManager.getBlobServerPort)

    case RequestRegisteredTaskManagers =>
      import scala.collection.JavaConverters._
      sender ! decorateMessage(
        RegisteredTaskManagers(
          instanceManager.getAllRegisteredInstances.asScala
        )
      )

    case Heartbeat(instanceID, metricsReport, accumulators) =>
      log.debug(s"Received hearbeat message from $instanceID.")

      Future {
        updateAccumulators(accumulators)
      }(context.dispatcher)

      instanceManager.reportHeartBeat(instanceID, metricsReport)

    case message: AccumulatorMessage => handleAccumulatorMessage(message)

    case message: InfoMessage => handleInfoRequestMessage(message, sender())

    case RequestStackTrace(instanceID) =>
      val gateway = instanceManager.getRegisteredInstanceById(instanceID).getActorGateway
      gateway.forward(SendStackTrace, new AkkaActorGateway(sender(), leaderSessionID))

    case Terminated(taskManager) =>
      if (instanceManager.isRegistered(taskManager)) {
        log.info(s"Task manager ${taskManager.path} terminated.")

        instanceManager.unregisterTaskManager(taskManager)
        context.unwatch(taskManager)
      }

    case RequestJobManagerStatus =>
      sender() ! decorateMessage(JobManagerStatusAlive)

    case Disconnect(msg) =>
      val taskManager = sender()

      if (instanceManager.isRegistered(taskManager)) {
        log.info(s"Task manager ${taskManager.path} wants to disconnect, because $msg.")

        instanceManager.unregisterTaskManager(taskManager)
        context.unwatch(taskManager)
      }

    case RequestLeaderSessionID =>
      sender() ! ResponseLeaderSessionID(leaderSessionID)
  }

  /**
   * Submits a job to the job manager. The job is registered at the libraryCacheManager which
   * creates the job's class loader. The job graph is appended to the corresponding execution
   * graph and the execution vertices are queued for scheduling.
   *
   * @param jobGraph representing the Flink job
   * @param listenToEvents true if the sender wants to listen to job status and execution state
   *                       change notifications. false if not.
   */
  private def submitJob(jobGraph: JobGraph, listenToEvents: Boolean): Unit = {
    if (jobGraph == null) {
      sender ! decorateMessage(
        Failure(
          new JobSubmissionException(null, "JobGraph must not be null.")
        )
      )
    }
    else {
      val jobId = jobGraph.getJobID
      val jobName = jobGraph.getName
      var executionGraph: ExecutionGraph = null

      log.info(s"Received job ${jobId} (${jobName}).")

      try {
        // Important: We need to make sure that the library registration is the first action,
        // because this makes sure that the uploaded jar files are removed in case of
        // unsuccessful
        try {
          libraryCacheManager.registerJob(jobGraph.getJobID, jobGraph.getUserJarBlobKeys)
        }
        catch {
          case t: Throwable =>
            throw new JobSubmissionException(jobId,
            "Cannot set up the user code libraries: " + t.getMessage, t)
        }

        val userCodeLoader = libraryCacheManager.getClassLoader(jobGraph.getJobID)
        if (userCodeLoader == null) {
          throw new JobSubmissionException(jobId,
            "The user code class loader could not be initialized.")
        }

        if (jobGraph.getNumberOfVertices == 0) {
          throw new JobSubmissionException(jobId, "The given job is empty")
        }

        // see if there already exists an ExecutionGraph for the corresponding job ID
        executionGraph = currentJobs.getOrElseUpdate(
          jobGraph.getJobID,
          (new ExecutionGraph(
            executionContext,
            jobGraph.getJobID,
            jobGraph.getName,
            jobGraph.getJobConfiguration,
            timeout,
            jobGraph.getUserJarBlobKeys,
            userCodeLoader),
            JobInfo(sender(), System.currentTimeMillis())
          )
        )._1

        // configure the execution graph
        val jobNumberRetries = if (jobGraph.getNumberOfExecutionRetries >= 0) {
          jobGraph.getNumberOfExecutionRetries
        } else {
          defaultExecutionRetries
        }
        executionGraph.setNumberOfRetriesLeft(jobNumberRetries)
        executionGraph.setDelayBeforeRetrying(delayBetweenRetries)
        executionGraph.setScheduleMode(jobGraph.getScheduleMode())
        executionGraph.setQueuedSchedulingAllowed(jobGraph.getAllowQueuedScheduling())
        executionGraph.setJsonPlan(jobGraph.getJsonPlan())
        
        // initialize the vertices that have a master initialization hook
        // file output formats create directories here, input formats create splits
        if (log.isDebugEnabled) {
          log.debug(s"Running initialization on master for job ${jobId} (${jobName}).")
        }

        val numSlots = scheduler.getTotalNumberOfSlots()

        for (vertex <- jobGraph.getVertices.asScala) {

          val executableClass = vertex.getInvokableClassName
          if (executableClass == null || executableClass.length == 0) {
            throw new JobSubmissionException(jobId,
              s"The vertex ${vertex.getID} (${vertex.getName}) has no invokable class.")
          }

          if (vertex.getParallelism() == ExecutionConfig.PARALLELISM_AUTO_MAX) {
            vertex.setParallelism(numSlots)
          }

          try {
            vertex.initializeOnMaster(userCodeLoader)
          }
          catch {
            case t: Throwable => throw new JobExecutionException(jobId,
              "Cannot initialize task '" + vertex.getName + "': " + t.getMessage, t)
          }
        }

        // topologically sort the job vertices and attach the graph to the existing one
        val sortedTopology = jobGraph.getVerticesSortedTopologicallyFromSources()
        if (log.isDebugEnabled) {
          log.debug(s"Adding ${sortedTopology.size()} vertices from " +
            s"job graph ${jobId} (${jobName}).")
        }
        executionGraph.attachJobGraph(sortedTopology)

        if (log.isDebugEnabled) {
          log.debug(s"Successfully created execution graph from job graph ${jobId} (${jobName}).")
        }

        // configure the state checkpointing
        val snapshotSettings = jobGraph.getSnapshotSettings
        if (snapshotSettings != null) {

          val idToVertex: JobVertexID => ExecutionJobVertex = id => {
            val vertex = executionGraph.getJobVertex(id)
            if (vertex == null) {
              throw new JobSubmissionException(jobId,
                "The snapshot checkpointing settings refer to non-existent vertex " + id)
            }
            vertex
          }

          val triggerVertices: java.util.List[ExecutionJobVertex] =
            snapshotSettings.getVerticesToTrigger.asScala.map(idToVertex).asJava

          val ackVertices: java.util.List[ExecutionJobVertex] =
            snapshotSettings.getVerticesToAcknowledge.asScala.map(idToVertex).asJava

          val confirmVertices: java.util.List[ExecutionJobVertex] =
            snapshotSettings.getVerticesToConfirm.asScala.map(idToVertex).asJava

          executionGraph.enableSnapshotCheckpointing(
            snapshotSettings.getCheckpointInterval,
            snapshotSettings.getCheckpointTimeout,
            triggerVertices,
            ackVertices,
            confirmVertices,
            context.system,
            leaderSessionID)
        }

        // get notified about job status changes
        executionGraph.registerJobStatusListener(new AkkaActorGateway(self, leaderSessionID))

        if (listenToEvents) {
          // the sender wants to be notified about state changes
          val gateway = new AkkaActorGateway(sender(), leaderSessionID)

          executionGraph.registerExecutionListener(gateway)
          executionGraph.registerJobStatusListener(gateway)
        }

        // done with submitting the job
        sender ! decorateMessage(Success(jobGraph.getJobID))
      }
      catch {
        case t: Throwable =>
          log.error(s"Failed to submit job ${jobId} (${jobName})", t)

          libraryCacheManager.unregisterJob(jobId)
          currentJobs.remove(jobId)

          if (executionGraph != null) {
            executionGraph.fail(t)
          }

          val rt: Throwable = if (t.isInstanceOf[JobExecutionException]) {
            t
          } else {
            new JobExecutionException(jobId, s"Failed to submit job ${jobId} (${jobName})", t)
          }

          sender ! decorateMessage(Failure(rt))
          return
      }

      // NOTE: Scheduling the job for execution is a separate action from the job submission.
      // The success of submitting the job must be independent from the success of scheduling
      // the job.
      try {
        log.info(s"Scheduling job ${executionGraph.getJobName}.")
        executionGraph.scheduleForExecution(scheduler)
      }
      catch {
        case t: Throwable => try {
          executionGraph.fail(t)
        }
        catch {
          case tt: Throwable => {
            log.error("Error while marking ExecutionGraph as failed.", tt)
          }
        }
      }
    }
  }

  /**
   * Dedicated handler for checkpoint messages.
   * 
   * @param actorMessage The checkpoint actor message.
   */
  private def handleCheckpointMessage(actorMessage: AbstractCheckpointMessage): Unit = {
    actorMessage match {
      case ackMessage: AcknowledgeCheckpoint =>
        val jid = ackMessage.getJob()
        currentJobs.get(jid) match {
          case Some((graph, _)) =>
            val coordinator = graph.getCheckpointCoordinator()
            if (coordinator != null) {
              try {
                coordinator.receiveAcknowledgeMessage(ackMessage)
              }
              catch {
                case t: Throwable =>
                  log.error(s"Error in CheckpointCoordinator while processing $ackMessage", t)
              }
            }
            else {
              log.error(
                s"Received ConfirmCheckpoint message for job $jid with no CheckpointCoordinator")
            }
            
          case None => log.error(s"Received ConfirmCheckpoint for unavailable job $jid")
        }

      // unknown checkpoint message
      case _ => unhandled(actorMessage)
    }
  }
  
  /**
   * Handle unmatched messages with an exception.
   */
  override def unhandled(message: Any): Unit = {
    // let the actor crash
    throw new RuntimeException("Received unknown message " + message)
  }

  /**
   * Handle messages that request or report accumulators.
   *
   * @param message The accumulator message.
   */
  private def handleAccumulatorMessage(message: AccumulatorMessage): Unit = {
      message match {
        case RequestAccumulatorResults(jobID) =>
          try {
            currentJobs.get(jobID) match {
              case Some((graph, jobInfo)) =>
                val accumulatorValues = graph.getAccumulatorsSerialized()
          sender() ! decorateMessage(AccumulatorResultsFound(jobID, accumulatorValues))
              case None =>
                archive.forward(message)
            }
          } catch {
          case e: Exception =>
            log.error("Cannot serialize accumulator result.", e)
            sender() ! decorateMessage(AccumulatorResultsErroneous(jobID, e))
          }

        case RequestAccumulatorResultsStringified(jobId) =>
          currentJobs.get(jobId) match {
            case Some((graph, jobInfo)) =>
              val stringifiedAccumulators = graph.getAccumulatorResultsStringified()
              sender() ! AccumulatorResultStringsFound(jobId, stringifiedAccumulators)
            case None =>
              archive.forward(message)
          }
      }
  }

  /**
   * Dedicated handler for monitor info request messages.
   *
   * @param actorMessage The info request message.
   */
  private def handleInfoRequestMessage(actorMessage: InfoMessage, theSender: ActorRef): Unit = {
    try {
      actorMessage match {

        case _ : RequestJobsOverview =>
          // get our own overview
          val ourJobs = createJobStatusOverview()

          // get the overview from the archive
          val future = (archive ? RequestJobsOverview.getInstance())(timeout)

          future.onSuccess {
            case archiveOverview: JobsOverview =>
              theSender ! new JobsOverview(ourJobs, archiveOverview)
          }(context.dispatcher)

        case _ : RequestJobsWithIDsOverview =>
          // get our own overview
          val ourJobs = createJobStatusWithIDsOverview()

          // get the overview from the archive
          val future = (archive ? RequestJobsWithIDsOverview.getInstance())(timeout)

          future.onSuccess {
            case archiveOverview: JobsWithIDsOverview =>
              theSender ! new JobsWithIDsOverview(ourJobs, archiveOverview)
          }(context.dispatcher)

        case _ : RequestStatusOverview =>

          val ourJobs = createJobStatusOverview()

          val numTMs = instanceManager.getNumberOfRegisteredTaskManagers()
          val numSlotsTotal = instanceManager.getTotalNumberOfSlots()
          val numSlotsAvailable = instanceManager.getNumberOfAvailableSlots()

          // add to that the jobs from the archive
          val future = (archive ? RequestJobsOverview.getInstance())(timeout)
          future.onSuccess {
            case archiveOverview: JobsOverview =>
              theSender ! new StatusOverview(numTMs, numSlotsTotal, numSlotsAvailable,
                ourJobs, archiveOverview)
          }(context.dispatcher)

        case _ : RequestStatusWithJobIDsOverview =>

          val ourJobs = createJobStatusWithIDsOverview()

          val numTMs = instanceManager.getNumberOfRegisteredTaskManagers()
          val numSlotsTotal = instanceManager.getTotalNumberOfSlots()
          val numSlotsAvailable = instanceManager.getNumberOfAvailableSlots()

          // add to that the jobs from the archive
          val future = (archive ? RequestJobsWithIDsOverview.getInstance())(timeout)
          future.onSuccess {
            case archiveOverview: JobsWithIDsOverview =>
              theSender ! new StatusWithJobIDsOverview(numTMs, numSlotsTotal, numSlotsAvailable,
                ourJobs, archiveOverview)
          }(context.dispatcher)

        case _ => throw new Exception("Unrecognized info message " + actorMessage)
      }
    }
    catch {
      case e: Throwable => log.error(s"Error responding to message $actorMessage", e)
    }
  }

  private def createJobStatusOverview() : JobsOverview = {
    var runningOrPending = 0
    var finished = 0
    var canceled = 0
    var failed = 0

    currentJobs.values.foreach {
      _._1.getState() match {
        case JobStatus.FINISHED => finished += 1
        case JobStatus.CANCELED => canceled += 1
        case JobStatus.FAILED => failed += 1
        case _ => runningOrPending += 1
      }
    }

    new JobsOverview(runningOrPending, finished, canceled, failed)
  }

  private def createJobStatusWithIDsOverview() : JobsWithIDsOverview = {
    val runningOrPending = new java.util.ArrayList[JobID]()
    val finished = new java.util.ArrayList[JobID]()
    val canceled = new java.util.ArrayList[JobID]()
    val failed = new java.util.ArrayList[JobID]()
    
    currentJobs.values.foreach { case (graph, _) =>
      graph.getState() match {
        case JobStatus.FINISHED => finished.add(graph.getJobID)
        case JobStatus.CANCELED => canceled.add(graph.getJobID)
        case JobStatus.FAILED => failed.add(graph.getJobID)
        case _ => runningOrPending.add(graph.getJobID)
      }
    }

    new JobsWithIDsOverview(runningOrPending, finished, canceled, failed)
  }

  /**
   * Removes the job and sends it to the MemoryArchivist
   * @param jobID ID of the job to remove and archive
   */
  private def removeJob(jobID: JobID): Unit = {
    currentJobs.remove(jobID) match {
      case Some((eg, _)) =>
        try {
          eg.prepareForArchiving()

          archive ! decorateMessage(ArchiveExecutionGraph(jobID, eg))
        } catch {
          case t: Throwable => log.error(s"Could not prepare the execution graph $eg for " +
            "archiving.", t)
        }

      case None =>
    }

    try {
      libraryCacheManager.unregisterJob(jobID)
    } catch {
      case t: Throwable =>
        log.error(s"Could not properly unregister job $jobID form the library cache.", t)
    }
  }

  /**
   * Updates the accumulators reported from a task manager via the Heartbeat message.
   * @param accumulators list of accumulator snapshots
   */
  private def updateAccumulators(accumulators : Seq[AccumulatorSnapshot]) = {
    accumulators foreach {
      case accumulatorEvent =>
        currentJobs.get(accumulatorEvent.getJobID) match {
          case Some((jobGraph, jobInfo)) =>
            jobGraph.updateAccumulators(accumulatorEvent)
          case None =>
          // ignore accumulator values for old job
        }
    }
  }
}

/**
 * Job Manager companion object. Contains the entry point (main method) to run the JobManager in a
 * standalone fashion. Also contains various utility methods to start the JobManager and to
 * look up the JobManager actor reference.
 */
object JobManager {

  val LOG = Logger(classOf[JobManager])

  val STARTUP_FAILURE_RETURN_CODE = 1
  val RUNTIME_FAILURE_RETURN_CODE = 2

  /** Name of the JobManager actor */
  val JOB_MANAGER_NAME = "jobmanager"

  /** Name of the archive actor */
  val ARCHIVE_NAME = "archive"


  /**
   * Entry point (main method) to run the JobManager in a standalone fashion.
   *
   * @param args The command line arguments.
   */
  def main(args: Array[String]): Unit = {
    // startup checks and logging
    EnvironmentInformation.logEnvironmentInfo(LOG.logger, "JobManager", args)
    EnvironmentInformation.checkJavaVersion()

    // parsing the command line arguments
    val (configuration: Configuration,
         executionMode: JobManagerMode,
         streamingMode: StreamingMode,
         listeningHost: String, listeningPort: Int) =
    try {
      parseArgs(args)
    }
    catch {
      case t: Throwable => {
        LOG.error(t.getMessage(), t)
        System.exit(STARTUP_FAILURE_RETURN_CODE)
        null
      }
    }

    // we want to check that the JobManager hostname is in the config
    // if it is not in there, the actor system will bind to the loopback interface's
    // address and will not be reachable from anyone remote
    if (listeningHost == null) {
      val message = "Config parameter '" + ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY +
        "' is missing (hostname/address to bind JobManager to)."
      LOG.error(message)
      System.exit(STARTUP_FAILURE_RETURN_CODE)
    }

    // address and will not be reachable from anyone remote
    if (listeningPort <= 0 || listeningPort >= 65536) {
      val message = "Config parameter '" + ConfigConstants.JOB_MANAGER_IPC_PORT_KEY +
        "' is invalid, it must be great than 0 and less than 65536."
      LOG.error(message)
      System.exit(STARTUP_FAILURE_RETURN_CODE)
    }

    // run the job manager
    try {
      if (SecurityUtils.isSecurityEnabled) {
        LOG.info("Security is enabled. Starting secure JobManager.")
        SecurityUtils.runSecured(new FlinkSecuredRunner[Unit] {
          override def run(): Unit = {
            runJobManager(configuration, executionMode, streamingMode,
                          listeningHost, listeningPort)
          }
        })
      }
      else {
        LOG.info("Security is not enabled. Starting non-authenticated JobManager.")
        runJobManager(configuration, executionMode, streamingMode,
                      listeningHost, listeningPort)
      }
    }
    catch {
      case t: Throwable => {
        LOG.error("Failed to run JobManager.", t)
        System.exit(STARTUP_FAILURE_RETURN_CODE)
      }
    }
  }

  /**
   * Starts and runs the JobManager with all its components. First, this method starts a
   * dedicated actor system for the JobManager. Second, its starts all components of the
   * JobManager (including library cache, instance manager, scheduler). Finally, it starts
   * the JobManager actor itself.
   *
   * This method blocks indefinitely (or until the JobManager's actor system is shut down).
   *
   * @param configuration The configuration object for the JobManager.
   * @param executionMode The execution mode in which to run. Execution mode LOCAL will spawn an
   *                      an additional TaskManager in the same process.
   * @param streamingMode The streaming mode to run the system in (streaming vs. batch-only)
   * @param listeningAddress The hostname where the JobManager should listen for messages.
   * @param listeningPort The port where the JobManager should listen for messages.
   */
  def runJobManager(
      configuration: Configuration,
      executionMode: JobManagerMode,
      streamingMode: StreamingMode,
      listeningAddress: String,
      listeningPort: Int)
    : Unit = {

    LOG.info("Starting JobManager")

    // Bring up the job manager actor system first, bind it to the given address.
    LOG.info(s"Starting JobManager actor system at $listeningAddress:$listeningPort.")

    val jobManagerSystem = try {
      val akkaConfig = AkkaUtils.getAkkaConfig(
        configuration,
        Some((listeningAddress, listeningPort))
      )
      if (LOG.isDebugEnabled) {
        LOG.debug("Using akka configuration\n " + akkaConfig)
      }
      AkkaUtils.createActorSystem(akkaConfig)
    }
    catch {
      case t: Throwable => {
        if (t.isInstanceOf[org.jboss.netty.channel.ChannelException]) {
          val cause = t.getCause()
          if (cause != null && t.getCause().isInstanceOf[java.net.BindException]) {
            val address = listeningAddress + ":" + listeningPort
            throw new Exception("Unable to create JobManager at address " + address +
              " - " + cause.getMessage(), t)
          }
        }
        throw new Exception("Could not create JobManager actor system", t)
      }
    }

    try {
      // bring up the job manager actor
      LOG.info("Starting JobManager actor")
      val (jobManager, archiver) = startJobManagerActors(
        configuration,
        jobManagerSystem,
        streamingMode)

      // start a process reaper that watches the JobManager. If the JobManager actor dies,
      // the process reaper will kill the JVM process (to ensure easy failure detection)
      LOG.debug("Starting JobManager process reaper")
      jobManagerSystem.actorOf(
        Props(
          classOf[ProcessReaper],
          jobManager,
          LOG.logger,
          RUNTIME_FAILURE_RETURN_CODE),
        "JobManager_Process_Reaper")

      // bring up a local task manager, if needed
      if (executionMode == JobManagerMode.LOCAL) {
        LOG.info("Starting embedded TaskManager for JobManager's LOCAL execution mode")

        val taskManagerActor = TaskManager.startTaskManagerComponentsAndActor(
          configuration,
          jobManagerSystem,
          listeningAddress,
          Some(TaskManager.TASK_MANAGER_NAME),
          Some(jobManager.path.toString),
          true,
          streamingMode,
          classOf[TaskManager])

        LOG.debug("Starting TaskManager process reaper")
        jobManagerSystem.actorOf(
          Props(
            classOf[ProcessReaper],
            taskManagerActor,
            LOG.logger,
            RUNTIME_FAILURE_RETURN_CODE),
          "TaskManager_Process_Reaper")
      }

      // start the job manager web frontend
      if (configuration.getBoolean(ConfigConstants.JOB_MANAGER_NEW_WEB_FRONTEND_KEY, false)) {
        LOG.info("Starting NEW JobManger web frontend")
        
        // start the new web frontend. we need to load this dynamically
        // because it is not in the same project/dependencies
        startWebRuntimeMonitor(configuration, jobManager, archiver)
      }
      else if (configuration.getInteger(ConfigConstants.JOB_MANAGER_WEB_PORT_KEY, 0) != -1) {
        LOG.info("Starting JobManger web frontend")
        val webServer = new WebInfoServer(configuration, jobManager, archiver)
        webServer.start()
      }
    }
    catch {
      case t: Throwable => {
        LOG.error("Error while starting up JobManager", t)
        try {
          jobManagerSystem.shutdown()
        } catch {
          case tt: Throwable => LOG.warn("Could not cleanly shut down actor system", tt)
        }
        throw t
      }
    }

    // block until everything is shut down
    jobManagerSystem.awaitTermination()
  }

  /**
   * Loads the configuration, execution mode and the listening address from the provided command
   * line arguments.
   *
   * @param args command line arguments
   * @return Quadruple of configuration, execution mode and an optional listening address
   */
  def parseArgs(args: Array[String]):
                     (Configuration, JobManagerMode, StreamingMode, String, Int) = {
    val parser = new scopt.OptionParser[JobManagerCliOptions]("JobManager") {
      head("Flink JobManager")

      opt[String]("configDir") action { (arg, conf) => 
        conf.setConfigDir(arg)
        conf
      } text {
        "The configuration directory."
      }

      opt[String]("executionMode") action { (arg, conf) =>
        conf.setJobManagerMode(arg)
        conf
      } text {
        "The execution mode of the JobManager (CLUSTER / LOCAL)"
      }

      opt[String]("streamingMode").optional().action { (arg, conf) =>
        conf.setStreamingMode(arg)
        conf
      } text {
        "The streaming mode of the JobManager (STREAMING / BATCH)"
      }

      opt[String]("host").optional().action { (arg, conf) =>
        conf.setHost(arg)
        conf
      } text {
        "Network address for communication with the job manager"
      }
    }

    val config = parser.parse(args, new JobManagerCliOptions()).getOrElse {
      throw new Exception(
        s"Invalid command line agruments: ${args.mkString(" ")}. Usage: ${parser.usage}")
    }
    
    val configDir = config.getConfigDir()
    
    if (configDir == null) {
      throw new Exception("Missing parameter '--configDir'")
    }
    if (config.getJobManagerMode() == null) {
      throw new Exception("Missing parameter '--executionMode'")
    }

    LOG.info("Loading configuration from " + configDir)
    GlobalConfiguration.loadConfiguration(configDir)
    val configuration = GlobalConfiguration.getConfiguration()

    if (new File(configDir).isDirectory) {
      configuration.setString(ConfigConstants.FLINK_BASE_DIR_PATH_KEY, configDir + "/..")
    }

    // high availability mode
    val (hostname: String, port: Int ) = 
      if (ZooKeeperUtil.isJobManagerHighAvailabilityEnabled(configuration)) {
        // TODO @removeme @tillrohrmann This is the place where the host and random port for JM is
        // chosen.  For the FlinkMiniCluster you have to choose it on your own.
        LOG.info("Starting JobManager in High-Availability Mode")
  
        if (config.getHost() == null) {
          throw new Exception("Missing parameter '--host'. Parameter is required when " +
            "running in high-availability mode")
        }
  
        // Let web server listen on random port
        configuration.setInteger(ConfigConstants.JOB_MANAGER_WEB_PORT_KEY, 0)
  
        (config.getHost(), 0)
      }
      else {
        LOG.info("Staring JobManager without high-availability")
        
        if (config.getHost() != null) {
          throw new Exception("Found an explicit address for JobManager communication " +
            "via the CLI option '--host'.\n" +
            "This parameter must only be set if the JobManager is started in high-availability " +
            "mode and connects to a ZooKeeper quorum.\n" +
            "Please configure ZooKeeper or don't set the '--host' option, so that the JobManager " +
            "uses the address configured under 'conf/flink-conf.yaml'.")
        }
  
        val host = configuration.getString(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, null)
        val port = configuration.getInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY,
            ConfigConstants.DEFAULT_JOB_MANAGER_IPC_PORT)
        (host, port)
      }

    (configuration, config.getJobManagerMode(), config.getStreamingMode(), hostname, port)
  }

  /**
   * Create the job manager components as (instanceManager, scheduler, libraryCacheManager,
   *              archiverProps, accumulatorManager, defaultExecutionRetries,
   *              delayBetweenRetries, timeout)
   *
   * @param configuration The configuration from which to parse the config values.
   * @return The members for a default JobManager.
   */
  def createJobManagerComponents(configuration: Configuration)
    : (ExecutionContext, InstanceManager, FlinkScheduler, BlobLibraryCacheManager,
      Props, Int, Long, FiniteDuration, Int) = {

    val timeout: FiniteDuration = AkkaUtils.getTimeout(configuration)

    val archiveCount = configuration.getInteger(ConfigConstants.JOB_MANAGER_WEB_ARCHIVE_COUNT,
      ConfigConstants.DEFAULT_JOB_MANAGER_WEB_ARCHIVE_COUNT)

    val cleanupInterval = configuration.getLong(
      ConfigConstants.LIBRARY_CACHE_MANAGER_CLEANUP_INTERVAL,
      ConfigConstants.DEFAULT_LIBRARY_CACHE_MANAGER_CLEANUP_INTERVAL) * 1000

    val executionRetries = configuration.getInteger(
      ConfigConstants.DEFAULT_EXECUTION_RETRIES_KEY,
      ConfigConstants.DEFAULT_EXECUTION_RETRIES)

    // configure the delay between execution retries.
    // unless explicitly specifies, this is dependent on the heartbeat timeout
    val pauseString = configuration.getString(ConfigConstants.AKKA_WATCH_HEARTBEAT_PAUSE,
                                              ConfigConstants.DEFAULT_AKKA_ASK_TIMEOUT)
    val delayString = configuration.getString(ConfigConstants.DEFAULT_EXECUTION_RETRY_DELAY_KEY,
                                              pauseString)

    val delayBetweenRetries: Long = try {
        Duration(delayString).toMillis
      }
      catch {
        case n: NumberFormatException => throw new Exception(
          s"Invalid config value for ${ConfigConstants.DEFAULT_EXECUTION_RETRY_DELAY_KEY}: " +
            s"${pauseString}. Value must be a valid duration (such as 100 milli or 1 min)");
      }

    val archiveProps: Props = Props(classOf[MemoryArchivist], archiveCount)

    val executionContext = ExecutionContext.fromExecutor(new ForkJoinPool())

    var blobServer: BlobServer = null
    var instanceManager: InstanceManager = null
    var scheduler: FlinkScheduler = null
    var libraryCacheManager: BlobLibraryCacheManager = null

    try {
      blobServer = new BlobServer(configuration)
      instanceManager = new InstanceManager()
      scheduler = new FlinkScheduler(executionContext)
      libraryCacheManager = new BlobLibraryCacheManager(blobServer, cleanupInterval)

      instanceManager.addInstanceListener(scheduler)
    }
    catch {
      case t: Throwable => {
        if (libraryCacheManager != null) {
          libraryCacheManager.shutdown()
        }
        if (scheduler != null) {
          scheduler.shutdown()
        }
        if (instanceManager != null) {
          instanceManager.shutdown()
        }
        if (blobServer != null) {
          blobServer.shutdown()
        }
        throw t
      }
    }

    (executionContext,
      instanceManager,
      scheduler,
      libraryCacheManager,
      archiveProps,
      executionRetries,
      delayBetweenRetries,
      timeout,
      archiveCount)
  }

  /**
   * Starts the JobManager and job archiver based on the given configuration, in the
   * given actor system.
   *
   * @param configuration The configuration for the JobManager
   * @param actorSystem Teh actor system running the JobManager
   * @return A tuple of references (JobManager Ref, Archiver Ref)
   */
  def startJobManagerActors(
      configuration: Configuration,
      actorSystem: ActorSystem,
      streamingMode: StreamingMode)
    : (ActorRef, ActorRef) = {

    startJobManagerActors(
      configuration,
      actorSystem,
      Some(JOB_MANAGER_NAME),
      Some(ARCHIVE_NAME),
      streamingMode)
  }
  /**
   * Starts the JobManager and job archiver based on the given configuration, in the
   * given actor system.
   *
   * @param configuration The configuration for the JobManager
   * @param actorSystem The actor system running the JobManager
   * @param jobMangerActorName Optionally the name of the JobManager actor. If none is given,
   *                          the actor will have the name generated by the actor system.
   * @param archiverActorName Optionally the name of the archive actor. If none is given,
   *                          the actor will have the name generated by the actor system.
   * @param streamingMode The mode to run the system in (streaming vs. batch-only)
   * 
   * @return A tuple of references (JobManager Ref, Archiver Ref)
   */
  def startJobManagerActors(
      configuration: Configuration,
      actorSystem: ActorSystem,
      jobMangerActorName: Option[String],
      archiverActorName: Option[String],
      streamingMode: StreamingMode)
    : (ActorRef, ActorRef) = {

    val (executionContext,
      instanceManager,
      scheduler,
      libraryCacheManager,
      archiveProps,
      executionRetries,
      delayBetweenRetries,
      timeout,
      _) = createJobManagerComponents(configuration)

    // start the archiver with the given name, or without (avoid name conflicts)
    val archiver: ActorRef = archiverActorName match {
      case Some(actorName) => actorSystem.actorOf(archiveProps, actorName)
      case None => actorSystem.actorOf(archiveProps)
    }

    val jobManagerProps = Props(
      classOf[JobManager],
      configuration,
      executionContext,
      instanceManager,
      scheduler,
      libraryCacheManager,
      archiver,
      executionRetries,
      delayBetweenRetries,
      timeout,
      streamingMode)

    val jobManager: ActorRef = jobMangerActorName match {
      case Some(actorName) => actorSystem.actorOf(jobManagerProps, actorName)
      case None => actorSystem.actorOf(jobManagerProps)
    }

    (jobManager, archiver)
  }

  def startActor(props: Props, actorSystem: ActorSystem): ActorRef = {
    actorSystem.actorOf(props, JOB_MANAGER_NAME)
  }

  // --------------------------------------------------------------------------
  //  Resolving the JobManager endpoint
  // --------------------------------------------------------------------------

  /**
   * Builds the akka actor path for the JobManager actor, given the socket address
   * where the JobManager's actor system runs.
   *
   * @param address The address of the JobManager's actor system.
   * @return The akka URL of the JobManager actor.
   */
  def getRemoteJobManagerAkkaURL(address: InetSocketAddress): String = {
    val hostPort = address.getAddress().getHostAddress() + ":" + address.getPort()
    s"akka.tcp://flink@$hostPort/user/$JOB_MANAGER_NAME"
  }

  /**
   * Builds the akka actor path for the JobManager actor to address the actor within
   * its own actor system.
   *
   * @return The local akka URL of the JobManager actor.
   */
  def getLocalJobManagerAkkaURL: String = {
    "akka://flink/user/" + JOB_MANAGER_NAME
  }

  def getJobManagerRemoteReferenceFuture(
      address: InetSocketAddress,
      system: ActorSystem,
      timeout: FiniteDuration)
    : Future[ActorRef] = {

    AkkaUtils.getReference(getRemoteJobManagerAkkaURL(address), system, timeout)
  }

  /**
   * Resolves the JobManager actor reference in a blocking fashion.
   *
   * @param jobManagerUrl The akka URL of the JobManager.
   * @param system The local actor system that should perform the lookup.
   * @param timeout The maximum time to wait until the lookup fails.
   * @throws java.io.IOException Thrown, if the lookup fails.
   * @return The ActorRef to the JobManager
   */
  @throws(classOf[IOException])
  def getJobManagerRemoteReference(
      jobManagerUrl: String,
      system: ActorSystem,
      timeout: FiniteDuration)
    : ActorRef = {

    try {
      val future = AkkaUtils.getReference(jobManagerUrl, system, timeout)
      Await.result(future, timeout)
    }
    catch {
      case e @ (_ : ActorNotFound | _ : TimeoutException) =>
        throw new IOException(
          s"JobManager at $jobManagerUrl not reachable. " +
            s"Please make sure that the JobManager is running and its port is reachable.", e)

      case e: IOException =>
        throw new IOException("Could not connect to JobManager at " + jobManagerUrl, e)
    }
  }

  /**
   * Resolves the JobManager actor reference in a blocking fashion.
   *
   * @param address The socket address of the JobManager's actor system.
   * @param system The local actor system that should perform the lookup.
   * @param timeout The maximum time to wait until the lookup fails.
   * @throws java.io.IOException Thrown, if the lookup fails.
   * @return The ActorRef to the JobManager
   */
  @throws(classOf[IOException])
  def getJobManagerRemoteReference(
      address: InetSocketAddress,
      system: ActorSystem,
      timeout: FiniteDuration)
    : ActorRef = {

    val jmAddress = getRemoteJobManagerAkkaURL(address)
    getJobManagerRemoteReference(jmAddress, system, timeout)
  }

  /**
   * Resolves the JobManager actor reference in a blocking fashion.
   *
   * @param address The socket address of the JobManager's actor system.
   * @param system The local actor system that should perform the lookup.
   * @param config The config describing the maximum time to wait until the lookup fails.
   * @throws java.io.IOException Thrown, if the lookup fails.
   * @return The ActorRef to the JobManager
   */
  @throws(classOf[IOException])
  def getJobManagerRemoteReference(
      address: InetSocketAddress,
      system: ActorSystem,
      config: Configuration)
    : ActorRef = {

    val timeout = AkkaUtils.getLookupTimeout(config)
    getJobManagerRemoteReference(address, system, timeout)
  }

  /** Returns the [[ActorGateway]] for the provided JobManager. The function automatically
    * retrieves the current leader session ID from the JobManager and instantiates the
    * [[AkkaActorGateway]] with it.
    *
    * @param jobManager ActorRef to the [[JobManager]]
    * @param timeout Timeout for the blocking leader session ID retrieval
    * @throws java.lang.Exception
    * @return Gateway to the specified JobManager
    */
  @throws(classOf[Exception])
  def getJobManagerGateway(
    jobManager: ActorRef,
    timeout: FiniteDuration
    ): ActorGateway = {
    val futureLeaderSessionID = (jobManager ? RequestLeaderSessionID)(timeout)
      .mapTo[ResponseLeaderSessionID]

    val leaderSessionID = Await.result(futureLeaderSessionID, timeout).leaderSessionID

    new AkkaActorGateway(jobManager, leaderSessionID)
  }

  // --------------------------------------------------------------------------
  //  Utilities
  // --------------------------------------------------------------------------

  /**
   * Starts the web runtime monitor. Because the actual implementation of the
   * runtime monitor is in another project, we load the runtime monitor dynamically.
   * 
   * Because failure to start the web runtime monitor is not considered fatal,
   * this method does not throw any exceptions, but only logs them.
   * 
   * @param config The configuration for the runtime monitor.
   * @param jobManager The JobManager actor.
   * @param archiver The execution graph archive actor.
   */
  def startWebRuntimeMonitor(config: Configuration,
                             jobManager: ActorRef,
                             archiver: ActorRef): Unit = {
    // try to load and instantiate the class
    val monitor: WebMonitor =
      try {
        val classname = "org.apache.flink.runtime.webmonitor.WebRuntimeMonitor"
        val clazz: Class[_ <: WebMonitor] = Class.forName(classname)
                                                 .asSubclass(classOf[WebMonitor])
        
        val ctor: Constructor[_ <: WebMonitor] = clazz.getConstructor(classOf[Configuration],
                                                                      classOf[ActorRef],
                                                                      classOf[ActorRef])
        ctor.newInstance(config, jobManager, archiver)
      }
      catch {
        case e: ClassNotFoundException =>
          LOG.error("Could not load web runtime monitor. " +
              "Probably reason: flink-runtime-web is not in the classpath")
          LOG.debug("Caught exception", e)
          null
        case e: InvocationTargetException =>
          LOG.error("WebServer could not be created", e.getTargetException())
          null
        case t: Throwable =>
          LOG.error("Failed to instantiate web runtime monitor.", t)
          null
      }
    
    if (monitor != null) {
      try {
        monitor.start()
      }
      catch {
        case e: Exception => 
          LOG.error("Failed to start web runtime monitor", e)
      }
    }
  }
}
