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

package org.apache.flink.runtime.taskmanager

import java.io.{File, FileInputStream, IOException}
import java.lang.management.ManagementFactory
import java.net.{BindException, InetAddress, InetSocketAddress, ServerSocket}
import java.util
import java.util.concurrent.{Callable, TimeUnit, TimeoutException}
import java.util.{Collections, UUID}

import _root_.akka.actor._
import _root_.akka.pattern.ask
import _root_.akka.util.Timeout
import grizzled.slf4j.Logger
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.flink.api.common.time.Time
import org.apache.flink.configuration._
import org.apache.flink.core.fs.FileSystem
import org.apache.flink.runtime.accumulators.AccumulatorSnapshot
import org.apache.flink.runtime.akka.{AkkaUtils, DefaultQuarantineHandler, QuarantineMonitor}
import org.apache.flink.runtime.blob.BlobCacheService
import org.apache.flink.runtime.broadcast.BroadcastVariableManager
import org.apache.flink.runtime.clusterframework.BootstrapTools
import org.apache.flink.runtime.clusterframework.messages.StopCluster
import org.apache.flink.runtime.clusterframework.types.{AllocationID, ResourceID}
import org.apache.flink.runtime.concurrent.{Executors, FutureUtils}
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor
import org.apache.flink.runtime.execution.ExecutionState
import org.apache.flink.runtime.execution.librarycache.{BlobLibraryCacheManager, LibraryCacheManager}
import org.apache.flink.runtime.executiongraph.{ExecutionAttemptID, PartitionInfo}
import org.apache.flink.runtime.filecache.FileCache
import org.apache.flink.runtime.highavailability.HighAvailabilityServicesUtils.AddressResolution
import org.apache.flink.runtime.highavailability.{HighAvailabilityServices, HighAvailabilityServicesUtils}
import org.apache.flink.runtime.instance.{ActorGateway, AkkaActorGateway, HardwareDescription, InstanceID}
import org.apache.flink.runtime.io.disk.iomanager.IOManager
import org.apache.flink.runtime.io.network.NetworkEnvironment
import org.apache.flink.runtime.io.network.netty.PartitionProducerStateChecker
import org.apache.flink.runtime.io.network.partition.ResultPartitionConsumableNotifier
import org.apache.flink.runtime.leaderretrieval.{LeaderRetrievalListener, LeaderRetrievalService}
import org.apache.flink.runtime.memory.MemoryManager
import org.apache.flink.runtime.messages.Messages._
import org.apache.flink.runtime.messages.RegistrationMessages._
import org.apache.flink.runtime.messages.StackTraceSampleMessages.{SampleTaskStackTrace, StackTraceSampleMessages, TriggerStackTraceSample}
import org.apache.flink.runtime.messages.TaskManagerMessages._
import org.apache.flink.runtime.messages.TaskMessages._
import org.apache.flink.runtime.messages.checkpoint.{AbstractCheckpointMessage, NotifyCheckpointComplete, TriggerCheckpoint}
import org.apache.flink.runtime.messages.{Acknowledge, StackTraceSampleResponse}
import org.apache.flink.runtime.metrics.groups.TaskManagerMetricGroup
import org.apache.flink.runtime.metrics.util.MetricUtils
import org.apache.flink.runtime.metrics.{MetricRegistryConfiguration, MetricRegistryImpl, MetricRegistry => FlinkMetricRegistry}
import org.apache.flink.runtime.process.ProcessReaper
import org.apache.flink.runtime.security.{SecurityConfiguration, SecurityUtils}
import org.apache.flink.runtime.state.{TaskExecutorLocalStateStoresManager, TaskStateManagerImpl}
import org.apache.flink.runtime.taskexecutor.{TaskExecutor, TaskManagerConfiguration, TaskManagerServices, TaskManagerServicesConfiguration}
import org.apache.flink.runtime.util._
import org.apache.flink.runtime.{FlinkActor, LeaderSessionMessageFilter, LogMessages}
import org.apache.flink.util.NetUtils

import scala.collection.JavaConverters._
import scala.concurrent._
import scala.concurrent.duration._
import scala.language.postfixOps

/**
 * The TaskManager is responsible for executing the individual tasks of a Flink job. It is
 * implemented as an actor. The TaskManager has the following phases:
 *
 * - "Waiting to be registered": In that phase, it periodically
 *   sends a [[RegisterTaskManager]] message to the JobManager.
 *   Upon successful registration, the JobManager replies with an [[AcknowledgeRegistration]]
 *   message. This stops the registration messages and initializes all fields
 *   that require the JobManager's actor reference.
 *
 * - "Operational": Here the TaskManager accepts and processes task messages, like
 *   [[SubmitTask]], [[CancelTask]], [[FailTask]].
 *   If the TaskManager disconnects from the JobManager (because the JobManager is no longer
 *   reachable), the TaskManager gets back to the "waiting to be registered" state.
 *
 *
 *  ========== Failure model of the TaskManager ==========
 *
 *  The TaskManager tries to compensate for task failures as far as possible by marking
 *  the task as failed and removing all its resources. This causes the JobManager to
 *  restart the task (on this same TaskManager or on a different TaskManager).
 *
 *  In certain cases, exceptions indicate that the TaskManager is unable to proceed.
 *  The most robust way to clean up is letting the OS/kernel do it, so we will trigger
 *  killing the process. In case of YARN (or resilient standalone mode), the process
 *  will be restarted, producing a clean state.
 *  To achieve this, we kill the TaskManager actor. The watch dog actor (process reaper)
 *  will recognize that and kill the TaskManager process.
 *
 *  Fatal errors that require TaskManager JVM restart include:
 *
 *    - Errors bringing up the Network Stack or Library Cache after the TaskManager
 *      has registered at the JobManager. The TaskManager cannot operate without.
 *
 *    - Exceptions while releasing the task resources from the network stack, intermediate
 *      results, or memory manager. Those situations indicate a critical leak in the
 *      resource management, which can only be reliably fixed through a JVM restart.
 *
 *    - Exceptions releasing intermediate result resources. Critical resource leak,
 *      requires a clean JVM.
 */
class TaskManager(
    protected val config: TaskManagerConfiguration,
    protected val resourceID: ResourceID,
    protected val location: TaskManagerLocation,
    protected val memoryManager: MemoryManager,
    protected val ioManager: IOManager,
    protected val network: NetworkEnvironment,
    protected val taskManagerLocalStateStoresManager: TaskExecutorLocalStateStoresManager,
    protected val numberOfSlots: Int,
    protected val highAvailabilityServices: HighAvailabilityServices,
    protected val taskManagerMetricGroup: TaskManagerMetricGroup)
  extends FlinkActor
  with LeaderSessionMessageFilter // Mixin order is important: We want to filter after logging
  with LogMessages // Mixin order is important: first we want to support message logging
  with LeaderRetrievalListener {

  override val log = Logger(getClass)

  /** The timeout for all actor ask futures */
  protected val askTimeout = new Timeout(config.getTimeout().getSize, config.getTimeout().getUnit())

  /** The TaskManager's physical execution resources */
  protected val resources = HardwareDescription.extractFromSystem(memoryManager.getMemorySize())

  /** Registry of all tasks currently executed by this TaskManager */
  protected val runningTasks = new java.util.HashMap[ExecutionAttemptID, Task]()

  /** Handler for shared broadcast variables (shared between multiple Tasks) */
  protected val bcVarManager = new BroadcastVariableManager()



  protected val leaderRetrievalService: LeaderRetrievalService = highAvailabilityServices.
    getJobManagerLeaderRetriever(
      HighAvailabilityServices.DEFAULT_JOB_ID)

  /** Actors which want to be notified once this task manager has been
    * registered at the job manager */
  private val waitForRegistration = scala.collection.mutable.Set[ActorRef]()

  private var blobCache: Option[BlobCacheService] = None
  /** Handler for distributed files cached by this TaskManager */
  private var fileCache: Option[FileCache] = None
  private var libraryCacheManager: Option[LibraryCacheManager] = None

  /* The current leading JobManager Actor associated with */
  protected var currentJobManager: Option[ActorRef] = None
  /* The current leading JobManager URL */
  private var jobManagerAkkaURL: Option[String] = None

  private var instanceID: InstanceID = null

  private var heartbeatScheduler: Option[Cancellable] = None

  var leaderSessionID: Option[UUID] = None

  private var scheduledTaskManagerRegistration: Option[Cancellable] = None
  private var currentRegistrationRun: UUID = UUID.randomUUID()

  private var connectionUtils: Option[(
    CheckpointResponder,
    PartitionProducerStateChecker,
    ResultPartitionConsumableNotifier,
    TaskManagerActions)] = None

  // --------------------------------------------------------------------------
  //  Actor messages and life cycle
  // --------------------------------------------------------------------------

  /**
   * Called prior to the actor receiving any messages.
   * Logs some context info and triggers the initial attempt to register at the
   * JobManager.
   */
  override def preStart(): Unit = {
    log.info(s"Starting TaskManager actor at ${self.path.toSerializationFormat}.")
    log.info(s"TaskManager data connection information: $location")
    log.info(s"TaskManager has $numberOfSlots task slot(s).")

    // log the initial memory utilization
    if (log.isInfoEnabled) {
      log.info(MemoryLogger.getMemoryUsageStatsAsString(ManagementFactory.getMemoryMXBean))
    }

    try {
      leaderRetrievalService.start(this)
    } catch {
      case e: Exception =>
        log.error("Could not start leader retrieval service.", e)
        throw new RuntimeException("Could not start leader retrieval service.", e)
    }
  }

  /**
   * Called after the actor is stopped.
   * Makes sure all currently running tasks are cancelled and all components
   * (like network stack, library cache, memory manager, ...) are properly shut down.
   */
  override def postStop(): Unit = {
    log.info(s"Stopping TaskManager ${self.path.toSerializationFormat}.")

    cancelAndClearEverything(new Exception("TaskManager is shutting down."))

    if (isConnected) {
      try {
        disassociateFromJobManager()
      } catch {
        case t: Exception => log.error("Could not cleanly disassociate from JobManager", t)
      }
    }

    try {
      leaderRetrievalService.stop()
    } catch {
      case e: Exception => log.error("Leader retrieval service did not shut down properly.")
    }

    try {
      ioManager.shutdown()
    } catch {
      case t: Exception => log.error("I/O manager did not shutdown properly.", t)
    }

    try {
      memoryManager.shutdown()
    } catch {
      case t: Exception => log.error("Memory manager did not shutdown properly.", t)
    }

    try {
      network.shutdown()
    } catch {
      case t: Exception => log.error("Network environment did not shutdown properly.", t)
    }

    try {
      taskManagerLocalStateStoresManager.shutdown()
    } catch {
      case t: Exception => log.error("Task state manager did not shutdown properly.", t)
    }

    taskManagerMetricGroup.close()

    log.info(s"Task manager ${self.path} is completely shut down.")
  }

  /**
   * Central handling of actor messages. This method delegates to the more specialized
   * methods for handling certain classes of messages.
   */
  override def handleMessage: Receive = {
    // task messages are most common and critical, we handle them first
    case message: TaskMessage => handleTaskMessage(message)

    // messages for coordinating checkpoints
    case message: AbstractCheckpointMessage => handleCheckpointingMessage(message)

    case JobManagerLeaderAddress(address, newLeaderSessionID) =>
      handleJobManagerLeaderAddress(address, newLeaderSessionID)

    // registration messages for connecting and disconnecting from / to the JobManager
    case message: RegistrationMessage => handleRegistrationMessage(message)

    // task sampling messages
    case message: StackTraceSampleMessages => handleStackTraceSampleMessage(message)

    // ----- miscellaneous messages ----

    // periodic heart beats that transport metrics
    case SendHeartbeat => sendHeartbeatToJobManager()

    // sends the stack trace of this TaskManager to the sender
    case SendStackTrace => sendStackTrace(sender())

    // registers the message sender to be notified once this TaskManager has completed
    // its registration at the JobManager
    case NotifyWhenRegisteredAtJobManager =>
      if (isConnected) {
        sender ! decorateMessage(RegisteredAtJobManager(leaderSessionID.orNull))
      } else {
        waitForRegistration += sender
      }

    // this message indicates that some actor watched by this TaskManager has died
    case Terminated(actor: ActorRef) =>
      if (isConnected && actor == currentJobManager.orNull) {
          handleJobManagerDisconnect("JobManager is no longer reachable")
          triggerTaskManagerRegistration()
      } else {
        log.warn(s"Received unrecognized disconnect message " +
            s"from ${if (actor == null) null else actor.path}.")
      }

    case Disconnect(instanceIdToDisconnect, cause) =>
      if (instanceIdToDisconnect.equals(instanceID)) {
        handleJobManagerDisconnect(s"JobManager requested disconnect: ${cause.getMessage()}")
        triggerTaskManagerRegistration()
      } else {
        log.debug(s"Received disconnect message for wrong instance id ${instanceIdToDisconnect}.")
      }

    case msg: StopCluster =>
      log.info(s"Stopping TaskManager with final application status ${msg.finalStatus()} " +
        s"and diagnostics: ${msg.message()}")
      shutdown()

    case FatalError(message, cause) =>
      killTaskManagerFatal(message, cause)

    case RequestTaskManagerLog(requestType : LogTypeRequest) =>
      blobCache match {
        case Some(_) =>
          handleRequestTaskManagerLog(sender(), requestType, currentJobManager.get)
        case None =>
          sender() ! akka.actor.Status.Failure(new IOException("BlobCache not " +
            "available. Cannot upload TaskManager logs."))
      }

    case RequestBroadcastVariablesWithReferences =>
      sender ! decorateMessage(
        ResponseBroadcastVariablesWithReferences(
          bcVarManager.getNumberOfVariablesWithReferences)
      )

    case RequestNumActiveConnections =>
      val numActive = if (!network.isShutdown) {
        network.getConnectionManager.getNumberOfActiveConnections
      } else {
        0
      }

      sender ! decorateMessage(ResponseNumActiveConnections(numActive))
  }

  /**
   * Handle unmatched messages with an exception.
   */
  override def unhandled(message: Any): Unit = {
    val errorMessage = "Received unknown message " + message
    val error = new RuntimeException(errorMessage)
    log.error(errorMessage)

    // terminate all we are currently running (with a dedicated message)
    // before the actor is stopped
    cancelAndClearEverything(error)

    // let the actor crash
    throw error
  }

  /**
   * Handler for messages concerning the deployment and status updates of
   * tasks.
   *
   * @param message The task message.
   */
  private def handleTaskMessage(message: TaskMessage): Unit = {

    // at very first, check that we are actually currently associated with a JobManager
    if (!isConnected) {
      log.debug(s"Dropping message $message because the TaskManager is currently " +
        "not connected to a JobManager.")
    } else {
      // we order the messages by frequency, to make sure the code paths for matching
      // are as short as possible
      message match {

        // tell the task about the availability of a new input partition
        case UpdateTaskSinglePartitionInfo(executionID, resultID, partitionInfo) =>
          updateTaskInputPartitions(
            executionID,
            Collections.singletonList(new PartitionInfo(resultID, partitionInfo)))

        // tell the task about the availability of some new input partitions
        case UpdateTaskMultiplePartitionInfos(executionID, partitionInfos) =>
          updateTaskInputPartitions(executionID, partitionInfos)

        // discards intermediate result partitions of a task execution on this TaskManager
        case FailIntermediateResultPartitions(executionID) =>
          log.info("Discarding the results produced by task execution " + executionID)
          try {
            network.getResultPartitionManager.releasePartitionsProducedBy(executionID)
          } catch {
            case t: Throwable => killTaskManagerFatal(
            "Fatal leak: Unable to release intermediate result partition data", t)
          }

        // notifies the TaskManager that the state of a task has changed.
        // the TaskManager informs the JobManager and cleans up in case the transition
        // was into a terminal state, or in case the JobManager cannot be informed of the
        // state transition

      case updateMsg @ UpdateTaskExecutionState(taskExecutionState: TaskExecutionState) =>

          // we receive these from our tasks and forward them to the JobManager
          currentJobManager foreach {
            jobManager => {
            val futureResponse = (jobManager ? decorateMessage(updateMsg))(askTimeout)

              val executionID = taskExecutionState.getID

              futureResponse.mapTo[Boolean].onComplete {
                // IMPORTANT: In the future callback, we cannot directly modify state
                //            but only send messages to the TaskManager to do those changes
                case scala.util.Success(result) =>
                  if (!result) {
                  self ! decorateMessage(
                    FailTask(
                      executionID,
                      new Exception("Task has been cancelled on the JobManager."))
                    )
                  }

                case scala.util.Failure(t) =>
                self ! decorateMessage(
                  FailTask(
                    executionID,
                    new Exception(
                      "Failed to send ExecutionStateChange notification to JobManager", t))
                )
              }(context.dispatcher)
            }
          }

        // removes the task from the TaskManager and frees all its resources
        case TaskInFinalState(executionID) =>
          unregisterTaskAndNotifyFinalState(executionID)

        // starts a new task on the TaskManager
        case SubmitTask(tdd) =>
          submitTask(tdd)

        // marks a task as failed for an external reason
        // external reasons are reasons other than the task code itself throwing an exception
        case FailTask(executionID, cause) =>
          val task = runningTasks.get(executionID)
          if (task != null) {
            task.failExternally(cause)
          } else {
            log.debug(s"Cannot find task to fail for execution $executionID)")
          }

        // stops a task
        case StopTask(executionID) =>
          val task = runningTasks.get(executionID)
          if (task != null) {
            try {
              task.stopExecution()
              sender ! decorateMessage(Acknowledge.get())
            } catch {
              case t: Throwable =>
                sender ! decorateMessage(Status.Failure(t))
            }
          } else {
            log.debug(s"Cannot find task to stop for execution ${executionID})")
            sender ! decorateMessage(Acknowledge.get())
          }

        // cancels a task
        case CancelTask(executionID) =>
          val task = runningTasks.get(executionID)
          if (task != null) {
            task.cancelExecution()
            sender ! decorateMessage(Acknowledge.get())
          } else {
            log.debug(s"Cannot find task to cancel for execution $executionID)")
            sender ! decorateMessage(Acknowledge.get())
          }
      }
      }
  }

  /**
   * Handler for messages related to checkpoints.
   *
   * @param actorMessage The checkpoint message.
   */
  private def handleCheckpointingMessage(actorMessage: AbstractCheckpointMessage): Unit = {

    actorMessage match {
      case message: TriggerCheckpoint =>
        val taskExecutionId = message.getTaskExecutionId
        val checkpointId = message.getCheckpointId
        val timestamp = message.getTimestamp
        val checkpointOptions = message.getCheckpointOptions

        log.debug(s"Receiver TriggerCheckpoint $checkpointId@$timestamp for $taskExecutionId.")

        val task = runningTasks.get(taskExecutionId)
        if (task != null) {
          task.triggerCheckpointBarrier(checkpointId, timestamp, checkpointOptions)
        } else {
          log.debug(s"TaskManager received a checkpoint request for unknown task $taskExecutionId.")
        }

      case message: NotifyCheckpointComplete =>
        val taskExecutionId = message.getTaskExecutionId
        val checkpointId = message.getCheckpointId
        val timestamp = message.getTimestamp

        log.debug(s"Receiver ConfirmCheckpoint $checkpointId@$timestamp for $taskExecutionId.")

        val task = runningTasks.get(taskExecutionId)
        if (task != null) {
          task.notifyCheckpointComplete(checkpointId)
        } else {
          log.debug(
            s"TaskManager received a checkpoint confirmation for unknown task $taskExecutionId.")
        }

      // unknown checkpoint message
      case _ => unhandled(actorMessage)
    }
  }

  /**
   * Handler for messages concerning the registration of the TaskManager at the JobManager.
   *
   * Errors must not propagate out of the handler, but need to be handled internally.
   *
   * @param message The registration message.
   */
  private def handleRegistrationMessage(message: RegistrationMessage): Unit = {
    message match {
      case TriggerTaskManagerRegistration(
        jobManagerURL,
        timeout,
        deadline,
        attempt,
        registrationRun) =>

        if (registrationRun.equals(this.currentRegistrationRun)) {
          if (isConnected) {
            // this may be the case, if we queue another attempt and
            // in the meantime, the registration is acknowledged
            log.debug(
              "TaskManager was triggered to register at JobManager, but is already registered")
          } else if (deadline.exists(_.isOverdue())) {
            // we failed to register in time. that means we should quit
            log.error("Failed to register at the JobManager within the defined maximum " +
                        "connect time. Shutting down ...")

            // terminate ourselves (hasta la vista)
            self ! decorateMessage(PoisonPill)
          } else {
            if (!jobManagerAkkaURL.equals(Option(jobManagerURL))) {
              throw new Exception("Invalid internal state: Trying to register at JobManager " +
                                    s"$jobManagerURL even though the current JobManagerAkkaURL " +
                                    s"is set to ${jobManagerAkkaURL.getOrElse("")}")
            }

            log.info(s"Trying to register at JobManager $jobManagerURL " +
                       s"(attempt $attempt, timeout: $timeout)")

            val jobManager = context.actorSelection(jobManagerURL)

            jobManager ! decorateMessage(
              RegisterTaskManager(
                resourceID,
                location,
                resources,
                numberOfSlots)
            )

            // the next timeout computes via exponential backoff with cap
            val nextTimeout = (timeout * 2).min(new FiniteDuration(
              config.getMaxRegistrationPause().toMilliseconds,
              TimeUnit.MILLISECONDS))

            // schedule a check to trigger a new registration attempt if not registered
            // by the timeout
            scheduledTaskManagerRegistration = Option(context.system.scheduler.scheduleOnce(
              timeout,
              self,
              decorateMessage(TriggerTaskManagerRegistration(
                jobManagerURL,
                nextTimeout,
                deadline,
                attempt + 1,
                registrationRun)
              ))(context.dispatcher))
          }
        } else {
          log.info(s"Discarding registration run with ID $registrationRun")
        }

      // successful registration. associate with the JobManager
      // we disambiguate duplicate or erroneous messages, to simplify debugging
      case AcknowledgeRegistration(id, blobPort) =>
        val jobManager = sender()

        if (isConnected) {
          if (jobManager == currentJobManager.orNull) {
            log.debug("Ignoring duplicate registration acknowledgement.")
          } else {
            log.warn(s"Ignoring 'AcknowledgeRegistration' message from ${jobManager.path} , " +
              s"because the TaskManager is already registered at ${currentJobManager.orNull}")
          }
        } else {
          // not yet connected, so let's associate with that JobManager
          try {
            associateWithJobManager(jobManager, id, blobPort)
          } catch {
            case t: Throwable =>
              killTaskManagerFatal(
                "Unable to start TaskManager components and associate with the JobManager", t)
          }
        }

      // we are already registered at that specific JobManager - duplicate answer, rare cases
      case AlreadyRegistered(id, blobPort) =>
        val jobManager = sender()

        if (isConnected) {
          if (jobManager == currentJobManager.orNull) {
            log.debug("Ignoring duplicate registration acknowledgement.")
          } else {
            log.warn(s"Received 'AlreadyRegistered' message from " +
              s"JobManager ${jobManager.path}, even through TaskManager is currently " +
              s"registered at ${currentJobManager.orNull}")
          }
        } else {
          // not connected, yet, so let's associate
          log.info("Received 'AlreadyRegistered' message before 'AcknowledgeRegistration'")

          try {
            associateWithJobManager(jobManager, id, blobPort)
          } catch {
            case t: Throwable =>
              killTaskManagerFatal(
                "Unable to start TaskManager components after registering at JobManager", t)
          }
        }

      case RefuseRegistration(reason) =>
        if (currentJobManager.isEmpty) {
          log.error(s"The registration at JobManager $jobManagerAkkaURL was refused, " +
            s"because: $reason. Retrying later...")

          if(jobManagerAkkaURL.isDefined) {
            // try the registration again after some time
            val delay: FiniteDuration = new FiniteDuration(
              config.getRefusedRegistrationPause().getSize(),
              config.getRefusedRegistrationPause().getUnit())
            val deadline: Option[Deadline] = Option(config.getMaxRegistrationDuration())
              .map {
                duration => new FiniteDuration(duration.getSize(), duration.getUnit()) +
                  delay fromNow
              }

            // start a new registration run
            currentRegistrationRun = UUID.randomUUID()

            scheduledTaskManagerRegistration.foreach(_.cancel())

            scheduledTaskManagerRegistration = Option(
              context.system.scheduler.scheduleOnce(delay) {
                self ! decorateMessage(
                  TriggerTaskManagerRegistration(
                    jobManagerAkkaURL.get,
                    new FiniteDuration(
                      config.getInitialRegistrationPause().getSize(),
                      config.getInitialRegistrationPause().getUnit()),
                    deadline,
                    1,
                    currentRegistrationRun)
                )
              }(context.dispatcher))
          }
        } else {
          // ignore RefuseRegistration messages which arrived after AcknowledgeRegistration
          if (sender() == currentJobManager.orNull) {
            log.warn(s"Received 'RefuseRegistration' from the JobManager (${sender().path})" +
              s" even though this TaskManager is already registered there.")
          } else {
            log.warn(s"Ignoring 'RefuseRegistration' from unrelated " +
              s"JobManager (${sender().path})")
          }
        }

      case _ => unhandled(message)
    }
  }

  private def handleStackTraceSampleMessage(message: StackTraceSampleMessages): Unit = {
    message match {

      // Triggers the sampling of a task
      case TriggerStackTraceSample(
        sampleId,
        executionId,
        numSamples,
        delayBetweenSamples,
        maxStackTraceDepth) =>

          log.debug(s"Triggering stack trace sample $sampleId.")

          val senderRef = sender()

          self ! SampleTaskStackTrace(
            sampleId,
            executionId,
            delayBetweenSamples,
            maxStackTraceDepth,
            numSamples,
            new java.util.ArrayList(),
            senderRef)

      // Repeatedly sent to self to sample a task
      case SampleTaskStackTrace(
        sampleId,
        executionId,
        delayBetweenSamples,
        maxStackTraceDepth,
        remainingNumSamples,
        currentTraces,
        sender) =>

        try {
          if (remainingNumSamples >= 1) {
            getStackTrace(executionId, maxStackTraceDepth) match {
              case Some(stackTrace) =>

                currentTraces.add(stackTrace)

                if (remainingNumSamples > 1) {
                  // ---- Continue ----
                  val msg = SampleTaskStackTrace(
                    sampleId,
                    executionId,
                    delayBetweenSamples,
                    maxStackTraceDepth,
                    remainingNumSamples - 1,
                    currentTraces,
                    sender)

                  context.system.scheduler.scheduleOnce(
                    new FiniteDuration(delayBetweenSamples.getSize, delayBetweenSamples.getUnit),
                    self,
                    msg)(context.dispatcher)
                } else {
                  // ---- Done ----
                  log.debug(s"Done with stack trace sample $sampleId.")

                  sender ! new StackTraceSampleResponse(sampleId, executionId, currentTraces)
                }

              case None =>
                if (currentTraces.isEmpty()) {
                  throw new IllegalStateException(s"Cannot sample task $executionId. " +
                    s"Either the task is not known to the task manager or it is not running.")
                } else {
                  // Task removed during sampling. Reply with partial result.
                  sender ! new StackTraceSampleResponse(sampleId, executionId, currentTraces)
                }
            }
          } else {
            throw new IllegalStateException("Non-positive number of remaining samples")
          }
        } catch {
          case e: Exception =>
            sender ! decorateMessage(Status.Failure(e))
        }

      case _ => unhandled(message)
    }

    /**
      * Returns a stack trace of a running task.
      *
      * @param executionId ID of the running task.
      * @param maxStackTraceDepth Maximum depth of the stack trace. 0 indicates
      *                           no maximum and collects the complete stack
      *                           trace.
      * @return Stack trace of the running task.
      */
    def getStackTrace(
      executionId: ExecutionAttemptID,
      maxStackTraceDepth: Int): Option[Array[StackTraceElement]] = {

      val task = runningTasks.get(executionId)

      if (task != null && task.getExecutionState == ExecutionState.RUNNING) {
        val stackTrace : Array[StackTraceElement] = task.getExecutingThread.getStackTrace

        if (maxStackTraceDepth > 0) {
          Option(util.Arrays.copyOfRange(stackTrace, 0, maxStackTraceDepth.min(stackTrace.length)))
        } else {
          Option(stackTrace)
        }
      } else {
        Option.empty
      }
    }
  }

  private def handleRequestTaskManagerLog(
      sender: ActorRef,
      requestType: LogTypeRequest,
      jobManager: ActorRef)
    : Unit = {
    val logFilePathOption = Option(config.getConfiguration().getString(
      ConfigConstants.TASK_MANAGER_LOG_PATH_KEY, System.getProperty("log.file")))
    logFilePathOption match {
      case None => sender ! akka.actor.Status.Failure(
        new IOException("TaskManager log files are unavailable. " +
        "Log file location not found in environment variable log.file or configuration key "
        + ConfigConstants.TASK_MANAGER_LOG_PATH_KEY + "."))
      case Some(logFilePath) =>
        val file: File = requestType match {
          case LogFileRequest => new File(logFilePath);
          case StdOutFileRequest =>
            new File(logFilePath.substring(0, logFilePath.length - 4) + ".out");
        }
        if (file.exists()) {
          val fis = new FileInputStream(file);
          Future {
            blobCache.get.getTransientBlobService.putTransient(fis)
          }(context.dispatcher)
            .onComplete {
              case scala.util.Success(value) =>
                sender ! value
                fis.close()
              case scala.util.Failure(e) =>
                sender ! akka.actor.Status.Failure(e)
                fis.close()
            }(context.dispatcher)
        } else {
          sender ! akka.actor.Status.Failure(
            new IOException("TaskManager log files are unavailable. " +
            "Log file could not be found at " + file.getAbsolutePath + "."))
        }
    }
  }

  // --------------------------------------------------------------------------
  //  Task Manager / ResourceManager / JobManager association and initialization
  // --------------------------------------------------------------------------

  /**
    * Checks whether the TaskManager is currently connected to the JobManager.
    *
    * @return True, if the TaskManager is currently connected to the JobManager, false otherwise.
    */
  protected def isConnected : Boolean = currentJobManager.isDefined

  /**
   * Associates the TaskManager with the given JobManager. After this
   * method has completed, the TaskManager is ready to receive work
   * from the given JobManager.
   *
   * @param jobManager The JobManager to associate with.
   * @param id The instanceID under which the TaskManager is registered
   *           at the JobManager.
   * @param blobPort The JobManager's port for the BLOB server.
   */
  private def associateWithJobManager(
      jobManager: ActorRef,
      id: InstanceID,
      blobPort: Int)
    : Unit = {

    if (jobManager == null) {
      throw new NullPointerException("jobManager must not be null.")
    }
    if (id == null) {
      throw new NullPointerException("instance ID must not be null.")
    }
    if (blobPort <= 0 || blobPort > 65535) {
      throw new IllegalArgumentException("blob port is out of range: " + blobPort)
    }

    // sanity check that we are not currently registered with a different JobManager
    if (isConnected) {
      if (currentJobManager.get == jobManager) {
        log.warn("Received call to finish registration with JobManager " +
          jobManager.path + " even though TaskManager is already registered.")
        return
      }
      else {
        throw new IllegalStateException("Attempt to register with JobManager " +
          jobManager.path + " even though TaskManager is currently registered with JobManager " +
          currentJobManager.get.path)
      }
    }

    // not yet associated, so associate
    log.info(s"Successful registration at JobManager (${jobManager.path}), " +
      "starting network stack and library cache.")

    // sanity check that the JobManager dependent components are not set up currently
    if (connectionUtils.isDefined || blobCache.isDefined) {
      throw new IllegalStateException("JobManager-specific components are already initialized.")
    }

    currentJobManager = Some(jobManager)
    instanceID = id

    val jobManagerGateway = new AkkaActorGateway(jobManager, leaderSessionID.orNull)
    val taskManagerGateway = new AkkaActorGateway(self, leaderSessionID.orNull)

    val checkpointResponder = new ActorGatewayCheckpointResponder(jobManagerGateway)

    val taskManagerConnection = new ActorGatewayTaskManagerActions(taskManagerGateway)

    val partitionStateChecker = new ActorGatewayPartitionProducerStateChecker(
      jobManagerGateway,
      new FiniteDuration(config.getTimeout().toMilliseconds, TimeUnit.MILLISECONDS))

    val resultPartitionConsumableNotifier = new ActorGatewayResultPartitionConsumableNotifier(
      context.dispatcher,
      jobManagerGateway,
      new FiniteDuration(config.getTimeout().toMilliseconds, TimeUnit.MILLISECONDS))

    connectionUtils = Some(
      (checkpointResponder,
        partitionStateChecker,
        resultPartitionConsumableNotifier,
        taskManagerConnection))


    val kvStateServer = network.getKvStateServer()

    if (kvStateServer != null) {
      val kvStateRegistry = network.getKvStateRegistry()

      kvStateRegistry.registerListener(
        HighAvailabilityServices.DEFAULT_JOB_ID,
        new ActorGatewayKvStateRegistryListener(
          jobManagerGateway,
          kvStateServer.getServerAddress))
    }

    val proxy = network.getKvStateProxy

    if (proxy != null) {
      proxy.updateKvStateLocationOracle(
        HighAvailabilityServices.DEFAULT_JOB_ID,
        new ActorGatewayKvStateLocationOracle(
          jobManagerGateway,
          config.getTimeout()))
    }

    // start a blob service, if a blob server is specified
    val jmHost = jobManager.path.address.host.getOrElse("localhost")
    val address = new InetSocketAddress(jmHost, blobPort)

    log.info(s"Determined BLOB server address to be $address. Starting BLOB cache.")

    try {
      val blobcache = new BlobCacheService(
        config.getConfiguration(),
        highAvailabilityServices.createBlobStore(),
        address)
      blobCache = Option(blobcache)
      libraryCacheManager = Some(
        new BlobLibraryCacheManager(
          blobcache.getPermanentBlobService,
          config.getClassLoaderResolveOrder(),
          config.getAlwaysParentFirstLoaderPatterns))
      fileCache = Some(
        new FileCache(config.getTmpDirectories(), blobcache.getPermanentBlobService)
      )
    }
    catch {
      case e: Exception =>
        val message = "Could not create BLOB cache or library cache."
        log.error(message, e)
        throw new RuntimeException(message, e)
    }

    // watch job manager to detect when it dies
    context.watch(jobManager)

    // schedule regular heartbeat message for oneself
    heartbeatScheduler = Some(
      context.system.scheduler.schedule(
        TaskManager.HEARTBEAT_INTERVAL,
        TaskManager.HEARTBEAT_INTERVAL,
        self,
        decorateMessage(SendHeartbeat)
      )(context.dispatcher)
    )

    // notify all the actors that listen for a successful registration
    for (listener <- waitForRegistration) {
      listener ! RegisteredAtJobManager(leaderSessionID.orNull)
    }
    waitForRegistration.clear()
  }

  /**
   * Disassociates the TaskManager from the JobManager. This cleans
   * removes all tasks currently running, discards all intermediate
   * results and all cached libraries.
   */
  private def disassociateFromJobManager(): Unit = {
    if (!isConnected) {
      log.warn("TaskManager received message to disassociate from JobManager, even though " +
        "it is not currently associated with a JobManager")
      return
    }

    log.info("Disassociating from JobManager")

    // stop the periodic heartbeats
    heartbeatScheduler foreach {
      _.cancel()
    }
    heartbeatScheduler = None

    // stop the monitoring of the JobManager
    currentJobManager foreach {
      jm => context.unwatch(jm)
    }

    // de-register from the JobManager (faster detection of disconnect)
    currentJobManager foreach {
      _ ! decorateMessage(
        Disconnect(
          instanceID,
          new Exception(s"TaskManager ${self.path} is disassociating")))
    }

    currentJobManager = None
    instanceID = null

    // shut down BLOB and library cache
    fileCache foreach {
      cache => cache.shutdown()
    }
    fileCache = None

    libraryCacheManager foreach {
      manager => manager.shutdown()
    }
    libraryCacheManager = None

    blobCache foreach {
      service =>
        try {
          service.close()
        } catch {
          case ioe: IOException => log.error("Could not properly shutdown blob service.", ioe)
        }
    }
    blobCache = None

    // disassociate the slot environment
    connectionUtils = None

    if (network.getKvStateRegistry != null) {
      network.getKvStateRegistry.unregisterListener(HighAvailabilityServices.DEFAULT_JOB_ID)
    }

    val proxy = network.getKvStateProxy

    if (proxy != null) {
      // clear the key-value location oracle
      proxy.updateKvStateLocationOracle(HighAvailabilityServices.DEFAULT_JOB_ID, null)
    }

    // failsafe shutdown of the metrics registry
    try {
      taskManagerMetricGroup.close()
    } catch {
      case t: Exception => log.warn("TaskManagerMetricGroup could not be closed successfully.", t)
    }
  }

  protected def handleJobManagerDisconnect(msg: String): Unit = {
    if (isConnected) {
      val jobManager = currentJobManager.orNull
      // check if it comes from our JobManager
      try {
        val message = s"TaskManager ${self.path} disconnects from JobManager " +
          s"${jobManager.path}: " + msg
        log.info(message)

        // cancel all our tasks with a proper error message
        cancelAndClearEverything(new Exception(message))

        // reset our state to disassociated
        disassociateFromJobManager()
      }
      catch {
        // this is pretty bad, it leaves the TaskManager in a state where it cannot
        // cleanly reconnect
        case t: Throwable =>
          killTaskManagerFatal("Failed to disassociate from the JobManager", t)
      }
    }
  }

  // --------------------------------------------------------------------------
  //  Task Operations
  // --------------------------------------------------------------------------

  /**
   * Receives a [[TaskDeploymentDescriptor]] describing the task to be executed. It eagerly
   * acknowledges the task reception to the sender and asynchronously starts the initialization of
   * the task.
   *
   * @param tdd TaskDeploymentDescriptor describing the task to be executed on this [[TaskManager]]
   */
  private def submitTask(tdd: TaskDeploymentDescriptor): Unit = {
    try {
      // grab some handles and sanity check on the fly
      val jobManagerActor = currentJobManager match {
        case Some(jm) => jm
        case None =>
          throw new IllegalStateException("TaskManager is not associated with a JobManager.")
      }
      val libCache = libraryCacheManager match {
        case Some(manager) => manager
        case None => throw new IllegalStateException("There is no valid library cache manager.")
      }
      val blobCache = this.blobCache match {
        case Some(manager) => manager
        case None => throw new IllegalStateException("There is no valid BLOB cache.")
      }

      val fileCache = this.fileCache match {
        case Some(manager) => manager
        case None => throw new IllegalStateException("There is no valid file cache.")
      }

      val slot = tdd.getTargetSlotNumber
      if (slot < 0 || slot >= numberOfSlots) {
        throw new IllegalArgumentException(s"Target slot $slot does not exist on TaskManager.")
      }

      val (checkpointResponder,
        partitionStateChecker,
        resultPartitionConsumableNotifier,
        taskManagerConnection) = connectionUtils match {
        case Some(x) => x
        case None => throw new IllegalStateException("The connection utils have not been " +
                                                       "initialized.")
      }

      // create the task. this does not grab any TaskManager resources or download
      // any libraries except for offloaded TaskDeploymentDescriptor data which
      // was too big for the RPC - the operation may only block for the latter

      val jobManagerGateway = new AkkaActorGateway(jobManagerActor, leaderSessionID.orNull)

      try {
        tdd.loadBigData(blobCache.getPermanentBlobService);
      } catch {
        case e @ (_: IOException | _: ClassNotFoundException) =>
          throw new IOException("Could not deserialize the job information.", e)
      }

      val jobInformation = try {
        tdd.getSerializedJobInformation.deserializeValue(getClass.getClassLoader)
      } catch {
        case e @ (_: IOException | _: ClassNotFoundException) =>
          throw new IOException("Could not deserialize the job information.", e)
      }
      if (tdd.getJobId != jobInformation.getJobId) {
        throw new IOException(
          "Inconsistent job ID information inside TaskDeploymentDescriptor (" +
          tdd.getJobId + " vs. " + jobInformation.getJobId + ")")
      }

      val taskInformation = try {
        tdd.getSerializedTaskInformation.deserializeValue(getClass.getClassLoader)
      } catch {
        case e@(_: IOException | _: ClassNotFoundException) =>
          throw new IOException("Could not deserialize the job vertex information.", e)
      }

      val taskMetricGroup = taskManagerMetricGroup.addTaskForJob(
        jobInformation.getJobId,
        jobInformation.getJobName,
        taskInformation.getJobVertexId,
        tdd.getExecutionAttemptId,
        taskInformation.getTaskName,
        tdd.getSubtaskIndex,
        tdd.getAttemptNumber)

      val inputSplitProvider = new TaskInputSplitProvider(
        jobManagerGateway,
        jobInformation.getJobId,
        taskInformation.getJobVertexId,
        tdd.getExecutionAttemptId,
        new FiniteDuration(
          config.getTimeout().getSize(),
          config.getTimeout().getUnit()))

      val jobID = jobInformation.getJobId

      // Allocation ids do not work properly in legacy mode, so we just fake one, based on the jid.
      val fakeAllocationID = new AllocationID(jobID.getLowerPart, jobID.getUpperPart)

      val taskLocalStateStore = taskManagerLocalStateStoresManager.localStateStoreForSubtask(
        jobID,
        fakeAllocationID,
        taskInformation.getJobVertexId,
        tdd.getSubtaskIndex)

      val taskStateManager = new TaskStateManagerImpl(
        jobID,
        tdd.getExecutionAttemptId,
        taskLocalStateStore,
        tdd.getTaskRestore,
        checkpointResponder)

      val task = new Task(
        jobInformation,
        taskInformation,
        tdd.getExecutionAttemptId,
        tdd.getAllocationId,
        tdd.getSubtaskIndex,
        tdd.getAttemptNumber,
        tdd.getProducedPartitions,
        tdd.getInputGates,
        tdd.getTargetSlotNumber,
        memoryManager,
        ioManager,
        network,
        bcVarManager,
        taskStateManager,
        taskManagerConnection,
        inputSplitProvider,
        checkpointResponder,
        blobCache,
        libCache,
        fileCache,
        config,
        taskMetricGroup,
        resultPartitionConsumableNotifier,
        partitionStateChecker,
        context.dispatcher)

      log.info(s"Received task ${task.getTaskInfo.getTaskNameWithSubtasks()}")

      val execId = tdd.getExecutionAttemptId
      // add the task to the map
      val prevTask = runningTasks.put(execId, task)
      if (prevTask != null) {
        // already have a task for that ID, put if back and report an error
        runningTasks.put(execId, prevTask)
        throw new IllegalStateException("TaskManager already contains a task for id " + execId)
      }

      // all good, we kick off the task, which performs its own initialization
      task.startTaskThread()

      sender ! decorateMessage(Acknowledge.get())
    }
    catch {
      case t: Throwable =>
        log.error("SubmitTask failed", t)
        sender ! decorateMessage(Status.Failure(t))
    }
  }

  /**
   * Informs a task about additional locations of its input data partitions.
   *
   * @param executionId The execution attempt ID of the task.
   * @param partitionInfos The descriptor of the intermediate result partitions.
   */
  private def updateTaskInputPartitions(
       executionId: ExecutionAttemptID,
       partitionInfos: java.lang.Iterable[PartitionInfo])
    : Unit = {

    Option(runningTasks.get(executionId)) match {
      case Some(task) =>

        val errors: Iterable[String] = partitionInfos.asScala.flatMap { info =>

          val resultID = info.getIntermediateDataSetID
          val partitionInfo = info.getInputChannelDeploymentDescriptor
          val reader = task.getInputGateById(resultID)

          if (reader != null) {
            Future {
              try {
                reader.updateInputChannel(partitionInfo)
              }
              catch {
                case t: Throwable =>
                  log.error(s"Could not update input data location for task " +
                    s"${task.getTaskInfo.getTaskName}. Trying to fail  task.", t)

                  try {
                    task.failExternally(t)
                  }
                  catch {
                    case t: Throwable =>
                      log.error("Failed canceling task with execution ID " + executionId +
                        " after task update failure.", t)
                  }
              }
            }(context.dispatcher)
            None
          }
          else {
            Some(s"No reader with ID $resultID for task $executionId was found.")
          }
        }

        if (errors.isEmpty) {
          sender ! decorateMessage(Acknowledge.get())
        } else {
          sender ! decorateMessage(Status.Failure(new Exception(errors.mkString("\n"))))
        }

      case None =>
        log.debug(s"Discard update for input partitions of task $executionId : " +
          s"task is no longer running.")
        sender ! decorateMessage(Acknowledge.get())
    }
  }

  /**
   * Marks all tasks currently registered as failed (with the given
   * cause) and removes them.
   *
   * @param cause The exception given to the tasks as the failure reason.
   */
  private def cancelAndClearEverything(cause: Throwable) {
    if (runningTasks.size > 0) {
      log.info("Cancelling all computations and discarding all cached data.")

      for (t <- runningTasks.values().asScala) {
        t.failExternally(cause)
      }
      runningTasks.clear()
    }
  }

  private def unregisterTaskAndNotifyFinalState(executionID: ExecutionAttemptID): Unit = {

    val task = runningTasks.remove(executionID)
    if (task != null) {

      // the task must be in a terminal state
      if (!task.getExecutionState.isTerminal) {
        try {
          task.failExternally(new Exception("Task is being removed from TaskManager"))
        } catch {
          case e: Exception => log.error("Could not properly fail task", e)
        }
      }

      log.info(s"Un-registering task and sending final execution state " +
        s"${task.getExecutionState} to JobManager for task ${task.getTaskInfo.getTaskName} " +
        s"(${task.getExecutionId})")

      val accumulators = {
        val registry = task.getAccumulatorRegistry
        registry.getSnapshot
      }

      self ! decorateMessage(
        UpdateTaskExecutionState(
          new TaskExecutionState(
            task.getJobID,
            task.getExecutionId,
            task.getExecutionState,
            task.getFailureCause,
            accumulators,
            task.getMetricGroup.getIOMetricGroup.createSnapshot())
        )
      )
    }
    else {
      log.error(s"Cannot find task with ID $executionID to unregister.")
    }
  }

  // --------------------------------------------------------------------------
  //  Miscellaneous actions
  // --------------------------------------------------------------------------

  /**
   * Sends a heartbeat message to the JobManager (if connected) with the current
   * metrics report.
   */
  protected def sendHeartbeatToJobManager(): Unit = {
    try {
      log.debug("Sending heartbeat to JobManager")

      val accumulatorEvents =
        scala.collection.mutable.Buffer[AccumulatorSnapshot]()

      runningTasks.asScala foreach {
        case (execID, task) =>
          try {
            val registry = task.getAccumulatorRegistry
            val accumulators = registry.getSnapshot
            accumulatorEvents.append(accumulators)
          } catch {
            case e: Exception =>
              log.warn("Failed to take accumulator snapshot for task {}.",
                execID, ExceptionUtils.getRootCause(e))
          }
      }

       currentJobManager foreach {
        jm => jm ! decorateMessage(Heartbeat(instanceID, accumulatorEvents))
      }
    }
    catch {
      case e: Exception => log.warn("Error sending the metric heartbeat to the JobManager", e)
    }
  }

  /**
   * Sends a message with the stack trace of all threads to the given recipient.
   *
   * @param recipient The target of the stack trace message
   */
  private def sendStackTrace(recipient: ActorRef): Unit = {
    if (recipient == null) {
      return
    }

    try {
      val traces = Thread.getAllStackTraces.asScala
      val stackTraceStr = traces.map {
        case (thread: Thread, elements: Array[StackTraceElement]) =>
          "Thread: " + thread.getName + '\n' + elements.mkString("\n")
        }.mkString("\n\n")

      recipient ! decorateMessage(new StackTrace(instanceID, stackTraceStr))
    }
    catch {
      case e: Exception => log.error("Failed to send stack trace to " + recipient.path, e)
    }
  }

  /**
   * Prints a big error message in the log and kills the TaskManager actor.
   *
   * @param cause The exception that caused the fatal problem.
   */
  private def killTaskManagerFatal(message: String, cause: Throwable): Unit = {
    log.error("\n" +
      "==============================================================\n" +
      "======================      FATAL      =======================\n" +
      "==============================================================\n" +
      "\n" +
      "A fatal error occurred, forcing the TaskManager to shut down: " + message, cause)

    self ! Kill
  }

  override def notifyLeaderAddress(leaderAddress: String, leaderSessionID: UUID): Unit = {
    self ! JobManagerLeaderAddress(leaderAddress, leaderSessionID)
  }

  /** Handles the notification about a new leader and its address. If the TaskManager is still
    * connected to a JobManager, it first disconnects from it. It then retrieves the new
    * JobManager address from the new leading JobManager and starts the registration process.
    *
    * @param newJobManagerAkkaURL Akka URL of the new job manager
    * @param leaderSessionID New leader session ID associated with the leader
    */
  private def handleJobManagerLeaderAddress(
      newJobManagerAkkaURL: String,
      leaderSessionID: UUID)
    : Unit = {

    currentJobManager match {
      case Some(jm) =>
        Option(newJobManagerAkkaURL) match {
          case Some(newJMAkkaURL) =>
            handleJobManagerDisconnect(s"JobManager $newJMAkkaURL was elected as leader.")
          case None =>
            handleJobManagerDisconnect(s"Old JobManager lost its leadership.")
        }
      case None =>
    }

    this.jobManagerAkkaURL = Option(newJobManagerAkkaURL)
    this.leaderSessionID = Option(leaderSessionID)

    if (this.leaderSessionID.isDefined) {
      // only trigger the registration if we have obtained a valid leader id (!= null)
      triggerTaskManagerRegistration()
    }
  }

  /** Starts the TaskManager's registration process to connect to the JobManager.
    */
  def triggerTaskManagerRegistration(): Unit = {
    if(jobManagerAkkaURL.isDefined) {
      // begin attempts to reconnect
      val deadline: Option[Deadline] = Option(config.getMaxRegistrationDuration())
        .map{ duration => new FiniteDuration(duration.getSize(), duration.getUnit()).fromNow }

      // start a new registration run
      currentRegistrationRun = UUID.randomUUID()

      scheduledTaskManagerRegistration.foreach(_.cancel())

      self ! decorateMessage(
        TriggerTaskManagerRegistration(
          jobManagerAkkaURL.get,
          new FiniteDuration(
            config.getInitialRegistrationPause().getSize(),
            config.getInitialRegistrationPause().getUnit()),
          deadline,
          1,
          currentRegistrationRun)
      )
    }
  }

  override def handleError(exception: Exception): Unit = {
    log.error("Error in leader retrieval service", exception)

    self ! decorateMessage(PoisonPill)
  }

  protected def shutdown(): Unit = {
    context.system.shutdown()

    // Await actor system termination and shut down JVM
    new ProcessShutDownThread(
      log.logger,
      context.system,
      FiniteDuration(10, SECONDS)).start()
  }
}

/**
 * TaskManager companion object. Contains TaskManager executable entry point, command
 * line parsing, constants, and setup methods for the TaskManager.
 */
object TaskManager {

  /** TaskManager logger for synchronous logging (not through the logging actor) */
  val LOG = Logger(classOf[TaskManager])

  /** Return code for unsuccessful TaskManager startup */
  val STARTUP_FAILURE_RETURN_CODE = 1

  /** Return code for critical errors during the runtime */
  val RUNTIME_FAILURE_RETURN_CODE = 2

  /** Maximum time (milli seconds) that the TaskManager will spend searching for a
    * suitable network interface to use for communication */
  val MAX_STARTUP_CONNECT_TIME = 120000L

  /** Time (milli seconds) after which the TaskManager will start logging failed
    * connection attempts */
  val STARTUP_CONNECT_LOG_SUPPRESS = 10000L

  val HEARTBEAT_INTERVAL: FiniteDuration = 5000 milliseconds


  // --------------------------------------------------------------------------
  //  TaskManager standalone entry point
  // --------------------------------------------------------------------------

  /**
   * Entry point (main method) to run the TaskManager in a standalone fashion.
   *
   * @param args The command line arguments.
   */
  def main(args: Array[String]): Unit = {
    // startup checks and logging
    EnvironmentInformation.logEnvironmentInfo(LOG.logger, "TaskManager", args)
    SignalHandler.register(LOG.logger)
    JvmShutdownSafeguard.installAsShutdownHook(LOG.logger)

    val maxOpenFileHandles = EnvironmentInformation.getOpenFileHandlesLimit()
    if (maxOpenFileHandles != -1) {
      LOG.info(s"Maximum number of open file descriptors is $maxOpenFileHandles")
    } else {
      LOG.info("Cannot determine the maximum number of open file descriptors")
    }

    // try to parse the command line arguments
    val configuration: Configuration = try {
      parseArgsAndLoadConfig(args)
    }
    catch {
      case t: Throwable =>
        LOG.error(t.getMessage(), t)
        System.exit(STARTUP_FAILURE_RETURN_CODE)
        null
    }

    // In Standalone mode, we generate a resource identifier.
    val resourceId = ResourceID.generate()

    // run the TaskManager (if requested in an authentication enabled context)
    SecurityUtils.install(new SecurityConfiguration(configuration))

    try {
      SecurityUtils.getInstalledContext.runSecured(new Callable[Unit] {
        override def call(): Unit = {
          selectNetworkInterfaceAndRunTaskManager(configuration, resourceId, classOf[TaskManager])
        }
      })
    }
    catch {
      case t: Throwable =>
        LOG.error("Failed to run TaskManager.", t)
        System.exit(STARTUP_FAILURE_RETURN_CODE)
    }
  }

  /**
   * Parse the command line arguments of the TaskManager and loads the configuration.
   *
   * @param args Command line arguments
   * @return The parsed configuration.
   */
  @throws(classOf[Exception])
  def parseArgsAndLoadConfig(args: Array[String]): Configuration = {

    // set up the command line parser
    val parser = new scopt.OptionParser[TaskManagerCliOptions]("TaskManager") {
      head("Flink TaskManager")

      opt[String]("configDir") action { (param, conf) =>
        conf.setConfigDir(param)
        conf
      } text {
        "Specify configuration directory."
      }
    }

    // parse the CLI arguments
    val cliConfig = parser.parse(args, new TaskManagerCliOptions()).getOrElse {
      throw new Exception(
        s"Invalid command line arguments: ${args.mkString(" ")}. Usage: ${parser.usage}")
    }

    // load the configuration
    val conf: Configuration = try {
      LOG.info("Loading configuration from " + cliConfig.getConfigDir())
      GlobalConfiguration.loadConfiguration(cliConfig.getConfigDir())
    }
    catch {
      case e: Exception => throw new Exception("Could not load configuration", e)
    }

    try {
      FileSystem.initialize(conf)
    }
    catch {
      case e: IOException => {
        throw new Exception("Error while setting the default " +
          "filesystem scheme from configuration.", e)
      }
    }

    conf
  }

  // --------------------------------------------------------------------------
  //  Starting and running the TaskManager
  // --------------------------------------------------------------------------

  /**
   * Starts and runs the TaskManager.
   *
   * This method first tries to select the network interface to use for the TaskManager
   * communication. The network interface is used both for the actor communication
   * (coordination) as well as for the data exchange between task managers. Unless
   * the hostname/interface is explicitly configured in the configuration, this
   * method will try out various interfaces and methods to connect to the JobManager
   * and select the one where the connection attempt is successful.
   *
   * After selecting the network interface, this method brings up an actor system
   * for the TaskManager and its actors, starts the TaskManager's services
   * (library cache, shuffle network stack, ...), and starts the TaskManager itself.
   *
   * @param configuration The configuration for the TaskManager.
   * @param taskManagerClass The actor class to instantiate.
   *                         Allows to use TaskManager subclasses for example for YARN.
   */
  @throws(classOf[Exception])
  def selectNetworkInterfaceAndRunTaskManager(
      configuration: Configuration,
      resourceID: ResourceID,
      taskManagerClass: Class[_ <: TaskManager])
    : Unit = {

    val highAvailabilityServices = HighAvailabilityServicesUtils.createHighAvailabilityServices(
      configuration,
      Executors.directExecutor(),
      AddressResolution.TRY_ADDRESS_RESOLUTION)

    val (taskManagerHostname, actorSystemPortRange) = selectNetworkInterfaceAndPortRange(
      configuration,
      highAvailabilityServices)

    try {
      runTaskManager(
        taskManagerHostname,
        resourceID,
        actorSystemPortRange,
        configuration,
        highAvailabilityServices,
        taskManagerClass)
    } finally {
      try {
        highAvailabilityServices.close()
      } catch {
        case t: Throwable => LOG.warn("Could not properly stop the high availability services.", t)
      }
    }
  }

  @throws(classOf[IOException])
  @throws(classOf[IllegalConfigurationException])
  def selectNetworkInterfaceAndPortRange(
      configuration: Configuration,
      highAvailabilityServices: HighAvailabilityServices)
    : (String, java.util.Iterator[Integer]) = {

    var taskManagerHostname = configuration.getString(
      ConfigConstants.TASK_MANAGER_HOSTNAME_KEY, null)

    if (taskManagerHostname != null) {
      LOG.info("Using configured hostname/address for TaskManager: " + taskManagerHostname)
    }
    else {
      val lookupTimeout = AkkaUtils.getLookupTimeout(configuration)

      val taskManagerAddress = LeaderRetrievalUtils.findConnectingAddress(
        highAvailabilityServices.getJobManagerLeaderRetriever(
          HighAvailabilityServices.DEFAULT_JOB_ID),
        lookupTimeout)

      taskManagerHostname = taskManagerAddress.getHostName()
      LOG.info(s"TaskManager will use hostname/address '$taskManagerHostname' " +
        s"(${taskManagerAddress.getHostAddress()}) for communication.")
    }

    // if no task manager port has been configured, use 0 (system will pick any free port)
    val portRange = configuration.getString(TaskManagerOptions.RPC_PORT)

    val portRangeIterator = try {
      NetUtils.getPortRangeFromString(portRange)
    } catch {
      case _: NumberFormatException =>
        throw new IllegalConfigurationException("Invalid value for '" +
          TaskManagerOptions.RPC_PORT.key() +
          "' (port for the TaskManager actor system) : " + portRange +
          " - Leave config parameter empty or use 0 to let the system choose a port automatically.")
    }

    (taskManagerHostname, portRangeIterator)

  }

  /**
   * Starts and runs the TaskManager. Brings up an actor system for the TaskManager and its
   * actors, starts the TaskManager's services (library cache, shuffle network stack, ...),
   * and starts the TaskManager itself.
   *
   * This method will also spawn a process reaper for the TaskManager (kill the process if
   * the actor fails) and optionally start the JVM memory logging thread.
   *
   * @param taskManagerHostname The hostname/address of the interface where the actor system
   *                         will communicate.
   * @param resourceID The id of the resource which the task manager will run on.
   * @param actorSystemPort The port at which the actor system will communicate.
   * @param configuration The configuration for the TaskManager.
   * @param highAvailabilityServices Service factory for high availability services
   */
  @throws(classOf[Exception])
  def runTaskManager(
      taskManagerHostname: String,
      resourceID: ResourceID,
      actorSystemPort: Int,
      configuration: Configuration,
      highAvailabilityServices: HighAvailabilityServices)
    : Unit = {

    runTaskManager(
      taskManagerHostname,
      resourceID,
      actorSystemPort,
      configuration,
      highAvailabilityServices,
      classOf[TaskManager])
  }

  /**
   * Starts and runs the TaskManager. Brings up an actor system for the TaskManager and its
   * actors, starts the TaskManager's services (library cache, shuffle network stack, ...),
   * and starts the TaskManager itself.
   *
   * This method will also spawn a process reaper for the TaskManager (kill the process if
   * the actor fails) and optionally start the JVM memory logging thread.
   *
   * @param taskManagerHostname The hostname/address of the interface where the actor system
   *                         will communicate.
   * @param resourceID The id of the resource which the task manager will run on.
   * @param actorSystemPort The port at which the actor system will communicate.
   * @param configuration The configuration for the TaskManager.
   * @param highAvailabilityServices Service factory for high availability services
   * @param taskManagerClass The actor class to instantiate. Allows the use of TaskManager
   *                         subclasses for example for YARN.
   */
  @throws(classOf[Exception])
  def runTaskManager(
      taskManagerHostname: String,
      resourceID: ResourceID,
      actorSystemPort: Int,
      configuration: Configuration,
      highAvailabilityServices: HighAvailabilityServices,
      taskManagerClass: Class[_ <: TaskManager])
    : Unit = {

    LOG.info("Starting TaskManager")

    // Bring up the TaskManager actor system first, bind it to the given address.

    LOG.info(s"Starting TaskManager actor system at $taskManagerHostname:$actorSystemPort.")

    val taskManagerSystem = BootstrapTools.startActorSystem(
      configuration,
      taskManagerHostname,
      actorSystemPort,
      LOG.logger)

    val metricRegistry = new MetricRegistryImpl(
      MetricRegistryConfiguration.fromConfiguration(configuration))

    metricRegistry.startQueryService(taskManagerSystem, resourceID)

    // start all the TaskManager services (network stack,  library cache, ...)
    // and the TaskManager actor
    try {

      LOG.info("Starting TaskManager actor")
      val taskManager = startTaskManagerComponentsAndActor(
        configuration,
        resourceID,
        taskManagerSystem,
        highAvailabilityServices,
        metricRegistry,
        taskManagerHostname,
        Some(TaskExecutor.TASK_MANAGER_NAME),
        localTaskManagerCommunication = false,
        taskManagerClass)

      // start a process reaper that watches the JobManager. If the TaskManager actor dies,
      // the process reaper will kill the JVM process (to ensure easy failure detection)
      LOG.debug("Starting TaskManager process reaper")
      taskManagerSystem.actorOf(
        Props(classOf[ProcessReaper], taskManager, LOG.logger, RUNTIME_FAILURE_RETURN_CODE),
        "TaskManager_Process_Reaper")

      if (configuration.getBoolean(TaskManagerOptions.EXIT_ON_FATAL_AKKA_ERROR)) {
        val quarantineHandler = new DefaultQuarantineHandler(
          Time.milliseconds(AkkaUtils.getTimeout(configuration).toMillis),
          RUNTIME_FAILURE_RETURN_CODE,
          LOG.logger)

        LOG.debug("Starting TaskManager quarantine monitor")
        taskManagerSystem.actorOf(
          Props(classOf[QuarantineMonitor], quarantineHandler, LOG.logger)
        )
      }

      // if desired, start the logging daemon that periodically logs the
      // memory usage information
      if (LOG.isInfoEnabled && configuration.getBoolean(
        TaskManagerOptions.DEBUG_MEMORY_LOG))
      {
        LOG.info("Starting periodic memory usage logger")

        val interval = configuration.getLong(
          TaskManagerOptions.DEBUG_MEMORY_USAGE_LOG_INTERVAL_MS)

        val logger = new MemoryLogger(LOG.logger, interval, taskManagerSystem)
        logger.start()
      }

      // block until everything is done
      taskManagerSystem.awaitTermination()
    } catch {
      case t: Throwable =>
        LOG.error("Error while starting up taskManager", t)
        try {
          taskManagerSystem.shutdown()
        } catch {
          case tt: Throwable => LOG.warn("Could not cleanly shut down actor system", tt)
        }
        throw t
    }

    // shut down the metric query service
    try {
      metricRegistry.shutdown().get()
    } catch {
      case t: Throwable =>
        LOG.error("Could not properly shut down the metric registry.", t)
    }
  }

  /**
    * Starts and runs the TaskManager with all its components trying to bind to
    * a port in the specified range.
    *
    * @param taskManagerHostname The hostname/address of the interface where the actor system
    *                         will communicate.
    * @param resourceID The id of the resource which the task manager will run on.
    * @param actorSystemPortRange The port at which the actor system will communicate.
    * @param configuration The configuration for the TaskManager.
    * @param taskManagerClass The actor class to instantiate. Allows the use of TaskManager
    *                         subclasses for example for YARN.
    */
  @throws(classOf[Exception])
  def runTaskManager(
    taskManagerHostname: String,
    resourceID: ResourceID,
    actorSystemPortRange: java.util.Iterator[Integer],
    configuration: Configuration,
    highAvailabilityServices: HighAvailabilityServices,
    taskManagerClass: Class[_ <: TaskManager])
    : Unit = {

    val result = AkkaUtils.retryOnBindException({
      // Try all ports in the range until successful
      val socket = NetUtils.createSocketFromPorts(
        actorSystemPortRange,
        new NetUtils.SocketFactory {
          override def createSocket(port: Int): ServerSocket = new ServerSocket(
            // Use the correct listening address, bound ports will only be
            // detected later by Akka.
            port, 0, InetAddress.getByName(NetUtils.getWildcardIPAddress))
        })

      val port =
        if (socket == null) {
          throw new BindException(s"Unable to allocate port for TaskManager.")
        } else {
          try {
            socket.getLocalPort()
          } finally {
            socket.close()
          }
        }

      runTaskManager(
        taskManagerHostname,
        resourceID,
        port,
        configuration,
        highAvailabilityServices,
        taskManagerClass)
    }, { !actorSystemPortRange.hasNext }, 5000)

    result match {
      case scala.util.Failure(f) => throw f
      case _ =>
    }
  }

  /**
   * Starts the task manager actor.
   *
   * @param configuration The configuration for the TaskManager.
   * @param resourceID The id of the resource which the task manager will run on.
   * @param actorSystem The actor system that should run the TaskManager actor.
   * @param highAvailabilityServices Factory to create high availability services
   * @param taskManagerHostname The hostname/address that describes the TaskManager's data location.
   * @param taskManagerActorName Optionally the name of the TaskManager actor. If none is given,
   *                             the actor will use a random name.
   * @param localTaskManagerCommunication If true, the TaskManager will not initiate the
   *                                      TCP network stack.
   * @param taskManagerClass The class of the TaskManager actor. May be used to give
   *                         subclasses that understand additional actor messages.
   * @throws org.apache.flink.configuration.IllegalConfigurationException
   *                              Thrown, if the given config contains illegal values.
   * @throws java.io.IOException Thrown, if any of the I/O components (such as buffer pools,
   *                             I/O manager, ...) cannot be properly started.
   * @throws java.lang.Exception Thrown is some other error occurs while parsing the configuration
   *                             or starting the TaskManager components.
   * @return An ActorRef to the TaskManager actor.
   */
  @throws(classOf[IllegalConfigurationException])
  @throws(classOf[IOException])
  @throws(classOf[Exception])
  def startTaskManagerComponentsAndActor(
      configuration: Configuration,
      resourceID: ResourceID,
      actorSystem: ActorSystem,
      highAvailabilityServices: HighAvailabilityServices,
      metricRegistry: FlinkMetricRegistry,
      taskManagerHostname: String,
      taskManagerActorName: Option[String],
      localTaskManagerCommunication: Boolean,
      taskManagerClass: Class[_ <: TaskManager])
    : ActorRef = {

    val taskManagerAddress = InetAddress.getByName(taskManagerHostname)

    val taskManagerServicesConfiguration = TaskManagerServicesConfiguration
      .fromConfiguration(configuration, taskManagerAddress, false)

    val taskManagerConfiguration = TaskManagerConfiguration.fromConfiguration(configuration)

    val taskManagerServices = TaskManagerServices.fromConfiguration(
      taskManagerServicesConfiguration,
      resourceID,
      actorSystem.dispatcher,
      EnvironmentInformation.getSizeOfFreeHeapMemoryWithDefrag,
      EnvironmentInformation.getMaxJvmHeapMemory)

    val taskManagerMetricGroup = MetricUtils.instantiateTaskManagerMetricGroup(
      metricRegistry,
      taskManagerServices.getTaskManagerLocation(),
      taskManagerServices.getNetworkEnvironment())

    // create the actor properties (which define the actor constructor parameters)
    val tmProps = getTaskManagerProps(
      taskManagerClass,
      taskManagerConfiguration,
      resourceID,
      taskManagerServices.getTaskManagerLocation(),
      taskManagerServices.getMemoryManager(),
      taskManagerServices.getIOManager(),
      taskManagerServices.getNetworkEnvironment(),
      taskManagerServices.getTaskManagerStateStore(),
      highAvailabilityServices,
      taskManagerMetricGroup)

    taskManagerActorName match {
      case Some(actorName) => actorSystem.actorOf(tmProps, actorName)
      case None => actorSystem.actorOf(tmProps)
    }
  }

  def getTaskManagerProps(
    taskManagerClass: Class[_ <: TaskManager],
    taskManagerConfig: TaskManagerConfiguration,
    resourceID: ResourceID,
    taskManagerLocation: TaskManagerLocation,
    memoryManager: MemoryManager,
    ioManager: IOManager,
    networkEnvironment: NetworkEnvironment,
    taskStateManager: TaskExecutorLocalStateStoresManager,
    highAvailabilityServices: HighAvailabilityServices,
    taskManagerMetricGroup: TaskManagerMetricGroup
  ): Props = {
    Props(
      taskManagerClass,
      taskManagerConfig,
      resourceID,
      taskManagerLocation,
      memoryManager,
      ioManager,
      networkEnvironment,
      taskStateManager,
      taskManagerConfig.getNumberSlots(),
      highAvailabilityServices,
      taskManagerMetricGroup)
  }

  // --------------------------------------------------------------------------
  //  Resolving the TaskManager actor
  // --------------------------------------------------------------------------

  /**
   * Resolves the TaskManager actor reference in a blocking fashion.
   *
   * @param taskManagerUrl The akka URL of the JobManager.
   * @param system The local actor system that should perform the lookup.
   * @param timeout The maximum time to wait until the lookup fails.
   * @throws java.io.IOException Thrown, if the lookup fails.
   * @return The ActorRef to the TaskManager
   */
  @throws(classOf[IOException])
  def getTaskManagerRemoteReference(
      taskManagerUrl: String,
      system: ActorSystem,
      timeout: FiniteDuration)
    : ActorRef = {
    try {
      val future = AkkaUtils.getActorRefFuture(taskManagerUrl, system, timeout)
      Await.result(future, timeout)
    }
    catch {
      case e @ (_ : ActorNotFound | _ : TimeoutException) =>
        throw new IOException(
          s"TaskManager at $taskManagerUrl not reachable. " +
            s"Please make sure that the TaskManager is running and its port is reachable.", e)

      case e: IOException =>
        throw new IOException("Could not connect to TaskManager at " + taskManagerUrl, e)
    }
  }
}
