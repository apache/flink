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

import java.io.{File, IOException}
import java.net.{InetAddress, InetSocketAddress}
import java.util.UUID
import java.util.concurrent.TimeUnit
import java.lang.reflect.Method
import java.lang.management.{OperatingSystemMXBean, ManagementFactory}

import _root_.akka.actor._
import _root_.akka.pattern.ask
import _root_.akka.util.Timeout

import com.codahale.metrics.{Gauge, MetricFilter, MetricRegistry}
import com.codahale.metrics.json.MetricsModule
import com.codahale.metrics.jvm.{MemoryUsageGaugeSet, GarbageCollectorMetricSet}

import com.fasterxml.jackson.databind.ObjectMapper
import grizzled.slf4j.Logger

import org.apache.flink.configuration._
import org.apache.flink.core.memory.{HybridMemorySegment, HeapMemorySegment, MemorySegmentFactory, MemoryType}
import org.apache.flink.runtime.accumulators.AccumulatorSnapshot
import org.apache.flink.runtime.leaderretrieval.{LeaderRetrievalListener, LeaderRetrievalService}
import org.apache.flink.runtime.messages.TaskMessages._
import org.apache.flink.runtime.messages.checkpoint.{NotifyCheckpointComplete, TriggerCheckpoint, AbstractCheckpointMessage}
import org.apache.flink.runtime.{FlinkActor, LeaderSessionMessageFilter, LogMessages}
import org.apache.flink.runtime.akka.AkkaUtils
import org.apache.flink.runtime.blob.{BlobService, BlobCache}
import org.apache.flink.runtime.broadcast.BroadcastVariableManager
import org.apache.flink.runtime.deployment.{InputChannelDeploymentDescriptor, TaskDeploymentDescriptor}
import org.apache.flink.runtime.execution.librarycache.{BlobLibraryCacheManager, FallbackLibraryCacheManager, LibraryCacheManager}
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID
import org.apache.flink.runtime.filecache.FileCache
import org.apache.flink.runtime.instance.{AkkaActorGateway, HardwareDescription, InstanceConnectionInfo, InstanceID}
import org.apache.flink.runtime.io.disk.iomanager.IOManager.IOMode
import org.apache.flink.runtime.io.disk.iomanager.{IOManager, IOManagerAsync}
import org.apache.flink.runtime.io.network.NetworkEnvironment
import org.apache.flink.runtime.io.network.netty.NettyConfig
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID
import org.apache.flink.runtime.memory.MemoryManager 
import org.apache.flink.runtime.messages.Messages._
import org.apache.flink.runtime.messages.RegistrationMessages._
import org.apache.flink.runtime.messages.TaskManagerMessages._
import org.apache.flink.util.NetUtils
import org.apache.flink.runtime.process.ProcessReaper
import org.apache.flink.runtime.security.SecurityUtils
import org.apache.flink.runtime.security.SecurityUtils.FlinkSecuredRunner
import org.apache.flink.runtime.util.{SignalHandler, LeaderRetrievalUtils, MathUtils, EnvironmentInformation}

import scala.concurrent._
import scala.concurrent.duration._
import scala.concurrent.forkjoin.ForkJoinPool
import scala.util.{Failure, Success}
import scala.collection.JavaConverters._
import scala.collection.JavaConversions._

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
    protected val connectionInfo: InstanceConnectionInfo,
    protected val memoryManager: MemoryManager,
    protected val ioManager: IOManager,
    protected val network: NetworkEnvironment,
    protected val numberOfSlots: Int,
    protected val leaderRetrievalService: LeaderRetrievalService)
  extends FlinkActor
  with LeaderSessionMessageFilter // Mixin order is important: We want to filter after logging
  with LogMessages // Mixin order is important: first we want to support message logging
  with LeaderRetrievalListener {

  override val log = Logger(getClass)

  /** The timeout for all actor ask futures */
  protected val askTimeout = new Timeout(config.timeout)

  /** The TaskManager's physical execution resources */
  protected val resources = HardwareDescription.extractFromSystem(memoryManager.getMemorySize())

  /** Registry of all tasks currently executed by this TaskManager */
  protected val runningTasks = new java.util.HashMap[ExecutionAttemptID, Task]()

  /** Handler for shared broadcast variables (shared between multiple Tasks) */
  protected val bcVarManager = new BroadcastVariableManager()

  /** Handler for distributed files cached by this TaskManager */
  protected val fileCache = new FileCache(config.configuration)

  /** Registry of metrics periodically transmitted to the JobManager */
  private val metricRegistry = TaskManager.createMetricsRegistry()

  /** Metric serialization */
  private val metricRegistryMapper: ObjectMapper = new ObjectMapper()
    .registerModule(
      new MetricsModule(
        TimeUnit.SECONDS,
        TimeUnit.MILLISECONDS,
        false,
        MetricFilter.ALL))

  /** Actors which want to be notified once this task manager has been
      registered at the job manager */
  private val waitForRegistration = scala.collection.mutable.Set[ActorRef]()

  private var blobService: Option[BlobService] = None
  private var libraryCacheManager: Option[LibraryCacheManager] = None
  protected var currentJobManager: Option[ActorRef] = None
  private var jobManagerAkkaURL: Option[String] = None
 
  private var instanceID: InstanceID = null

  private var heartbeatScheduler: Option[Cancellable] = None

  var leaderSessionID: Option[UUID] = None


  private val runtimeInfo = new TaskManagerRuntimeInfo(
       connectionInfo.getHostname(),
       new UnmodifiableConfiguration(config.configuration))
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
    log.info(s"TaskManager data connection information: $connectionInfo")
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
      fileCache.shutdown()
    } catch {
      case t: Exception => log.error("FileCache did not shutdown properly.", t)
    }

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

    case JobManagerLeaderAddress(address, leaderSessionID) => {
      handleJobManagerLeaderAddress(address, leaderSessionID)
    }

    // registration messages for connecting and disconnecting from / to the JobManager
    case message: RegistrationMessage => handleRegistrationMessage(message)

    // ----- miscellaneous messages ----

    // periodic heart beats that transport metrics
    case SendHeartbeat => sendHeartbeatToJobManager()

    // sends the stack trace of this TaskManager to the sender
    case SendStackTrace => sendStackTrace(sender())

    // registers the message sender to be notified once this TaskManager has completed
    // its registration at the JobManager
    case NotifyWhenRegisteredAtAnyJobManager =>
      if (isConnected) {
        sender ! decorateMessage(RegisteredAtJobManager)
      } else {
        waitForRegistration += sender
      }

    // this message indicates that some actor watched by this TaskManager has died
    case Terminated(actor: ActorRef) =>
      if (isConnected && actor == currentJobManager.orNull) {
        handleJobManagerDisconnect(sender(), "JobManager is no longer reachable")
        triggerTaskManagerRegistration()
      }
      else {
        log.warn(s"Received unrecognized disconnect message " +
          s"from ${if (actor == null) null else actor.path}.")
      }

    case Disconnect(msg) =>
      handleJobManagerDisconnect(sender(), "JobManager requested disconnect: " + msg)
      triggerTaskManagerRegistration()

    case FatalError(message, cause) =>
      killTaskManagerFatal(message, cause)
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
          updateTaskInputPartitions(executionID, List((resultID, partitionInfo)))

        // tell the task about the availability of some new input partitions
        case UpdateTaskMultiplePartitionInfos(executionID, partitionInfos) =>
          updateTaskInputPartitions(executionID, partitionInfos)

        // discards intermediate result partitions of a task execution on this TaskManager
        case FailIntermediateResultPartitions(executionID) =>
          log.info("Discarding the results produced by task execution " + executionID)
          if (network.isAssociated) {
            try {
              network.getPartitionManager.releasePartitionsProducedBy(executionID)
            } catch {
              case t: Throwable => killTaskManagerFatal(
              "Fatal leak: Unable to release intermediate result partition data", t)
            }
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
                case Success(result) =>
                  if (!result) {
                  self ! decorateMessage(
                    FailTask(
                      executionID,
                      new Exception("Task has been cancelled on the JobManager."))
                    )
                  }

                case Failure(t) =>
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
            log.debug(s"Cannot find task to fail for execution ${executionID})")
          }

        // cancels a task
        case CancelTask(executionID) =>
          val task = runningTasks.get(executionID)
          if (task != null) {
            task.cancelExecution()
            sender ! decorateMessage(new TaskOperationResult(executionID, true))
          } else {
            log.debug(s"Cannot find task to cancel for execution ${executionID})")
            sender ! decorateMessage(
              new TaskOperationResult(
                executionID,
                false,
              "No task with that execution ID was found.")
            )
          }

        case PartitionState(taskExecutionId, taskResultId, partitionId, state) =>
          Option(runningTasks.get(taskExecutionId)) match {
            case Some(task) =>
              task.onPartitionStateUpdate(taskResultId, partitionId, state)
            case None =>
              log.debug(s"Cannot find task $taskExecutionId to respond with partition state.")
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
        
        log.debug(s"Receiver TriggerCheckpoint ${checkpointId}@${timestamp} for $taskExecutionId.")

        val task = runningTasks.get(taskExecutionId)
        if (task != null) {
          task.triggerCheckpointBarrier(checkpointId, timestamp)
        } else {
          log.debug(s"Taskmanager received a checkpoint request for unknown task $taskExecutionId.")
        }

      case message: NotifyCheckpointComplete =>
        val taskExecutionId = message.getTaskExecutionId
        val checkpointId = message.getCheckpointId
        val timestamp = message.getTimestamp

        log.debug(s"Receiver ConfirmCheckpoint ${checkpointId}@${timestamp} for $taskExecutionId.")

        val task = runningTasks.get(taskExecutionId)
        if (task != null) {
          task.notifyCheckpointComplete(checkpointId)
        } else {
          log.debug(
            s"Taskmanager received a checkpoint confirmation for unknown task $taskExecutionId.")
        }

      // unknown checkpoint message
      case _ => unhandled(actorMessage)
    }
  }

  /**
   * Handler for messages concerning the registration of the TaskManager at
   * the JobManager.
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
        attempt) =>

        if (isConnected) {
          // this may be the case, if we queue another attempt and
          // in the meantime, the registration is acknowledged
          log.debug(
            "TaskManager was triggered to register at JobManager, but is already registered")
        }
        else if (deadline.exists(_.isOverdue())) {
          // we failed to register in time. that means we should quit
          log.error("Failed to register at the JobManager withing the defined maximum " +
            "connect time. Shutting down ...")

          // terminate ourselves (hasta la vista)
          self ! decorateMessage(PoisonPill)
        }
        else {
          if (!jobManagerAkkaURL.equals(Option(jobManagerURL))) {
            throw new Exception("Invalid internal state: Trying to register at JobManager " +
              s"${jobManagerURL} even though the current JobManagerAkkaURL is set to " +
              s"${jobManagerAkkaURL.getOrElse("")}")
          }

          log.info(s"Trying to register at JobManager ${jobManagerURL} " +
            s"(attempt ${attempt}, timeout: ${timeout})")

          val jobManager = context.actorSelection(jobManagerURL)

          jobManager ! decorateMessage(
            RegisterTaskManager(
              connectionInfo,
              resources,
              numberOfSlots)
          )

          // the next timeout computes via exponential backoff with cap
          val nextTimeout = (timeout * 2).min(TaskManager.MAX_REGISTRATION_TIMEOUT)

          // schedule (with our timeout s delay) a check triggers a new registration
          // attempt, if we are not registered by then
          context.system.scheduler.scheduleOnce(
            timeout,
            self,
            decorateMessage(TriggerTaskManagerRegistration(
              jobManagerURL,
              nextTimeout,
              deadline,
              attempt + 1)
            ))(context.dispatcher)
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
        }
        else {
          // not yet connected, so let's associate with that JobManager
          try {
            associateWithJobManager(jobManager, id, blobPort)
          } catch {
            case t: Throwable =>
              killTaskManagerFatal(
                "Unable to start TaskManager components after registering at JobManager", t)
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
        }
        else {
          // not connected, yet, to let's associate
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
          log.error(s"The registration at JobManager ${jobManagerAkkaURL} was refused, " +
            s"because: ${reason}. Retrying later...")

        if(jobManagerAkkaURL.isDefined) {
          // try the registration again after some time
          val delay: FiniteDuration = TaskManager.DELAY_AFTER_REFUSED_REGISTRATION
          val deadline: Option[Deadline] = config.maxRegistrationDuration.map {
            timeout => timeout + delay fromNow
          }

          context.system.scheduler.scheduleOnce(delay) {
            self ! decorateMessage(
              TriggerTaskManagerRegistration(
                jobManagerAkkaURL.get,
                TaskManager.INITIAL_REGISTRATION_TIMEOUT,
                deadline,
                1)
            )
          }(context.dispatcher)
        }
      }
        else {
          // ignore RefuseRegistration messages which arrived after AcknowledgeRegistration
          if (sender() == currentJobManager.orNull) {
            log.warn(s"Received 'RefuseRegistration' from the JobManager (${sender().path})" +
              s" even though this TaskManager is already registered there.")
          }
          else {
            log.warn(s"Ignoring 'RefuseRegistration' from unrelated " +
              s"JobManager (${sender().path})")
          }
        }

      case _ => unhandled(message)
    }
  }


  // --------------------------------------------------------------------------
  //  Task Manager / JobManager association and initialization
  // --------------------------------------------------------------------------

  /**
   * Checks whether the TaskManager is currently connected to its JobManager.
   *
   * @return True, if the TaskManager is currently connected to a JobManager, false otherwise.
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
    if (network.isAssociated || blobService.isDefined) {
      throw new IllegalStateException("JobManager-specific components are already initialized.")
    }

    currentJobManager = Some(jobManager)
    instanceID = id

    // start the network stack, now that we have the JobManager actor reference
    try {
      network.associateWithTaskManagerAndJobManager(
        new AkkaActorGateway(jobManager, leaderSessionID.orNull),
        new AkkaActorGateway(self, leaderSessionID.orNull)
      )


    }
    catch {
      case e: Exception =>
        val message = "Could not start network environment."
        log.error(message, e)
        throw new RuntimeException(message, e)
    }

    // start a blob service, if a blob server is specified
    if (blobPort > 0) {
      val jmHost = jobManager.path.address.host.getOrElse("localhost")
      val address = new InetSocketAddress(jmHost, blobPort)

      log.info(s"Determined BLOB server address to be $address. Starting BLOB cache.")

      try {
        val blobcache = new BlobCache(address, config.configuration)
        blobService = Option(blobcache)
        libraryCacheManager = Some(new BlobLibraryCacheManager(blobcache, config.cleanupInterval))
      }
      catch {
        case e: Exception =>
          val message = "Could not create BLOB cache or library cache."
          log.error(message, e)
          throw new RuntimeException(message, e)
      }
    }
    else {
      libraryCacheManager = Some(new FallbackLibraryCacheManager)
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
      listener ! RegisteredAtJobManager
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
      _ ! decorateMessage(Disconnect(s"TaskManager ${self.path} is disassociating"))
    }

    currentJobManager = None
    instanceID = null

    // shut down BLOB and library cache
    libraryCacheManager foreach {
      manager => manager.shutdown()
    }
    libraryCacheManager = None

    blobService foreach {
      service => service.shutdown()
    }
    blobService = None

    // disassociate the network environment
    network.disassociate()
  }

  protected def handleJobManagerDisconnect(jobManager: ActorRef, msg: String): Unit = {
    if (isConnected && jobManager != null) {

      // check if it comes from our JobManager
      if (jobManager == currentJobManager.orNull) {
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
      else {
        log.warn(s"Received erroneous JobManager disconnect message for ${jobManager.path}.")
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

      val slot = tdd.getTargetSlotNumber
      if (slot < 0 || slot >= numberOfSlots) {
        throw new IllegalArgumentException(s"Target slot $slot does not exist on TaskManager.")
      }

      // create the task. this does not grab any TaskManager resources or download
      // and libraries - the operation does not block

      val jobManagerGateway = new AkkaActorGateway(jobManagerActor, leaderSessionID.orNull)
      val selfGateway = new AkkaActorGateway(self, leaderSessionID.orNull)

      val task = new Task(
        tdd,
        memoryManager,
        ioManager,
        network,
        bcVarManager,
        selfGateway,
        jobManagerGateway,
        config.timeout,
        libCache,
        fileCache,
        runtimeInfo)

      log.info(s"Received task ${task.getTaskInfo.getTaskNameWithSubtasks}")

      val execId = tdd.getExecutionId
      // add the task to the map
      val prevTask = runningTasks.put(execId, task)
      if (prevTask != null) {
        // already have a task for that ID, put if back and report an error
        runningTasks.put(execId, prevTask)
        throw new IllegalStateException("TaskManager already contains a task for id " + execId)
      }
      
      // all good, we kick off the task, which performs its own initialization
      task.startTaskThread()
      
      sender ! decorateMessage(Acknowledge)
    }
    catch {
      case t: Throwable => 
        log.error("SubmitTask failed", t)
        sender ! decorateMessage(Failure(t))
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
       partitionInfos: Seq[(IntermediateDataSetID, InputChannelDeploymentDescriptor)])
    : Unit = {

    Option(runningTasks.get(executionId)) match {
      case Some(task) =>

        val errors: Seq[String] = partitionInfos.flatMap { info =>

          val (resultID, partitionInfo) = info
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
          sender ! decorateMessage(Acknowledge)
        } else {
          sender ! decorateMessage(Failure(new Exception(errors.mkString("\n"))))
        }

      case None =>
        log.debug(s"Discard update for input partitions of task $executionId : " +
          s"task is no longer running.")
        sender ! decorateMessage(Acknowledge)
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

      log.info(s"Unregistering task and sending final execution state " +
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
              accumulators)
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
      val metricsReport: Array[Byte] = metricRegistryMapper.writeValueAsBytes(metricRegistry)

      val accumulatorEvents =
        scala.collection.mutable.Buffer[AccumulatorSnapshot]()

      runningTasks foreach {
        case (execID, task) =>
          val registry = task.getAccumulatorRegistry
          val accumulators = registry.getSnapshot
          accumulatorEvents.append(accumulators)
      }

       currentJobManager foreach {
        jm => jm ! decorateMessage(Heartbeat(instanceID, metricsReport, accumulatorEvents))
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

      recipient ! decorateMessage(StackTrace(instanceID, stackTraceStr))
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

    self ! decorateMessage(Kill)
  }

  override def notifyLeaderAddress(leaderAddress: String, leaderSessionID: UUID): Unit = {
    self ! JobManagerLeaderAddress(leaderAddress, leaderSessionID)
  }

  /** Handles the notification about a new leader and its address. If the TaskManager is still
    * connected to another JobManager, it first disconnects from it. If the new JobManager
    * address is not null, then it starts the registration process.
    *
    * @param newJobManagerAkkaURL
    * @param leaderSessionID
    */
  private def handleJobManagerLeaderAddress(
      newJobManagerAkkaURL: String,
      leaderSessionID: UUID)
    : Unit = {

    currentJobManager match {
      case Some(jm) =>
        Option(newJobManagerAkkaURL) match {
          case Some(newJMAkkaURL) =>
            handleJobManagerDisconnect(jm, s"JobManager ${newJMAkkaURL} was elected as leader.")
          case None =>
            handleJobManagerDisconnect(jm, s"Old JobManager lost its leadership.")
        }
      case None =>
    }

    this.jobManagerAkkaURL = Option(newJobManagerAkkaURL)
    this.leaderSessionID = Option(leaderSessionID)

    triggerTaskManagerRegistration()
  }

  /** Starts the TaskManager's registration process to connect to the JobManager.
    *
    */
  def triggerTaskManagerRegistration(): Unit = {
    if(jobManagerAkkaURL.isDefined) {
      // begin attempts to reconnect
      val deadline: Option[Deadline] = config.maxRegistrationDuration.map(_.fromNow)

      self ! decorateMessage(
        TriggerTaskManagerRegistration(
          jobManagerAkkaURL.get,
          TaskManager.INITIAL_REGISTRATION_TIMEOUT,
          deadline,
          1)
      )
    }
  }

  override def handleError(exception: Exception): Unit = {
    log.error("Error in leader retrieval service", exception)

    self ! decorateMessage(PoisonPill)
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

  /** The name of the TaskManager actor */
  val TASK_MANAGER_NAME = "taskmanager"

  /** Maximum time (msecs) that the TaskManager will spend searching for a
    * suitable network interface to use for communication */
  val MAX_STARTUP_CONNECT_TIME = 120000L

  /** Time (msecs) after which the TaskManager will start logging failed
    * connection attempts */
  val STARTUP_CONNECT_LOG_SUPPRESS = 10000L

  val INITIAL_REGISTRATION_TIMEOUT: FiniteDuration = 500 milliseconds
  val MAX_REGISTRATION_TIMEOUT: FiniteDuration = 30 seconds

  val DELAY_AFTER_REFUSED_REGISTRATION: FiniteDuration = 10 seconds

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
    EnvironmentInformation.checkJavaVersion()
    SignalHandler.register(LOG.logger)

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
      case t: Throwable => {
        LOG.error(t.getMessage(), t)
        System.exit(STARTUP_FAILURE_RETURN_CODE)
        null
      }
    }

    // run the TaskManager (if requested in an authentication enabled context)
    try {
      if (SecurityUtils.isSecurityEnabled) {
        LOG.info("Security is enabled. Starting secure TaskManager.")
        SecurityUtils.runSecured(new FlinkSecuredRunner[Unit] {
          override def run(): Unit = {
            selectNetworkInterfaceAndRunTaskManager(configuration, classOf[TaskManager])
          }
        })
      }
      else {
        LOG.info("Security is not enabled. Starting non-authenticated TaskManager.")
        selectNetworkInterfaceAndRunTaskManager(configuration, classOf[TaskManager])
      }
    }
    catch {
      case t: Throwable => {
        LOG.error("Failed to run TaskManager.", t)
        System.exit(STARTUP_FAILURE_RETURN_CODE)
      }
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
        s"Invalid command line agruments: ${args.mkString(" ")}. Usage: ${parser.usage}")
    }

    // load the configuration
    val conf: Configuration = try {
      LOG.info("Loading configuration from " + cliConfig.getConfigDir())
      GlobalConfiguration.loadConfiguration(cliConfig.getConfigDir())
      GlobalConfiguration.getConfiguration()
    }
    catch {
      case e: Exception => throw new Exception("Could not load configuration", e)
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

   * @param configuration The configuration for the TaskManager.
   * @param taskManagerClass The actor class to instantiate.
   *                         Allows to use TaskManager subclasses for example for YARN.
   */
  @throws(classOf[Exception])
  def selectNetworkInterfaceAndRunTaskManager(
      configuration: Configuration,
      taskManagerClass: Class[_ <: TaskManager])
    : Unit = {

    val (taskManagerHostname, actorSystemPort) = selectNetworkInterfaceAndPort(configuration)

    runTaskManager(
      taskManagerHostname,
      actorSystemPort,
      configuration,
      taskManagerClass)
  }

  @throws(classOf[IOException])
  @throws(classOf[IllegalConfigurationException])
  def selectNetworkInterfaceAndPort(
      configuration: Configuration)
    : (String, Int) = {

    var taskManagerHostname = configuration.getString(
      ConfigConstants.TASK_MANAGER_HOSTNAME_KEY, null)

    if (taskManagerHostname != null) {
      LOG.info("Using configured hostname/address for TaskManager: " + taskManagerHostname)
    }
    else {
      val leaderRetrievalService = LeaderRetrievalUtils.createLeaderRetrievalService(configuration)
      val lookupTimeout = AkkaUtils.getLookupTimeout(configuration)

      val taskManagerAddress = LeaderRetrievalUtils.findConnectingAddress(
        leaderRetrievalService,
        lookupTimeout)

      taskManagerHostname = taskManagerAddress.getHostName()
      LOG.info(s"TaskManager will use hostname/address '${taskManagerHostname}' " +
        s"(${taskManagerAddress.getHostAddress()}) for communication.")
    }

    // if no task manager port has been configured, use 0 (system will pick any free port)
    val actorSystemPort = configuration.getInteger(ConfigConstants.TASK_MANAGER_IPC_PORT_KEY, 0)
    if (actorSystemPort < 0 || actorSystemPort > 65535) {
      throw new IllegalConfigurationException("Invalid value for '" +
        ConfigConstants.TASK_MANAGER_IPC_PORT_KEY +
        "' (port for the TaskManager actor system) : " + actorSystemPort +
        " - Leave config parameter empty or use 0 to let the system choose a port automatically.")
    }

    (taskManagerHostname, actorSystemPort)
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
   * @param actorSystemPort The port at which the actor system will communicate.
   * @param configuration The configuration for the TaskManager.
   */
  @throws(classOf[Exception])
  def runTaskManager(
      taskManagerHostname: String,
      actorSystemPort: Int,
      configuration: Configuration)
    : Unit = {

    runTaskManager(
      taskManagerHostname,
      actorSystemPort,
      configuration,
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
   * @param actorSystemPort The port at which the actor system will communicate.
   * @param configuration The configuration for the TaskManager.
   * @param taskManagerClass The actor class to instantiate. Allows the use of TaskManager
   *                         subclasses for example for YARN.
   */
  @throws(classOf[Exception])
  def runTaskManager(
      taskManagerHostname: String,
      actorSystemPort: Int,
      configuration: Configuration,
      taskManagerClass: Class[_ <: TaskManager])
    : Unit = {

    LOG.info(s"Starting TaskManager")

    // Bring up the TaskManager actor system first, bind it to the given address.
    
    LOG.info("Starting TaskManager actor system at " + 
      NetUtils.hostAndPortToUrlString(taskManagerHostname, actorSystemPort))

    val taskManagerSystem = try {
      val akkaConfig = AkkaUtils.getAkkaConfig(
        configuration,
        Some((taskManagerHostname, actorSystemPort))
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
            val address = NetUtils.hostAndPortToUrlString(taskManagerHostname, actorSystemPort)
            throw new IOException("Unable to bind TaskManager actor system to address " +
              address + " - " + cause.getMessage(), t)
          }
        }
        throw new Exception("Could not create TaskManager actor system", t)
      }
    }

    // start all the TaskManager services (network stack,  library cache, ...)
    // and the TaskManager actor
    try {
      LOG.info("Starting TaskManager actor")
      val taskManager = startTaskManagerComponentsAndActor(
        configuration,
        taskManagerSystem,
        taskManagerHostname,
        Some(TASK_MANAGER_NAME),
        None,
        false,
        taskManagerClass)

      // start a process reaper that watches the JobManager. If the TaskManager actor dies,
      // the process reaper will kill the JVM process (to ensure easy failure detection)
      LOG.debug("Starting TaskManager process reaper")
      taskManagerSystem.actorOf(
        Props(classOf[ProcessReaper], taskManager, LOG.logger, RUNTIME_FAILURE_RETURN_CODE),
        "TaskManager_Process_Reaper")

      // if desired, start the logging daemon that periodically logs the
      // memory usage information
      if (LOG.isInfoEnabled && configuration.getBoolean(
        ConfigConstants.TASK_MANAGER_DEBUG_MEMORY_USAGE_START_LOG_THREAD,
        ConfigConstants.DEFAULT_TASK_MANAGER_DEBUG_MEMORY_USAGE_START_LOG_THREAD))
      {
        LOG.info("Starting periodic memory usage logger")

        val interval = configuration.getLong(
          ConfigConstants.TASK_MANAGER_DEBUG_MEMORY_USAGE_LOG_INTERVAL_MS,
          ConfigConstants.DEFAULT_TASK_MANAGER_DEBUG_MEMORY_USAGE_LOG_INTERVAL_MS)

        val logger = new MemoryLogger(LOG.logger, interval, taskManagerSystem)
        logger.start()
      }

      // block until everything is done
      taskManagerSystem.awaitTermination()
    }
    catch {
      case t: Throwable => {
        LOG.error("Error while starting up taskManager", t)
        try {
          taskManagerSystem.shutdown()
        } catch {
          case tt: Throwable => LOG.warn("Could not cleanly shut down actor system", tt)
        }
        throw t
      }
    }
  }

  /**
   *
   * @param configuration The configuration for the TaskManager.
   * @param actorSystem The actor system that should run the TaskManager actor.
   * @param taskManagerHostname The hostname/address that describes the TaskManager's data location.
   * @param taskManagerActorName Optionally the name of the TaskManager actor. If none is given,
   *                             the actor will use a random name.
   * @param leaderRetrievalServiceOption Optionally, a leader retrieval service can be provided. If
   *                                     none is given, then a LeaderRetrievalService is
   *                                     constructed from the configuration.
   * @param localTaskManagerCommunication If true, the TaskManager will not initiate the
   *                                      TCP network stack.
   * @param taskManagerClass The class of the TaskManager actor. May be used to give
   *                         subclasses that understand additional actor messages.
   *
   * @throws org.apache.flink.configuration.IllegalConfigurationException
   *                              Thrown, if the given config contains illegal values.
   *
   * @throws java.io.IOException Thrown, if any of the I/O components (such as buffer pools,
   *                             I/O manager, ...) cannot be properly started.
   * @throws java.lang.Exception Thrown is some other error occurs while parsing the configuration
   *                             or starting the TaskManager components.
   *
   * @return An ActorRef to the TaskManager actor.
   */
  @throws(classOf[IllegalConfigurationException])
  @throws(classOf[IOException])
  @throws(classOf[Exception])
  def startTaskManagerComponentsAndActor(
      configuration: Configuration,
      actorSystem: ActorSystem,
      taskManagerHostname: String,
      taskManagerActorName: Option[String],
      leaderRetrievalServiceOption: Option[LeaderRetrievalService],
      localTaskManagerCommunication: Boolean,
      taskManagerClass: Class[_ <: TaskManager])
    : ActorRef = {

    val (taskManagerConfig : TaskManagerConfiguration,      
      netConfig: NetworkEnvironmentConfiguration,
      connectionInfo: InstanceConnectionInfo,
      memType: MemoryType
    ) = parseTaskManagerConfiguration(
      configuration,
      taskManagerHostname,
      localTaskManagerCommunication)

    // pre-start checks
    checkTempDirs(taskManagerConfig.tmpDirPaths)

    val executionContext = ExecutionContext.fromExecutor(new ForkJoinPool())

    // we start the network first, to make sure it can allocate its buffers first
    val network = new NetworkEnvironment(executionContext, taskManagerConfig.timeout, netConfig)

    // computing the amount of memory to use depends on how much memory is available
    // it strictly needs to happen AFTER the network stack has been initialized

    // check if a value has been configured
    val configuredMemory = configuration.getLong(ConfigConstants.TASK_MANAGER_MEMORY_SIZE_KEY, -1L)
    checkConfigParameter(configuredMemory == -1 || configuredMemory > 0, configuredMemory,
      ConfigConstants.TASK_MANAGER_MEMORY_SIZE_KEY,
      "MemoryManager needs at least one MB of memory. " +
        "If you leave this config parameter empty, the system automatically " +
        "pick a fraction of the available memory.")


    val preAllocateMemory = configuration.getBoolean(
      ConfigConstants.TASK_MANAGER_MEMORY_PRE_ALLOCATE_KEY,
      ConfigConstants.DEFAULT_TASK_MANAGER_MEMORY_PRE_ALLOCATE)

    val memorySize = if (configuredMemory > 0) {
      if (preAllocateMemory) {
        LOG.info(s"Using $configuredMemory MB for managed memory.")
      } else {
        LOG.info(s"Limiting managed memory to $configuredMemory MB, " +
          s"memory will be allocated lazily.")
      }
      configuredMemory << 20 // megabytes to bytes
    }
    else {
      val fraction = configuration.getFloat(
        ConfigConstants.TASK_MANAGER_MEMORY_FRACTION_KEY,
        ConfigConstants.DEFAULT_MEMORY_MANAGER_MEMORY_FRACTION)
      checkConfigParameter(fraction > 0.0f && fraction < 1.0f, fraction,
                           ConfigConstants.TASK_MANAGER_MEMORY_FRACTION_KEY,
                           "MemoryManager fraction of the free memory must be between 0.0 and 1.0")

      if (memType == MemoryType.HEAP) {
        val relativeMemSize = (EnvironmentInformation.getSizeOfFreeHeapMemoryWithDefrag() *
          fraction).toLong

        if (preAllocateMemory) {
          LOG.info(s"Using $fraction of the currently free heap space for managed " +
            s"heap memory (${relativeMemSize >> 20} MB).")
        } else {
          LOG.info(s"Limiting managed memory to $fraction of the currently free heap space " +
            s"(${relativeMemSize >> 20} MB), memory will be allocated lazily.")
        }

        relativeMemSize
      }
      else if (memType == MemoryType.OFF_HEAP) {

        // The maximum heap memory has been adjusted according to the fraction
        val maxMemory = EnvironmentInformation.getMaxJvmHeapMemory()
        val directMemorySize = (maxMemory / (1.0 - fraction) * fraction).toLong

        if (preAllocateMemory) {
          LOG.info(s"Using $fraction of the maximum memory size for " +
            s"managed off-heap memory (${directMemorySize >> 20} MB).")
        } else {
          LOG.info(s"Limiting managed memory to $fraction of the maximum memory size " +
            s"(${directMemorySize >> 20} MB), memory will be allocated lazily.")
        }

        directMemorySize
      }
      else {
        throw new RuntimeException("No supported memory type detected.")
      }
    }

    // now start the memory manager
    val memoryManager = try {
      new MemoryManager(
        memorySize,
        taskManagerConfig.numberOfSlots,
        netConfig.networkBufferSize,
        memType,
        preAllocateMemory)
    }
    catch {
      case e: OutOfMemoryError =>
        memType match {
          case MemoryType.HEAP =>
            throw new Exception(s"OutOfMemory error (${e.getMessage()})" +
              s" while allocating the TaskManager heap memory (${memorySize} bytes).", e)

          case MemoryType.OFF_HEAP =>
            throw new Exception(s"OutOfMemory error (${e.getMessage()})" +
              s" while allocating the TaskManager off-heap memory (${memorySize} bytes). " +
              s"Try increasing the maximum direct memory (-XX:MaxDirectMemorySize)", e)

          case _ => throw e
        }
    }

    // start the I/O manager last, it will create some temp directories.
    val ioManager: IOManager = new IOManagerAsync(taskManagerConfig.tmpDirPaths)

    val leaderRetrievalService = leaderRetrievalServiceOption match {
      case Some(lrs) => lrs
      case None => LeaderRetrievalUtils.createLeaderRetrievalService(configuration)
    }

    // create the actor properties (which define the actor constructor parameters)
    val tmProps = Props(
      taskManagerClass,
      taskManagerConfig,
      connectionInfo,
      memoryManager,
      ioManager,
      network,
      taskManagerConfig.numberOfSlots,
      leaderRetrievalService)

    taskManagerActorName match {
      case Some(actorName) => actorSystem.actorOf(tmProps, actorName)
      case None => actorSystem.actorOf(tmProps)
    }
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

  // --------------------------------------------------------------------------
  //  Parsing and checking the TaskManager Configuration
  // --------------------------------------------------------------------------

  /**
   * Utility method to extract TaskManager config parameters from the configuration and to
   * sanity check them.
   *
   * @param configuration The configuration.
   * @param taskManagerHostname The host name under which the TaskManager communicates.
   * @param localTaskManagerCommunication True, to skip initializing the network stack.
   *                                      Use only in cases where only one task manager runs.
   * @return A tuple (TaskManagerConfiguration, network configuration,
   *                  InstanceConnectionInfo, JobManager actor Akka URL).
   */
  @throws(classOf[IllegalArgumentException])
  def parseTaskManagerConfiguration(
      configuration: Configuration,
      taskManagerHostname: String,
      localTaskManagerCommunication: Boolean)
    : (TaskManagerConfiguration,
     NetworkEnvironmentConfiguration,
     InstanceConnectionInfo,
     MemoryType) = {

    // ------- read values from the config and check them ---------
    //                      (a lot of them)

    // ----> hosts / ports for communication and data exchange

    val dataport = configuration.getInteger(ConfigConstants.TASK_MANAGER_DATA_PORT_KEY,
      ConfigConstants.DEFAULT_TASK_MANAGER_DATA_PORT) match {
      case 0 => NetUtils.getAvailablePort()
      case x => x
    }

    checkConfigParameter(dataport > 0, dataport, ConfigConstants.TASK_MANAGER_DATA_PORT_KEY,
      "Leave config parameter empty or use 0 to let the system choose a port automatically.")

    val taskManagerAddress = InetAddress.getByName(taskManagerHostname)
    val connectionInfo = new InstanceConnectionInfo(taskManagerAddress, dataport)

    // ----> memory / network stack (shuffles/broadcasts), task slots, temp directories

    // we need this because many configs have been written with a "-1" entry
    val slots = configuration.getInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, 1) match {
      case -1 => 1
      case x => x
    }

    checkConfigParameter(slots >= 1, slots, ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS,
      "Number of task slots must be at least one.")

    val numNetworkBuffers = configuration.getInteger(
      ConfigConstants.TASK_MANAGER_NETWORK_NUM_BUFFERS_KEY,
      ConfigConstants.DEFAULT_TASK_MANAGER_NETWORK_NUM_BUFFERS)

    checkConfigParameter(numNetworkBuffers > 0, numNetworkBuffers,
      ConfigConstants.TASK_MANAGER_NETWORK_NUM_BUFFERS_KEY)
    
    val pageSize: Int = configuration.getInteger(
      ConfigConstants.TASK_MANAGER_MEMORY_SEGMENT_SIZE_KEY,
      ConfigConstants.DEFAULT_TASK_MANAGER_MEMORY_SEGMENT_SIZE)

    // check page size of for minimum size
    checkConfigParameter(pageSize >= MemoryManager.MIN_PAGE_SIZE, pageSize,
      ConfigConstants.TASK_MANAGER_MEMORY_SEGMENT_SIZE_KEY,
      "Minimum memory segment size is " + MemoryManager.MIN_PAGE_SIZE)

    // check page size for power of two
    checkConfigParameter(MathUtils.isPowerOf2(pageSize), pageSize,
      ConfigConstants.TASK_MANAGER_MEMORY_SEGMENT_SIZE_KEY,
      "Memory segment size must be a power of 2.")
    
    // check whether we use heap or off-heap memory
    val memType: MemoryType = 
      if (configuration.getBoolean(ConfigConstants.TASK_MANAGER_MEMORY_OFF_HEAP_KEY, false)) {
        MemoryType.OFF_HEAP
      } else {
        MemoryType.HEAP
      }
    
    // initialize the memory segment factory accordingly
    memType match {
      case MemoryType.HEAP =>
        if (!MemorySegmentFactory.isInitialized()) {
          MemorySegmentFactory.initializeFactory(HeapMemorySegment.FACTORY)
        }
        else if (MemorySegmentFactory.getFactory() != HeapMemorySegment.FACTORY) {
          throw new Exception("Memory type is set to heap memory, but memory segment " +
            "factory has been initialized for off-heap memory segments")
        }

      case MemoryType.OFF_HEAP =>
        if (!MemorySegmentFactory.isInitialized()) {
          MemorySegmentFactory.initializeFactory(HybridMemorySegment.FACTORY)
        }
        else if (MemorySegmentFactory.getFactory() != HybridMemorySegment.FACTORY) {
          throw new Exception("Memory type is set to off-heap memory, but memory segment " +
            "factory has been initialized for heap memory segments")
        }
    }
    
    val tmpDirs = configuration.getString(
      ConfigConstants.TASK_MANAGER_TMP_DIR_KEY,
      ConfigConstants.DEFAULT_TASK_MANAGER_TMP_PATH)
    .split(",|" + File.pathSeparator)

    val nettyConfig = if (localTaskManagerCommunication) {
      None
    } else {
      Some(
        new NettyConfig(
          connectionInfo.address(),
          connectionInfo.dataPort(),
          pageSize,
          configuration)
      )
    }

    // Default spill I/O mode for intermediate results
    val syncOrAsync = configuration.getString(
      ConfigConstants.TASK_MANAGER_NETWORK_DEFAULT_IO_MODE,
      ConfigConstants.DEFAULT_TASK_MANAGER_NETWORK_DEFAULT_IO_MODE)

    val ioMode : IOMode = if (syncOrAsync == "async") IOMode.ASYNC else IOMode.SYNC

    val networkConfig = NetworkEnvironmentConfiguration(
      numNetworkBuffers,
      pageSize,
      memType,
      ioMode,
      nettyConfig)

    // ----> timeouts, library caching, profiling

    val timeout = try {
      AkkaUtils.getTimeout(configuration)
    }
    catch {
      case e: Exception => throw new IllegalArgumentException(
        s"Invalid format for '${ConfigConstants.AKKA_ASK_TIMEOUT}'. " +
          s"Use formats like '50 s' or '1 min' to specify the timeout.")
    }
    LOG.info("Messages between TaskManager and JobManager have a max timeout of " + timeout)

    val cleanupInterval = configuration.getLong(
      ConfigConstants.LIBRARY_CACHE_MANAGER_CLEANUP_INTERVAL,
      ConfigConstants.DEFAULT_LIBRARY_CACHE_MANAGER_CLEANUP_INTERVAL) * 1000

    val finiteRegistratioDuration = try {
      val maxRegistrationDuration = Duration(configuration.getString(
        ConfigConstants.TASK_MANAGER_MAX_REGISTRATION_DURATION,
        ConfigConstants.DEFAULT_TASK_MANAGER_MAX_REGISTRATION_DURATION))

      if (maxRegistrationDuration.isFinite()) {
        Some(maxRegistrationDuration.asInstanceOf[FiniteDuration])
      } else {
        None
      }
    } catch {
      case e: NumberFormatException => throw new IllegalArgumentException(
        "Invalid format for parameter " + ConfigConstants.TASK_MANAGER_MAX_REGISTRATION_DURATION,
        e)
    }

    val taskManagerConfig = TaskManagerConfiguration(
      tmpDirs,
      cleanupInterval,
      timeout,
      finiteRegistratioDuration,
      slots,
      configuration)

    (taskManagerConfig, networkConfig, connectionInfo, memType)
  }

  /**
   * Gets the hostname and port of the JobManager from the configuration. Also checks that
   * the hostname is not null and the port non-negative.
   *
   * @param configuration The configuration to read the config values from.
   * @return A 2-tuple (hostname, port).
   */
  def getAndCheckJobManagerAddress(configuration: Configuration) : (String, Int) = {

    val hostname = configuration.getString(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, null)

    val port = configuration.getInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY,
      ConfigConstants.DEFAULT_JOB_MANAGER_IPC_PORT)

    if (hostname == null) {
      throw new Exception("Config parameter '" + ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY +
        "' is missing (hostname/address of JobManager to connect to).")
    }

    if (port <= 0 || port >= 65536) {
      throw new Exception("Invalid value for '" + ConfigConstants.JOB_MANAGER_IPC_PORT_KEY +
        "' (port of the JobManager actor system) : " + port +
        ".  it must be great than 0 and less than 65536.")
    }

    (hostname, port)
  }


  // --------------------------------------------------------------------------
  //  Miscellaneous Utilities
  // --------------------------------------------------------------------------

  /**
   * Validates a condition for a config parameter and displays a standard exception, if the
   * the condition does not hold.
   *
   * @param condition The condition that must hold. If the condition is false, an
   *                  exception is thrown.
   * @param parameter The parameter value. Will be shown in the exception message.
   * @param name The name of the config parameter. Will be shown in the exception message.
   * @param errorMessage The optional custom error message to append to the exception message.
   *
   * @throws IllegalConfigurationException Thrown if the condition is violated.
   */
  @throws(classOf[IllegalConfigurationException])
  private def checkConfigParameter(
      condition: Boolean,
      parameter: Any,
      name: String,
      errorMessage: String = "")
    : Unit = {
    if (!condition) {
      throw new IllegalConfigurationException(
        s"Invalid configuration value for '${name}' : ${parameter} - ${errorMessage}")
    }
  }

  /**
   * Validates that all the directories denoted by the strings do actually exist, are proper
   * directories (not files), and are writable.
   *
   * @param tmpDirs The array of directory paths to check.
   * @throws Exception Thrown if any of the directories does not exist or is not writable
   *                   or is a file, rather than a directory.
   */
  @throws(classOf[IOException])
  private def checkTempDirs(tmpDirs: Array[String]): Unit = {
    tmpDirs.zipWithIndex.foreach {
      case (dir: String, _) =>
        val file = new File(dir)

        if (!file.exists) {
          throw new IOException(
            s"Temporary file directory ${file.getAbsolutePath} does not exist.")
        }
        if (!file.isDirectory) {
          throw new IOException(
            s"Temporary file directory ${file.getAbsolutePath} is not a directory.")
        }
        if (!file.canWrite) {
          throw new IOException(
            s"Temporary file directory ${file.getAbsolutePath} is not writable.")
        }

        if (LOG.isInfoEnabled) {
          val totalSpaceGb = file.getTotalSpace >>  30
          val usableSpaceGb = file.getUsableSpace >> 30
          val usablePercentage = usableSpaceGb.asInstanceOf[Double] / totalSpaceGb * 100

          val path = file.getAbsolutePath

          LOG.info(f"Temporary file directory '$path': total $totalSpaceGb GB, " +
            f"usable $usableSpaceGb GB ($usablePercentage%.2f%% usable)")
        }
      case (_, id) => throw new IllegalArgumentException(s"Temporary file directory #$id is null.")
    }
  }

  /**
   * Creates the registry of default metrics, including stats about garbage collection, memory
   * usage, and system CPU load.
   *
   * @return The registry with the default metrics.
   */
  private def createMetricsRegistry() : MetricRegistry = {
    val metricRegistry = new MetricRegistry()

    // register default metrics
    metricRegistry.register("gc", new GarbageCollectorMetricSet)
    metricRegistry.register("memory", new MemoryUsageGaugeSet)
    metricRegistry.register("load", new Gauge[Double] {
      override def getValue: Double =
        ManagementFactory.getOperatingSystemMXBean().getSystemLoadAverage()
    })

    // Pre-processing steps for registering cpuLoad
    val osBean: OperatingSystemMXBean = ManagementFactory.getOperatingSystemMXBean()
        
    val fetchCPULoadMethod: Option[Method] = 
      try {
        Class.forName("com.sun.management.OperatingSystemMXBean")
          .getMethods()
          .find( _.getName() == "getProcessCpuLoad" )
      }
      catch {
        case t: Throwable =>
          LOG.warn("Cannot access com.sun.management.OperatingSystemMXBean.getProcessCpuLoad()" +
            " - CPU load metrics will not be available.")
          None
      }

    metricRegistry.register("cpuLoad", new Gauge[Double] {
      override def getValue: Double = {
        try{
          fetchCPULoadMethod.map(_.invoke(osBean).asInstanceOf[Double]).getOrElse(-1.0)
        }
        catch {
          case t: Throwable => {
            LOG.warn("Error retrieving CPU Load through OperatingSystemMXBean", t)
            -1.0
          }
        }
      }
    })
    metricRegistry
  }
}
