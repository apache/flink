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
import java.net.{BindException, InetAddress, InetSocketAddress, ServerSocket, UnknownHostException}
import java.lang.management.ManagementFactory
import java.util.UUID
import java.util.concurrent.{ExecutorService, TimeUnit, TimeoutException}
import javax.management.ObjectName

import akka.actor.Status.{Failure, Success}
import akka.actor._
import akka.pattern.ask
import grizzled.slf4j.Logger
import org.apache.flink.api.common.JobID
import org.apache.flink.api.common.time.Time
import org.apache.flink.configuration.{ConfigConstants, Configuration, GlobalConfiguration, HighAvailabilityOptions}
import org.apache.flink.core.fs.FileSystem
import org.apache.flink.core.io.InputSplitAssigner
import org.apache.flink.metrics.{Gauge, MetricGroup}
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup
import org.apache.flink.runtime.accumulators.AccumulatorSnapshot
import org.apache.flink.runtime.akka.{AkkaUtils, ListeningBehaviour}
import org.apache.flink.runtime.blob.BlobServer
import org.apache.flink.runtime.checkpoint._
import org.apache.flink.runtime.checkpoint.savepoint.{SavepointLoader, SavepointStore, SavepointStoreFactory}
import org.apache.flink.runtime.checkpoint.stats.{CheckpointStatsTracker, DisabledCheckpointStatsTracker, SimpleCheckpointStatsTracker}
import org.apache.flink.runtime.client._
import org.apache.flink.runtime.execution.SuppressRestartsException
import org.apache.flink.runtime.clusterframework.FlinkResourceManager
import org.apache.flink.runtime.clusterframework.messages._
import org.apache.flink.runtime.clusterframework.standalone.StandaloneResourceManager
import org.apache.flink.runtime.clusterframework.types.ResourceID
import org.apache.flink.runtime.execution.librarycache.BlobLibraryCacheManager
import org.apache.flink.runtime.executiongraph.restart.RestartStrategyFactory
import org.apache.flink.runtime.executiongraph.{ExecutionGraphBuilder, ExecutionGraph, ExecutionJobVertex, StatusListenerMessenger}
import org.apache.flink.runtime.instance.{AkkaActorGateway, InstanceManager}
import org.apache.flink.runtime.io.network.PartitionState
import org.apache.flink.runtime.jobgraph.{JobGraph, JobStatus}
import org.apache.flink.runtime.jobmanager.SubmittedJobGraphStore.SubmittedJobGraphListener
import org.apache.flink.runtime.jobmanager.scheduler.{Scheduler => FlinkScheduler}
import org.apache.flink.runtime.leaderelection.{LeaderContender, LeaderElectionService, StandaloneLeaderElectionService}
import org.apache.flink.runtime.messages.ArchiveMessages.ArchiveExecutionGraph
import org.apache.flink.runtime.messages.ExecutionGraphMessages.JobStatusChanged
import org.apache.flink.runtime.messages.JobManagerMessages._
import org.apache.flink.runtime.messages.Messages.{Acknowledge, Disconnect}
import org.apache.flink.runtime.messages.RegistrationMessages._
import org.apache.flink.runtime.messages.TaskManagerMessages.{Heartbeat, SendStackTrace}
import org.apache.flink.runtime.messages.TaskMessages.UpdateTaskExecutionState
import org.apache.flink.runtime.messages.accumulators.{AccumulatorMessage, AccumulatorResultStringsFound, AccumulatorResultsErroneous, AccumulatorResultsFound, RequestAccumulatorResults, RequestAccumulatorResultsStringified}
import org.apache.flink.runtime.messages.checkpoint.{AbstractCheckpointMessage, AcknowledgeCheckpoint, DeclineCheckpoint}
import org.apache.flink.runtime.messages.webmonitor.InfoMessage
import org.apache.flink.runtime.messages.webmonitor._
import org.apache.flink.runtime.metrics.{MetricRegistryConfiguration, MetricRegistry => FlinkMetricRegistry}
import org.apache.flink.runtime.metrics.groups.JobManagerMetricGroup
import org.apache.flink.runtime.process.ProcessReaper
import org.apache.flink.runtime.query.{KvStateMessage, UnknownKvStateLocation}
import org.apache.flink.runtime.query.KvStateMessage.{LookupKvStateLocation, NotifyKvStateRegistered, NotifyKvStateUnregistered}
import org.apache.flink.runtime.security.SecurityContext.{FlinkSecuredRunner, SecurityConfiguration}
import org.apache.flink.runtime.security.SecurityContext
import org.apache.flink.runtime.taskmanager.TaskManager
import org.apache.flink.runtime.util._
import org.apache.flink.runtime.webmonitor.{WebMonitor, WebMonitorUtils}
import org.apache.flink.runtime.{FlinkActor, LeaderSessionMessageFilter, LogMessages}
import org.apache.flink.util.{ConfigurationUtil, InstantiationUtil, NetUtils}
import org.jboss.netty.channel.ChannelException

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.concurrent._
import scala.concurrent.duration._
import scala.concurrent.forkjoin.ForkJoinPool
import scala.language.postfixOps

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
 *  the ExecutionGraph and the corresponding ExecutionJobVertices are scheduled for execution on
 *  the TaskManagers.
 *
 *  - [[CancelJob]] requests to cancel the job with the specified jobID. A successful cancellation
 *  is indicated by [[CancellationSuccess]] and a failure by [[CancellationFailure]]
 *
 * - [[UpdateTaskExecutionState]] is sent by a TaskManager to update the state of an
 * ExecutionVertex contained in the [[ExecutionGraph]].
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
    protected val executorService: ExecutorService,
    protected val instanceManager: InstanceManager,
    protected val scheduler: FlinkScheduler,
    protected val libraryCacheManager: BlobLibraryCacheManager,
    protected val archive: ActorRef,
    protected val restartStrategyFactory: RestartStrategyFactory,
    protected val timeout: FiniteDuration,
    protected val leaderElectionService: LeaderElectionService,
    protected val submittedJobGraphs : SubmittedJobGraphStore,
    protected val checkpointRecoveryFactory : CheckpointRecoveryFactory,
    protected val savepointStore: SavepointStore,
    protected val jobRecoveryTimeout: FiniteDuration,
    protected val metricsRegistry: Option[FlinkMetricRegistry])
  extends FlinkActor
  with LeaderSessionMessageFilter // mixin oder is important, we want filtering after logging
  with LogMessages // mixin order is important, we want first logging
  with LeaderContender
  with SubmittedJobGraphListener {

  override val log = Logger(getClass)

  /** The extra execution context, for futures, with a custom logging reporter */
  protected val executionContext: ExecutionContext = ExecutionContext.fromExecutor(
    executorService,
    (t: Throwable) => {
      if (!context.system.isTerminated) {
        log.error("Executor could not execute task", t)
      }
    })

  /** Either running or not yet archived jobs (session hasn't been ended). */
  protected val currentJobs = scala.collection.mutable.HashMap[JobID, (ExecutionGraph, JobInfo)]()

  protected val haMode = HighAvailabilityMode.fromConfig(flinkConfiguration)

  var leaderSessionID: Option[UUID] = None

  protected val jobManagerMetricGroup : Option[JobManagerMetricGroup] = metricsRegistry match {
    case Some(registry) =>
      val host = flinkConfiguration.getString(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, null)
      Option(new JobManagerMetricGroup(
        registry, NetUtils.ipAddressToUrlString(InetAddress.getByName(host))))
    case None =>
      log.warn("Could not instantiate JobManager metrics.")
      None
  }

  /** Futures which have to be completed before terminating the job manager */
  var futuresToComplete: Option[Seq[Future[Unit]]] = None

  /**
   * The port of the web monitor as configured. Make sure that it is actually configured before
   * starting the JobManager. This tightly couples the web monitor with the job manager. It is a
   * temporary workaround until all execution graph components are properly serializable and all
   * web monitors can transparently interact with each job manager. Currently each web server has
   * to run in the actor system of the associated job manager.
   */
  val webMonitorPort : Int = flinkConfiguration.getInteger(
    ConfigConstants.JOB_MANAGER_WEB_PORT_KEY, -1)

  /** The resource manager actor responsible for allocating and managing task manager resources. */
  var currentResourceManager: Option[ActorRef] = None

  /**
   * Run when the job manager is started. Simply logs an informational message.
   * The method also starts the leader election service.
   */
  override def preStart(): Unit = {
    log.info(s"Starting JobManager at $getAddress.")

    try {
      leaderElectionService.start(this)
    } catch {
      case e: Exception =>
        log.error("Could not start the JobManager because the leader election service did not " +
          "start.", e)
        throw new RuntimeException("Could not start the leader election service.", e)
    }

    try {
      submittedJobGraphs.start(this)
    } catch {
      case e: Exception =>
        log.error("Could not start the submitted job graphs service.", e)
        throw new RuntimeException("Could not start the submitted job graphs service.", e)
    }

    try {
      checkpointRecoveryFactory.start()
    } catch {
      case e: Exception =>
        log.error("Could not start the checkpoint recovery service.", e)
        throw new RuntimeException("Could not start the checkpoint recovery service.", e)
    }

    jobManagerMetricGroup match {
      case Some(group) =>
        instantiateMetrics(group)
      case None =>
        log.warn("Could not instantiate JobManager metrics.")
    }
  }

  override def postStop(): Unit = {
    log.info(s"Stopping JobManager $getAddress.")

    val newFuturesToComplete = cancelAndClearEverything(
      new Exception("The JobManager is shutting down."))

    implicit val executionContext = context.dispatcher

    val futureToComplete = Future.sequence(
      futuresToComplete.getOrElse(Seq()) ++ newFuturesToComplete)

    Await.ready(futureToComplete, timeout)

    // disconnect the registered task managers
    instanceManager.getAllRegisteredInstances.asScala.foreach {
      _.getActorGateway().tell(
        Disconnect("JobManager is shutting down"),
        new AkkaActorGateway(self, leaderSessionID.orNull))
    }

    try {
      savepointStore.shutdown()
    } catch {
      case e: Exception =>
        log.error("Could not shut down savepoint store.", e)
        throw new RuntimeException("Could not stop the  savepoint store store.", e)
    }

    try {
      // revoke leadership and stop leader election service
      leaderElectionService.stop()
    } catch {
      case e: Exception => log.error("Could not properly shutdown the leader election service.")
    }

    try {
      submittedJobGraphs.stop()
    } catch {
      case e: Exception => log.error("Could not properly stop the submitted job graphs service.")
    }

    try {
      checkpointRecoveryFactory.stop()
    } catch {
      case e: Exception => log.error("Could not properly stop the checkpoint recovery service.")
    }

    if (archive != ActorRef.noSender) {
      archive ! decorateMessage(PoisonPill)
    }

    instanceManager.shutdown()
    scheduler.shutdown()

    try {
      libraryCacheManager.shutdown()
    } catch {
      case e: IOException => log.error("Could not properly shutdown the library cache manager.", e)
    }

    // shut down the extra thread pool for futures
    executorService.shutdown()

    // failsafe shutdown of the metrics registry
    try {
      metricsRegistry.foreach(_.shutdown())
    } catch {
      case t: Exception => log.error("MetricRegistry did not shutdown properly.", t)
    }

    log.debug(s"Job manager ${self.path} is completely stopped.")
  }

  /**
   * Central work method of the JobManager actor. Receives messages and reacts to them.
   *
   * @return
   */
  override def handleMessage: Receive = {

    case GrantLeadership(newLeaderSessionID) =>
      log.info(s"JobManager $getAddress was granted leadership with leader session ID " +
        s"$newLeaderSessionID.")

      leaderSessionID = newLeaderSessionID

      // confirming the leader session ID might be blocking, thus do it in a future
      future {
        leaderElectionService.confirmLeaderSessionID(newLeaderSessionID.orNull)

        // TODO (critical next step) This needs to be more flexible and robust (e.g. wait for task
        // managers etc.)
        if (haMode != HighAvailabilityMode.NONE) {
          log.info(s"Delaying recovery of all jobs by $jobRecoveryTimeout.")

          context.system.scheduler.scheduleOnce(
            jobRecoveryTimeout,
            self,
            decorateMessage(RecoverAllJobs))(
            context.dispatcher)
        }
      }(context.dispatcher)

    case RevokeLeadership =>
      log.info(s"JobManager ${self.path.toSerializationFormat} was revoked leadership.")

      val newFuturesToComplete = cancelAndClearEverything(
        new Exception("JobManager is no longer the leader."))

      futuresToComplete = Some(futuresToComplete.getOrElse(Seq()) ++ newFuturesToComplete)

      // disconnect the registered task managers
      instanceManager.getAllRegisteredInstances.asScala.foreach {
        _.getActorGateway().tell(
          Disconnect("JobManager is no longer the leader"),
          new AkkaActorGateway(self, leaderSessionID.orNull))
      }

      instanceManager.unregisterAllTaskManagers()

      leaderSessionID = None

    case msg: RegisterResourceManager =>
      log.debug(s"Resource manager registration: $msg")

      // ditch current resource manager (if any)
      currentResourceManager = Option(msg.resourceManager())

      val taskManagerResources = instanceManager.getAllRegisteredInstances.asScala.map(
        instance => instance.getTaskManagerID).toList.asJava

      // confirm registration and send known task managers with their resource ids
      sender ! decorateMessage(new RegisterResourceManagerSuccessful(self, taskManagerResources))

    case msg: ReconnectResourceManager =>
      log.debug(s"Resource manager reconnect: $msg")

      /**
        * In most cases, the ResourceManager handles the reconnect itself (due to leader change)
        * but in case it doesn't we're sending a TriggerRegistrationAtJobManager message until we
        * receive a registration of this or another ResourceManager.
        */
      def reconnectRepeatedly(): Unit = {
        msg.resourceManager() ! decorateMessage(new TriggerRegistrationAtJobManager(self))
        // try again after some delay
        context.system.scheduler.scheduleOnce(2 seconds) {
          self ! decorateMessage(msg)
        }(context.dispatcher)
      }

      currentResourceManager match {
        case Some(rm) if rm.equals(msg.resourceManager()) =>
          // we should ditch the current resource manager
          log.debug(s"Disconnecting resource manager $rm and forcing a reconnect.")
          currentResourceManager = None
          reconnectRepeatedly()
        case Some(rm) =>
          // we have registered with another ResourceManager in the meantime, stop sending
          // TriggerRegistrationAtJobManager messages to the old ResourceManager
        case None =>
          log.warn(s"No resource manager ${msg.resourceManager()} connected. " +
            s"Telling old ResourceManager to register again.")
          reconnectRepeatedly()
      }

    case msg @ RegisterTaskManager(
          resourceId,
          connectionInfo,
          hardwareInformation,
          numberOfSlots) =>
      // we are being informed by the ResourceManager that a new task manager is available
      log.debug(s"RegisterTaskManager: $msg")

      val taskManager = sender()

      currentResourceManager match {
        case Some(rm) =>
          val future = (rm ? decorateMessage(new NotifyResourceStarted(msg.resourceId)))(timeout)
          future.onFailure {
            case t: Throwable =>
              t match {
                case _: TimeoutException =>
                  log.info("Attempt to register resource at ResourceManager timed out. Retrying")
                case _ =>
                  log.warn("Failure while asking ResourceManager for RegisterResource. Retrying", t)
              }
              self ! decorateMessage(new ReconnectResourceManager(rm))
          }(context.dispatcher)

        case None =>
          log.info("Task Manager Registration but not connected to ResourceManager")
      }

      // ResourceManager is told about the resource, now let's try to register TaskManager
      if (instanceManager.isRegistered(taskManager)) {
        val instanceID = instanceManager.getRegisteredInstance(taskManager).getId

        taskManager ! decorateMessage(
          AlreadyRegistered(
            instanceID,
            libraryCacheManager.getBlobServerPort))
      } else {
        try {
          val instanceID = instanceManager.registerTaskManager(
            taskManager,
            connectionInfo,
            hardwareInformation,
            numberOfSlots,
            leaderSessionID.orNull)

          taskManager ! decorateMessage(
            AcknowledgeRegistration(instanceID, libraryCacheManager.getBlobServerPort))

          // to be notified when the taskManager is no longer reachable
          context.watch(taskManager)
        } catch {
          // registerTaskManager throws an IllegalStateException if it is already shut down
          // let the actor crash and restart itself in this case
          case e: Exception =>
            log.error("Failed to register TaskManager at instance manager", e)

            taskManager ! decorateMessage(
              RefuseRegistration(e))
        }
      }

    case msg: ResourceRemoved =>
      // we're being informed by the resource manager that a resource has become unavailable
      // note: a Terminated event may already have removed the instance.
      val resourceID = msg.resourceId()
      log.debug(s"Resource has been removed: $resourceID")

      Option(instanceManager.getRegisteredInstance(resourceID)) match {
        case Some(instance) =>
          // trigger removal of task manager
          handleTaskManagerTerminated(instance.getActorGateway.actor())
        case None =>
          log.debug(s"Resource $resourceID has not been registered at job manager.")
      }

    case RequestNumberRegisteredTaskManager =>
      sender ! decorateMessage(instanceManager.getNumberOfRegisteredTaskManagers)

    case RequestTotalNumberOfSlots =>
      sender ! decorateMessage(instanceManager.getTotalNumberOfSlots)

    case SubmitJob(jobGraph, listeningBehaviour) =>
      val client = sender()

      val jobInfo = new JobInfo(client, listeningBehaviour, System.currentTimeMillis(),
        jobGraph.getSessionTimeout)

      submitJob(jobGraph, jobInfo)

    case RegisterJobClient(jobID, listeningBehaviour) =>
      val client = sender()
      currentJobs.get(jobID) match {
        case Some((executionGraph, jobInfo)) =>
          log.info(s"Registering client for job $jobID")
          jobInfo.clients += ((client, listeningBehaviour))
          val listener = new StatusListenerMessenger(client, leaderSessionID.orNull)
          executionGraph.registerJobStatusListener(listener)
          if (listeningBehaviour == ListeningBehaviour.EXECUTION_RESULT_AND_STATE_CHANGES) {
            executionGraph.registerExecutionListener(listener)
          }
          client ! decorateMessage(RegisterJobClientSuccess(jobID))
        case None =>
          client ! decorateMessage(JobNotFound(jobID))
      }

    case RecoverSubmittedJob(submittedJobGraph) =>
      if (!currentJobs.contains(submittedJobGraph.getJobId)) {
        submitJob(
          submittedJobGraph.getJobGraph(),
          submittedJobGraph.getJobInfo(),
          isRecovery = true)
      }
      else {
        log.info(s"Ignoring job recovery for ${submittedJobGraph.getJobId}, " +
          s"because it is already submitted.")
      }

    case RecoverJob(jobId) =>
      future {
        try {
          // The ActorRef, which is part of the submitted job graph can only be
          // de-serialized in the scope of an actor system.
          akka.serialization.JavaSerializer.currentSystem.withValue(
            context.system.asInstanceOf[ExtendedActorSystem]) {

            log.info(s"Attempting to recover job $jobId.")
            val submittedJobGraphOption = submittedJobGraphs.recoverJobGraph(jobId)

            submittedJobGraphOption match {
              case Some(submittedJobGraph) =>
                if (!leaderElectionService.hasLeadership()) {
                  // we've lost leadership. mission: abort.
                  log.warn(s"Lost leadership during recovery. Aborting recovery of $jobId.")
                } else {
                  self ! decorateMessage(RecoverSubmittedJob(submittedJobGraph))
                }

              case None => log.info(s"Attempted to recover job $jobId, but no job graph found.")
            }
          }
        } catch {
          case t: Throwable => log.error(s"Failed to recover job $jobId.", t)
        }
      }(context.dispatcher)

    case RecoverAllJobs =>
      future {
        try {
          // The ActorRef, which is part of the submitted job graph can only be
          // de-serialized in the scope of an actor system.
          akka.serialization.JavaSerializer.currentSystem.withValue(
            context.system.asInstanceOf[ExtendedActorSystem]) {

            log.info(s"Attempting to recover all jobs.")

            val jobGraphs = submittedJobGraphs.recoverJobGraphs().asScala

            if (!leaderElectionService.hasLeadership()) {
              // we've lost leadership. mission: abort.
              log.warn(s"Lost leadership during recovery. Aborting recovery of ${jobGraphs.size} " +
                s"jobs.")
            } else {
              log.info(s"Re-submitting ${jobGraphs.size} job graphs.")

              jobGraphs.foreach{
                submittedJobGraph =>
                  self ! decorateMessage(RecoverSubmittedJob(submittedJobGraph))
              }
            }
          }
        } catch {
          case t: Throwable => log.error("Fatal error: Failed to recover jobs.", t)
        }
      }(context.dispatcher)

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

    case StopJob(jobID) =>
      log.info(s"Trying to stop job with ID $jobID.")

      currentJobs.get(jobID) match {
        case Some((executionGraph, _)) =>
          try {
            if (!executionGraph.isStoppable()) {
              sender ! decorateMessage(
                StoppingFailure(
                  jobID,
                  new IllegalStateException(s"Job with ID $jobID is not stoppable."))
              )
            } else if (executionGraph.getState() != JobStatus.RUNNING) {
              sender ! decorateMessage(
                StoppingFailure(
                  jobID,
                  new IllegalStateException(s"Job with ID $jobID is in state " +
                    executionGraph.getState().name() + " but stopping is only allowed in state " +
                    "RUNNING."))
              )
            } else {
              executionGraph.stop()
              sender ! decorateMessage(StoppingSuccess(jobID))
            }
          } catch {
            case t: Throwable =>  sender ! decorateMessage(StoppingFailure(jobID, t))
          }
        case None =>
          log.info(s"No job found with ID $jobID.")
          sender ! decorateMessage(
            StoppingFailure(
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
              slot.getTaskManagerLocation().getHostname()
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

    case kvStateMsg : KvStateMessage =>
      handleKvStateMessage(kvStateMsg)

    case TriggerSavepoint(jobId) =>
      currentJobs.get(jobId) match {
        case Some((graph, _)) =>
          val checkpointCoordinator = graph.getCheckpointCoordinator()

          if (checkpointCoordinator != null) {
            // Immutable copy for the future
            val senderRef = sender()

            future {
              try {
                // Do this async, because checkpoint coordinator operations can
                // contain blocking calls to the state backend or ZooKeeper.
                val savepointFuture = checkpointCoordinator.triggerSavepoint(
                  System.currentTimeMillis())

                savepointFuture.onComplete {
                  // Success, respond with the savepoint path
                  case scala.util.Success(savepointPath) =>
                    senderRef ! TriggerSavepointSuccess(jobId, savepointPath)

                  // Failure, respond with the cause
                  case scala.util.Failure(t) =>
                    senderRef ! TriggerSavepointFailure(
                      jobId,
                      new Exception("Failed to complete savepoint", t))
                }(context.dispatcher)
              } catch {
                case e: Exception =>
                  senderRef ! TriggerSavepointFailure(jobId, new Exception(
                    "Failed to trigger savepoint", e))
              }
            }(context.dispatcher)
          } else {
            sender() ! TriggerSavepointFailure(jobId, new IllegalStateException(
              "Checkpointing disabled. You can enable it via the execution environment of " +
                "your job."))
          }

        case None =>
          sender() ! TriggerSavepointFailure(jobId, new IllegalArgumentException("Unknown job."))
      }

    case DisposeSavepoint(savepointPath) =>
      val senderRef = sender()
      future {
        try {
          log.info(s"Disposing savepoint at '$savepointPath'.")

          val savepoint = savepointStore.loadSavepoint(savepointPath)

          log.debug(s"$savepoint")

          // Dispose the savepoint
          savepointStore.disposeSavepoint(savepointPath)

          senderRef ! DisposeSavepointSuccess
        } catch {
          case t: Throwable =>
            log.error(s"Failed to dispose savepoint at '$savepointPath'.", t)

            senderRef ! DisposeSavepointFailure(t)
        }
      }(context.dispatcher)

    case JobStatusChanged(jobID, newJobStatus, timeStamp, error) =>
      currentJobs.get(jobID) match {
        case Some((executionGraph, jobInfo)) => executionGraph.getJobName

          log.info(
            s"Status of job $jobID (${executionGraph.getJobName}) changed to $newJobStatus.",
            error)

          if (newJobStatus.isGloballyTerminalState()) {
            jobInfo.end = timeStamp

            future{
              // TODO If removing the JobGraph from the SubmittedJobGraphsStore fails, the job will
              // linger around and potentially be recovered at a later time. There is nothing we
              // can do about that, but it should be communicated with the Client.
              if (jobInfo.sessionAlive) {
                jobInfo.setLastActive()
                val lastActivity = jobInfo.lastActive
                context.system.scheduler.scheduleOnce(jobInfo.sessionTimeout seconds) {
                  // remove only if no activity occurred in the meantime
                  if (lastActivity == jobInfo.lastActive) {
                    self ! decorateMessage(RemoveJob(jobID, removeJobFromStateBackend = true))
                  }
                }(context.dispatcher)
              } else {
                self ! decorateMessage(RemoveJob(jobID, removeJobFromStateBackend = true))
              }

              // is the client waiting for the job result?
              newJobStatus match {
                case JobStatus.FINISHED =>
                try {
                  val accumulatorResults = executionGraph.getAccumulatorsSerialized()
                  val result = new SerializedJobExecutionResult(
                    jobID,
                    jobInfo.duration,
                    accumulatorResults)

                  jobInfo.notifyNonDetachedClients(
                    decorateMessage(JobResultSuccess(result)))
                } catch {
                  case e: Exception =>
                    log.error(s"Cannot fetch final accumulators for job $jobID", e)
                    val exception = new JobExecutionException(jobID,
                      "Failed to retrieve accumulator results.", e)

                    jobInfo.notifyNonDetachedClients(
                      decorateMessage(JobResultFailure(
                        new SerializedThrowable(exception))))
                }

                case JobStatus.CANCELED =>
                  // the error may be packed as a serialized throwable
                  val unpackedError = SerializedThrowable.get(
                    error, executionGraph.getUserClassLoader())

                  jobInfo.notifyNonDetachedClients(
                    decorateMessage(JobResultFailure(
                      new SerializedThrowable(
                        new JobCancellationException(jobID, "Job was cancelled.", unpackedError)))))

                case JobStatus.FAILED =>
                  val unpackedError = SerializedThrowable.get(
                    error, executionGraph.getUserClassLoader())

                  jobInfo.notifyNonDetachedClients(
                    decorateMessage(JobResultFailure(
                      new SerializedThrowable(
                        new JobExecutionException(jobID, "Job execution failed.", unpackedError)))))

                case x =>
                  val exception = new JobExecutionException(jobID, s"$x is not a terminal state.")
                  jobInfo.notifyNonDetachedClients(
                    decorateMessage(JobResultFailure(
                      new SerializedThrowable(exception))))
                  throw exception
              }
            }(context.dispatcher)
          }
        case None =>
          self ! decorateMessage(RemoveJob(jobID, removeJobFromStateBackend = true))
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
        new PartitionState(
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

    case RequestClassloadingProps(jobID) =>
      currentJobs.get(jobID) match {
        case Some((graph, jobInfo)) =>
          sender() ! decorateMessage(
            ClassloadingProps(
              libraryCacheManager.getBlobServerPort,
              graph.getRequiredJarFiles,
              graph.getRequiredClasspaths))
        case None =>
          sender() ! decorateMessage(JobNotFound(jobID))
      }

    case RequestBlobManagerPort =>
      sender ! decorateMessage(libraryCacheManager.getBlobServerPort)

    case RequestArchive =>
      sender ! decorateMessage(ResponseArchive(archive))

    case RequestRegisteredTaskManagers =>
      sender ! decorateMessage(
        RegisteredTaskManagers(
          instanceManager.getAllRegisteredInstances.asScala
        )
      )

    case RequestTaskManagerInstance(instanceID) =>
      sender ! decorateMessage(
        TaskManagerInstance(Option(instanceManager.getRegisteredInstanceById(instanceID)))
      )

    case Heartbeat(instanceID, metricsReport, accumulators) =>
      log.debug(s"Received heartbeat message from $instanceID.")

      updateAccumulators(accumulators)

      instanceManager.reportHeartBeat(instanceID, metricsReport)

    case message: AccumulatorMessage => handleAccumulatorMessage(message)

    case message: InfoMessage => handleInfoRequestMessage(message, sender())

    case RequestStackTrace(instanceID) =>
      val gateway = instanceManager.getRegisteredInstanceById(instanceID).getActorGateway
      gateway.forward(SendStackTrace, new AkkaActorGateway(sender, leaderSessionID.orNull))

    case Terminated(taskManagerActorRef) =>
      handleTaskManagerTerminated(taskManagerActorRef)

    case RequestJobManagerStatus =>
      sender() ! decorateMessage(JobManagerStatusAlive)

    case RemoveJob(jobID, clearPersistedJob) =>
      currentJobs.get(jobID) match {
        case Some((graph, info)) =>
            removeJob(graph.getJobID, clearPersistedJob) match {
              case Some(futureToComplete) =>
                futuresToComplete = Some(futuresToComplete.getOrElse(Seq()) :+ futureToComplete)
              case None =>
            }
        case None =>
      }

    case RemoveCachedJob(jobID) =>
      currentJobs.get(jobID) match {
        case Some((graph, info)) =>
          if (graph.getState.isGloballyTerminalState) {
            removeJob(graph.getJobID, removeJobFromStateBackend = true) match {
              case Some(futureToComplete) =>
                futuresToComplete = Some(futuresToComplete.getOrElse(Seq()) :+ futureToComplete)
              case None =>
            }
          } else {
            // triggers removal upon completion of job
            info.sessionAlive = false
          }
        case None =>
      }

    case Disconnect(msg) =>
      val taskManager = sender()

      if (instanceManager.isRegistered(taskManager)) {
        log.info(s"Task manager ${taskManager.path} wants to disconnect, because $msg.")

        instanceManager.unregisterTaskManager(taskManager, false)
        context.unwatch(taskManager)
      }

    case msg: StopCluster =>

      log.info(s"Stopping JobManager with final application status ${msg.finalStatus()} " +
        s"and diagnostics: ${msg.message()}")

      // stop all task managers
      instanceManager.getAllRegisteredInstances.asScala foreach {
        instance =>
          instance.getActorGateway.tell(msg)
      }

      // send resource manager the ok
      currentResourceManager match {
        case Some(rm) =>
          try {
            // inform rm and wait for it to confirm
            val waitTime = FiniteDuration(5, TimeUnit.SECONDS)
            val answer = (rm ? decorateMessage(msg))(waitTime)
            Await.ready(answer, waitTime)
          } catch {
            case e: TimeoutException =>
            case e: InterruptedException =>
          }
        case None =>
          // ResourceManager not available
          // we choose not to wait here beacuse it might block the shutdown forever
      }

      sender() ! decorateMessage(StopClusterSuccessful.getInstance())
      shutdown()

    case RequestLeaderSessionID =>
      sender() ! ResponseLeaderSessionID(leaderSessionID.orNull)

    case RequestWebMonitorPort =>
      sender() ! ResponseWebMonitorPort(webMonitorPort)
  }

  /**
    * Handler to be executed when a task manager terminates.
    * (Akka Deathwatch or notification from ResourceManager)
    *
    * @param taskManager The ActorRef of the taskManager
    */
  private def handleTaskManagerTerminated(taskManager: ActorRef): Unit = {
    if (instanceManager.isRegistered(taskManager)) {
      log.info(s"Task manager ${taskManager.path} terminated.")

      instanceManager.unregisterTaskManager(taskManager, true)
      context.unwatch(taskManager)
    }
  }

  /**
   * Submits a job to the job manager. The job is registered at the libraryCacheManager which
   * creates the job's class loader. The job graph is appended to the corresponding execution
   * graph and the execution vertices are queued for scheduling.
   *
   * @param jobGraph representing the Flink job
   * @param jobInfo the job info
   * @param isRecovery Flag indicating whether this is a recovery or initial submission
   */
  private def submitJob(jobGraph: JobGraph, jobInfo: JobInfo, isRecovery: Boolean = false): Unit = {
    if (jobGraph == null) {
      jobInfo.notifyClients(
        decorateMessage(JobResultFailure(
          new SerializedThrowable(
            new JobSubmissionException(null, "JobGraph must not be null.")))))
    }
    else {
      val jobId = jobGraph.getJobID
      val jobName = jobGraph.getName
      var executionGraph: ExecutionGraph = null

      log.info(s"Submitting job $jobId ($jobName)" + (if (isRecovery) " (Recovery)" else "") + ".")

      try {
        // Important: We need to make sure that the library registration is the first action,
        // because this makes sure that the uploaded jar files are removed in case of
        // unsuccessful
        try {
          libraryCacheManager.registerJob(jobGraph.getJobID, jobGraph.getUserJarBlobKeys,
            jobGraph.getClasspaths)
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

        val restartStrategy =
          Option(jobGraph.getSerializedExecutionConfig()
            .deserializeValue(userCodeLoader)
            .getRestartStrategy())
            .map(RestartStrategyFactory.createRestartStrategy) match {
            case Some(strategy) => strategy
            case None => restartStrategyFactory.createRestartStrategy()
          }

        log.info(s"Using restart strategy $restartStrategy for $jobId.")

        val jobMetrics = jobManagerMetricGroup match {
          case Some(group) =>
            group.addJob(jobGraph) match {
              case (jobGroup:Any) => jobGroup
              case null => new UnregisteredMetricsGroup()
            }
          case None =>
            new UnregisteredMetricsGroup()
        }

        val numSlots = scheduler.getTotalNumberOfSlots()

        // see if there already exists an ExecutionGraph for the corresponding job ID
        val registerNewGraph = currentJobs.get(jobGraph.getJobID) match {
          case Some((graph, currentJobInfo)) =>
            executionGraph = graph
            currentJobInfo.setLastActive()
            false
          case None =>
            true
        }

        executionGraph = ExecutionGraphBuilder.buildGraph(
          executionGraph,
          jobGraph,
          flinkConfiguration,
          executionContext,
          userCodeLoader,
          checkpointRecoveryFactory,
          savepointStore,
          Time.of(timeout.length, timeout.unit),
          restartStrategy,
          jobMetrics,
          numSlots,
          log.logger)
        
        if (registerNewGraph) {
          currentJobs.put(jobGraph.getJobID, (executionGraph, jobInfo))
        }

        // get notified about job status changes
        executionGraph.registerJobStatusListener(
          new StatusListenerMessenger(self, leaderSessionID.orNull))

        jobInfo.clients foreach {
          // the sender wants to be notified about state changes
          case (client, ListeningBehaviour.EXECUTION_RESULT_AND_STATE_CHANGES) =>
            val listener  = new StatusListenerMessenger(client, leaderSessionID.orNull)
            executionGraph.registerExecutionListener(listener)
            executionGraph.registerJobStatusListener(listener)
          case _ => // do nothing
        }

      } catch {
        case t: Throwable =>
          log.error(s"Failed to submit job $jobId ($jobName)", t)

          libraryCacheManager.unregisterJob(jobId)
          currentJobs.remove(jobId)

          if (executionGraph != null) {
            executionGraph.fail(t)
          }

          val rt: Throwable = if (t.isInstanceOf[JobExecutionException]) {
            t
          } else {
            new JobExecutionException(jobId, s"Failed to submit job $jobId ($jobName)", t)
          }

          jobInfo.notifyClients(
            decorateMessage(JobResultFailure(new SerializedThrowable(rt))))
          return
      }

      // execute the recovery/writing the jobGraph into the SubmittedJobGraphStore asynchronously
      // because it is a blocking operation
      future {
        try {
          if (isRecovery) {
            // this is a recovery of a master failure (this master takes over)
            executionGraph.restoreLatestCheckpointedState()
          }
          else {
            // load a savepoint only if this is not starting from a newer checkpoint
            // as part of an master failure recovery
            val snapshotSettings = jobGraph.getSnapshotSettings
            if (snapshotSettings != null) {
              val savepointPath = snapshotSettings.getSavepointPath()

              if (savepointPath != null) {
                // got a savepoint
                try {
                  log.info(s"Starting job from savepoint '$savepointPath'.")

                  // load the savepoint as a checkpoint into the system
                  val savepoint: CompletedCheckpoint = SavepointLoader.loadAndValidateSavepoint(
                    jobId, executionGraph.getAllVertices, savepointStore, savepointPath)

                  executionGraph.getCheckpointCoordinator.getCheckpointStore
                    .addCheckpoint(savepoint)

                  // Reset the checkpoint ID counter
                  val nextCheckpointId: Long = savepoint.getCheckpointID + 1
                  log.info(s"Reset the checkpoint ID to $nextCheckpointId")
                  executionGraph.getCheckpointCoordinator.getCheckpointIdCounter
                    .setCount(nextCheckpointId)

                  executionGraph.restoreLatestCheckpointedState()
                } catch {
                  case e: Exception =>
                    throw new SuppressRestartsException(e)
                }
              }
            }

            try {
              submittedJobGraphs.putJobGraph(new SubmittedJobGraph(jobGraph, jobInfo))
            } catch {
              case t: Throwable =>
                // Don't restart the execution if this fails. Otherwise, the
                // job graph will skip ZooKeeper in case of HA.
                new SuppressRestartsException(t)
            }
          }

          jobInfo.notifyClients(
            decorateMessage(JobSubmitSuccess(jobGraph.getJobID)))

          if (leaderElectionService.hasLeadership) {
            // There is a small chance that multiple job managers schedule the same job after if
            // they try to recover at the same time. This will eventually be noticed, but can not be
            // ruled out from the beginning.

            // NOTE: Scheduling the job for execution is a separate action from the job submission.
            // The success of submitting the job must be independent from the success of scheduling
            // the job.
            log.info(s"Scheduling job $jobId ($jobName).")

            executionGraph.scheduleForExecution(scheduler)
          } else {
            // Remove the job graph. Otherwise it will be lingering around and possibly removed from
            // ZooKeeper by this JM.
            self ! decorateMessage(RemoveJob(jobId, removeJobFromStateBackend = false))

            log.warn(s"Submitted job $jobId, but not leader. The other leader needs to recover " +
              "this. I am not scheduling the job for execution.")
          }
        } catch {
          case t: Throwable => try {
            executionGraph.fail(t)
          } catch {
            case tt: Throwable =>
              log.error("Error while marking ExecutionGraph as failed.", tt)
          }
        }
      }(context.dispatcher)
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
            val checkpointCoordinator = graph.getCheckpointCoordinator()

            if (checkpointCoordinator != null) {
              future {
                try {
                  if (!checkpointCoordinator.receiveAcknowledgeMessage(ackMessage)) {
                    log.info("Received message for non-existing checkpoint " +
                      ackMessage.getCheckpointId)
                  }
                }
                catch {
                  case t: Throwable =>
                    log.error(s"Error in CheckpointCoordinator while processing $ackMessage", t)
                }
              }(context.dispatcher)
            }
            else {
              log.error(
                s"Received AcknowledgeCheckpoint message for job $jid with no " +
                  s"CheckpointCoordinator")
            }

          case None => log.error(s"Received AcknowledgeCheckpoint for unavailable job $jid")
        }

      case declineMessage: DeclineCheckpoint =>
        val jid = declineMessage.getJob()
        currentJobs.get(jid) match {
          case Some((graph, _)) =>
            val checkpointCoordinator = graph.getCheckpointCoordinator()

            if (checkpointCoordinator != null) {
              future {
                try {
                  if (!checkpointCoordinator.receiveDeclineMessage(declineMessage)) {
                    log.info("Received message for non-existing checkpoint " +
                      declineMessage.getCheckpointId)
                  }
                }
                catch {
                  case t: Throwable =>
                    log.error(s"Error in CheckpointCoordinator while processing $declineMessage", t)
                }
              }(context.dispatcher)
            }
            else {
              log.error(
                s"Received DeclineCheckpoint message for job $jid with no CheckpointCoordinator")
            }

          case None => log.error(s"Received DeclineCheckpoint for unavailable job $jid")
        }


      // unknown checkpoint message
      case _ => unhandled(actorMessage)
    }
  }

  /**
    * Handle all [KvStateMessage] instances for KvState location lookups and
    * registration.
    *
    * @param actorMsg The KvState actor message.
    */
  private def handleKvStateMessage(actorMsg: KvStateMessage): Unit = {
    actorMsg match {
      // Client KvStateLocation lookup
      case msg: LookupKvStateLocation =>
        currentJobs.get(msg.getJobId) match {
          case Some((graph, _)) =>
            try {
              log.debug(s"Lookup key-value state for job ${msg.getJobId} with registration " +
                         s"name ${msg.getRegistrationName}.")

              val registry = graph.getKvStateLocationRegistry
              val location = registry.getKvStateLocation(msg.getRegistrationName)
              if (location == null) {
                sender() ! Failure(new UnknownKvStateLocation(msg.getRegistrationName))
              } else {
                sender() ! Success(location)
              }
            } catch {
              case t: Throwable =>
                sender() ! Failure(t)
            }

          case None =>
            sender() ! Status.Failure(new IllegalStateException(s"Job ${msg.getJobId} not found"))
        }

      // TaskManager KvState registration
      case msg: NotifyKvStateRegistered =>
        currentJobs.get(msg.getJobId) match {
          case Some((graph, _)) =>
            try {
              log.debug(s"Key value state registered for job ${msg.getJobId} under " +
                         s"name ${msg.getRegistrationName}.")

              graph.getKvStateLocationRegistry.notifyKvStateRegistered(
                msg.getJobVertexId,
                msg.getKeyGroupRange,
                msg.getRegistrationName,
                msg.getKvStateId,
                msg.getKvStateServerAddress)
            } catch {
              case t: Throwable =>
                log.error(s"Failed to notify KvStateRegistry about registration $msg.")
            }

          case None => log.error(s"Received $msg for unavailable job.")
        }

      // TaskManager KvState unregistration
      case msg: NotifyKvStateUnregistered =>
        currentJobs.get(msg.getJobId) match {
          case Some((graph, _)) =>
            try {
              graph.getKvStateLocationRegistry.notifyKvStateUnregistered(
                msg.getJobVertexId,
                msg.getKeyGroupRange,
                msg.getRegistrationName)
            } catch {
              case t: Throwable =>
                log.error(s"Failed to notify KvStateRegistry about registration $msg.")
            }

          case None => log.error(s"Received $msg for unavailable job.")
        }

      case _ => unhandled(actorMsg)
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
            sender() ! decorateMessage(
              AccumulatorResultStringsFound(jobId, stringifiedAccumulators)
            )
          case None =>
            archive.forward(message)
        }

      case unknown =>
        log.warn(s"Received unknown AccumulatorMessage: $unknown")
    }
  }

  /**
   * Dedicated handler for monitor info request messages.
   * 
   * Note that this handler does not fail. Errors while responding to info messages are logged,
   * but will not cause the actor to crash.
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

        case msg : RequestJobDetails => 
          
          val ourDetails: Array[JobDetails] = if (msg.shouldIncludeRunning()) {
            currentJobs.values.map {
              v => WebMonitorUtils.createDetailsForJob(v._1)
            }.toArray[JobDetails]
          } else {
            null
          }
          
          if (msg.shouldIncludeFinished()) {
            val future = (archive ? msg)(timeout)
            future.onSuccess {
              case archiveDetails: MultipleJobsDetails =>
                theSender ! new MultipleJobsDetails(ourDetails, archiveDetails.getFinishedJobs())
            }(context.dispatcher)
          } else {
            theSender ! new MultipleJobsDetails(ourDetails, null)
          }
          
        case _ => log.error("Unrecognized info message " + actorMessage)
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
   * Removes the job and sends it to the MemoryArchivist.
   *
   * This should be called asynchronously. Removing the job from the [[SubmittedJobGraphStore]]
   * might block. Therefore be careful not to block the actor thread.
   *
   * @param jobID ID of the job to remove and archive
   * @param removeJobFromStateBackend true if the job shall be archived and removed from the state
   *                            backend
   */
  private def removeJob(jobID: JobID, removeJobFromStateBackend: Boolean): Option[Future[Unit]] = {
    // Don't remove the job yet...
    val futureOption = currentJobs.get(jobID) match {
      case Some((eg, _)) =>
        val result = if (removeJobFromStateBackend) {
          val futureOption = Some(future {
            try {
              // ...otherwise, we can have lingering resources when there is a  concurrent shutdown
              // and the ZooKeeper client is closed. Not removing the job immediately allow the
              // shutdown to release all resources.
              submittedJobGraphs.removeJobGraph(jobID)
            } catch {
              case t: Throwable => log.error(s"Could not remove submitted job graph $jobID.", t)
            }
          }(context.dispatcher))

          try {
            eg.prepareForArchiving()

            archive ! decorateMessage(ArchiveExecutionGraph(jobID, eg))
          } catch {
            case t: Throwable => log.error(s"Could not prepare the execution graph $eg for " +
              "archiving.", t)
          }

          futureOption
        } else {
          None
        }

        currentJobs.remove(jobID)

        result
      case None => None
    }

    try {
      libraryCacheManager.unregisterJob(jobID)
    } catch {
      case t: Throwable =>
        log.error(s"Could not properly unregister job $jobID form the library cache.", t)
    }
    jobManagerMetricGroup.map(_.removeJob(jobID))

    futureOption
  }

  /** Fails all currently running jobs and empties the list of currently running jobs. If the
    * [[JobClientActor]] waits for a result, then a [[JobExecutionException]] is sent.
    *
    * @param cause Cause for the cancelling.
    */
  private def cancelAndClearEverything(cause: Throwable)
    : Seq[Future[Unit]] = {
    val futures = for ((jobID, (eg, jobInfo)) <- currentJobs) yield {
      future {
        eg.suspend(cause)

        jobInfo.notifyNonDetachedClients(
          decorateMessage(
            Failure(
              new JobExecutionException(jobID, "All jobs are cancelled and cleared.", cause))))
      }(context.dispatcher)
    }

    currentJobs.clear()

    futures.toSeq
  }

  override def grantLeadership(newLeaderSessionID: UUID): Unit = {
    self ! decorateMessage(GrantLeadership(Option(newLeaderSessionID)))
  }

  override def revokeLeadership(): Unit = {
    self ! decorateMessage(RevokeLeadership)
  }

  override def onAddedJobGraph(jobId: JobID): Unit = {
    if (leaderSessionID.isDefined && !currentJobs.contains(jobId)) {
      self ! decorateMessage(RecoverJob(jobId))
    }
  }

  override def onRemovedJobGraph(jobId: JobID): Unit = {
    if (leaderSessionID.isDefined) {
      currentJobs.get(jobId).foreach(
        job =>
          future {
            // Fail the execution graph
            job._1.fail(new IllegalStateException("Another JobManager removed the job from " +
              "ZooKeeper."))
          }(context.dispatcher)
      )
    }
  }

  override def getAddress: String = {
    AkkaUtils.getAkkaURL(context.system, self)
  }

  /** Handles error occurring in the leader election service
    *
    * @param exception Exception being thrown in the leader election service
    */
  override def handleError(exception: Exception): Unit = {
    log.error("Received an error from the LeaderElectionService.", exception)

    // terminate JobManager in case of an error
    self ! decorateMessage(PoisonPill)
  }
  
  /**
   * Updates the accumulators reported from a task manager via the Heartbeat message.
   *
   * @param accumulators list of accumulator snapshots
   */
  private def updateAccumulators(accumulators : Seq[AccumulatorSnapshot]) = {
    accumulators foreach {
      case accumulatorEvent =>
        currentJobs.get(accumulatorEvent.getJobID) match {
          case Some((jobGraph, jobInfo)) =>
            future {
              jobGraph.updateAccumulators(accumulatorEvent)
            }(context.dispatcher)
          case None =>
          // ignore accumulator values for old job
        }
    }
  }

  /**
    * Shutdown method which may be overridden for testing.
    */
  protected def shutdown() : Unit = {
    // Await actor system termination and shut down JVM
    new ProcessShutDownThread(
      log.logger,
      context.system,
      FiniteDuration(10, SECONDS)).start()

    // Shutdown and discard all queued messages
    context.system.shutdown()
  }

  private def instantiateMetrics(jobManagerMetricGroup: MetricGroup) : Unit = {
    jobManagerMetricGroup.gauge[Long, Gauge[Long]]("taskSlotsAvailable", new Gauge[Long] {
      override def getValue: Long = JobManager.this.instanceManager.getNumberOfAvailableSlots
    })
    jobManagerMetricGroup.gauge[Long, Gauge[Long]]("taskSlotsTotal", new Gauge[Long] {
      override def getValue: Long = JobManager.this.instanceManager.getTotalNumberOfSlots
    })
    jobManagerMetricGroup.gauge[Long, Gauge[Long]]("numRegisteredTaskManagers", new Gauge[Long] {
      override def getValue: Long
      = JobManager.this.instanceManager.getNumberOfRegisteredTaskManagers
    })
    jobManagerMetricGroup.gauge[Long, Gauge[Long]]("numRunningJobs", new Gauge[Long] {
      override def getValue: Long = JobManager.this.currentJobs.size
    })
    instantiateStatusMetrics(jobManagerMetricGroup)
  }

  private def instantiateStatusMetrics(jobManagerMetricGroup: MetricGroup) : Unit = {
    val jvm = jobManagerMetricGroup
      .addGroup("Status")
      .addGroup("JVM")

    instantiateClassLoaderMetrics(jvm.addGroup("ClassLoader"))
    instantiateGarbageCollectorMetrics(jvm.addGroup("GarbageCollector"))
    instantiateMemoryMetrics(jvm.addGroup("Memory"))
    instantiateThreadMetrics(jvm.addGroup("Threads"))
    instantiateCPUMetrics(jvm.addGroup("CPU"))
  }

  private def instantiateClassLoaderMetrics(metrics: MetricGroup) {
    val mxBean = ManagementFactory.getClassLoadingMXBean

    metrics.gauge[Long, Gauge[Long]]("ClassesLoaded", new Gauge[Long] {
      override def getValue: Long = mxBean.getTotalLoadedClassCount
    })
    metrics.gauge[Long, Gauge[Long]]("ClassesUnloaded", new Gauge[Long] {
      override def getValue: Long = mxBean.getUnloadedClassCount
    })
  }

  private def instantiateGarbageCollectorMetrics(metrics: MetricGroup) {
    val garbageCollectors = ManagementFactory.getGarbageCollectorMXBeans

    for (garbageCollector <- garbageCollectors.asScala) {
      val gcGroup = metrics.addGroup(garbageCollector.getName)
      gcGroup.gauge[Long, Gauge[Long]]("Count", new Gauge[Long] {
        override def getValue: Long = garbageCollector.getCollectionCount
      })
      gcGroup.gauge[Long, Gauge[Long]]("Time", new Gauge[Long] {
        override def getValue: Long = garbageCollector.getCollectionTime
      })
    }
  }

  private def instantiateMemoryMetrics(metrics: MetricGroup) {
    val mxBean = ManagementFactory.getMemoryMXBean
    val heap = metrics.addGroup("Heap")
    heap.gauge[Long, Gauge[Long]]("Used", new Gauge[Long] {
      override def getValue: Long = mxBean.getHeapMemoryUsage.getUsed
    })
    heap.gauge[Long, Gauge[Long]]("Committed", new Gauge[Long] {
      override def getValue: Long = mxBean.getHeapMemoryUsage.getCommitted
    })
    heap.gauge[Long, Gauge[Long]]("Max", new Gauge[Long] {
      override def getValue: Long = mxBean.getHeapMemoryUsage.getMax
    })

    val nonHeap = metrics.addGroup("NonHeap")
    nonHeap.gauge[Long, Gauge[Long]]("Used", new Gauge[Long] {
      override def getValue: Long = mxBean.getNonHeapMemoryUsage.getUsed
    })
    nonHeap.gauge[Long, Gauge[Long]]("Committed", new Gauge[Long] {
      override def getValue: Long = mxBean.getNonHeapMemoryUsage.getCommitted
    })
    nonHeap.gauge[Long, Gauge[Long]]("Max", new Gauge[Long] {
      override def getValue: Long = mxBean.getNonHeapMemoryUsage.getMax
    })

    val con = ManagementFactory.getPlatformMBeanServer;

    val directObjectName = new ObjectName("java.nio:type=BufferPool,name=direct")

    val direct = metrics.addGroup("Direct")
    direct.gauge[Long, Gauge[Long]]("Count", new Gauge[Long] {
      override def getValue: Long = con
        .getAttribute(directObjectName, "Count").asInstanceOf[Long]
    })
    direct.gauge[Long, Gauge[Long]]("MemoryUsed", new Gauge[Long] {
      override def getValue: Long = con
        .getAttribute(directObjectName, "MemoryUsed").asInstanceOf[Long]
    })
    direct.gauge[Long, Gauge[Long]]("TotalCapacity", new Gauge[Long] {
      override def getValue: Long = con
        .getAttribute(directObjectName, "TotalCapacity").asInstanceOf[Long]
    })

    val mappedObjectName = new ObjectName("java.nio:type=BufferPool,name=mapped")

    val mapped = metrics.addGroup("Mapped")
    mapped.gauge[Long, Gauge[Long]]("Count", new Gauge[Long] {
      override def getValue: Long = con
        .getAttribute(mappedObjectName, "Count").asInstanceOf[Long]
    })
    mapped.gauge[Long, Gauge[Long]]("MemoryUsed", new Gauge[Long] {
      override def getValue: Long = con
        .getAttribute(mappedObjectName, "MemoryUsed").asInstanceOf[Long]
    })
    mapped.gauge[Long, Gauge[Long]]("TotalCapacity", new Gauge[Long] {
      override def getValue: Long = con
        .getAttribute(mappedObjectName, "TotalCapacity").asInstanceOf[Long]
    })
  }

  private def instantiateThreadMetrics(metrics: MetricGroup): Unit = {
    val mxBean = ManagementFactory.getThreadMXBean

    metrics.gauge[Int, Gauge[Int]]("Count", new Gauge[Int] {
      override def getValue: Int = mxBean.getThreadCount
    })
  }

  private def instantiateCPUMetrics(metrics: MetricGroup): Unit = {
    try {
      val mxBean = ManagementFactory.getOperatingSystemMXBean
        .asInstanceOf[com.sun.management.OperatingSystemMXBean]

      metrics.gauge[Double, Gauge[Double]]("Load", new Gauge[Double] {
        override def getValue: Double = mxBean.getProcessCpuLoad
      })
      metrics.gauge[Long, Gauge[Long]]("Time", new Gauge[Long] {
        override def getValue: Long = mxBean.getProcessCpuTime
      })
    }
    catch {
      case t: Throwable =>
        log.warn("Cannot access com.sun.management.OperatingSystemMXBean.getProcessCpuLoad()" +
          " - CPU load metrics will not be available.")
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
    SignalHandler.register(LOG.logger)
    JvmShutdownSafeguard.installAsShutdownHook(LOG.logger)

    // parsing the command line arguments
    val (configuration: Configuration,
         executionMode: JobManagerMode,
         listeningHost: String,
         listeningPortRange: java.util.Iterator[Integer]) =
    try {
      parseArgs(args)
    }
    catch {
      case t: Throwable =>
        LOG.error(t.getMessage(), t)
        System.exit(STARTUP_FAILURE_RETURN_CODE)
        null
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

    if (!listeningPortRange.hasNext) {
      if (ZooKeeperUtils.isZooKeeperRecoveryMode(configuration)) {
        val message = "Config parameter '" + ConfigConstants.HA_JOB_MANAGER_PORT +
          "' does not specify a valid port range."
        LOG.error(message)
        System.exit(STARTUP_FAILURE_RETURN_CODE)
      }
      else {
        val message = s"Config parameter '" + ConfigConstants.JOB_MANAGER_IPC_PORT_KEY +
          "' does not specify a valid port."
        LOG.error(message)
        System.exit(STARTUP_FAILURE_RETURN_CODE)
      }
    }

    // run the job manager
    SecurityContext.install(new SecurityConfiguration().setFlinkConfiguration(configuration))

    try {
      SecurityContext.getInstalled.runSecured(new FlinkSecuredRunner[Unit] {
        override def run(): Unit = {
          runJobManager(
            configuration,
            executionMode,
            listeningHost,
            listeningPortRange)
        }
      })
    } catch {
      case t: Throwable =>
        LOG.error("Failed to run JobManager.", t)
        System.exit(STARTUP_FAILURE_RETURN_CODE)
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
   * @param listeningAddress The hostname where the JobManager should listen for messages.
   * @param listeningPort The port where the JobManager should listen for messages.
   */
  def runJobManager(
      configuration: Configuration,
      executionMode: JobManagerMode,
      listeningAddress: String,
      listeningPort: Int)
    : Unit = {

    val (jobManagerSystem, _, _, webMonitorOption, _) = startActorSystemAndJobManagerActors(
      configuration,
      executionMode,
      listeningAddress,
      listeningPort,
      classOf[JobManager],
      classOf[MemoryArchivist],
      Option(classOf[StandaloneResourceManager])
    )

    // block until everything is shut down
    jobManagerSystem.awaitTermination()

    webMonitorOption.foreach{
      webMonitor =>
        try {
          webMonitor.stop()
        } catch {
          case t: Throwable =>
            LOG.warn("Could not properly stop the web monitor.", t)
        }
    }
  }

  /**
    * Starts and runs the JobManager with all its components trying to bind to
    * a port in the specified range.
    *
    * @param configuration The configuration object for the JobManager.
    * @param executionMode The execution mode in which to run. Execution mode LOCAL will spawn an
    *                      an additional TaskManager in the same process.
    * @param listeningAddress The hostname where the JobManager should listen for messages.
    * @param listeningPortRange The port range where the JobManager should listen for messages.
    */
  def runJobManager(
      configuration: Configuration,
      executionMode: JobManagerMode,
      listeningAddress: String,
      listeningPortRange: java.util.Iterator[Integer])
    : Unit = {

    val result = retryOnBindException({
      // Try all ports in the range until successful
      val socket = NetUtils.createSocketFromPorts(
        listeningPortRange,
        new NetUtils.SocketFactory {
          override def createSocket(port: Int): ServerSocket = new ServerSocket(
            // Use the correct listening address, bound ports will only be
            // detected later by Akka.
            port, 0, InetAddress.getByName(listeningAddress))
        })

      val port =
        if (socket == null) {
          throw new BindException(s"Unable to allocate port for JobManager.")
        } else {
          try {
            socket.getLocalPort()
          } finally {
            socket.close()
          }
        }

      runJobManager(configuration, executionMode, listeningAddress, port)
    }, { !listeningPortRange.hasNext }, 5000)

    result match {
      case scala.util.Failure(f) => throw f
      case _ =>
    }
  }

  /**
    * Retries a function if it fails because of a [[java.net.BindException]].
    *
    * @param fn The function to retry
    * @param stopCond Flag to signal termination
    * @param maxSleepBetweenRetries Max random sleep time between retries
    * @tparam T Return type of the the function to retry
    * @return Return value of the the function to retry
    */
  @tailrec
  def retryOnBindException[T](
      fn: => T,
      stopCond: => Boolean,
      maxSleepBetweenRetries : Long = 0 )
    : scala.util.Try[T] = {

    def sleepBeforeRetry() : Unit = {
      if (maxSleepBetweenRetries > 0) {
        val sleepTime = (Math.random() * maxSleepBetweenRetries).asInstanceOf[Long]
        LOG.info(s"Retrying after bind exception. Sleeping for ${sleepTime} ms.")
        Thread.sleep(sleepTime)
      }
    }

    scala.util.Try {
      fn
    } match {
      case scala.util.Failure(x: BindException) =>
        if (stopCond) {
          scala.util.Failure(new RuntimeException(
            "Unable to do further retries starting the actor system"))
        } else {
          sleepBeforeRetry()
          retryOnBindException(fn, stopCond)
        }
      case scala.util.Failure(x: Exception) => x.getCause match {
        case c: ChannelException =>
          if (stopCond) {
            scala.util.Failure(new RuntimeException(
              "Unable to do further retries starting the actor system"))
          } else {
            sleepBeforeRetry()
            retryOnBindException(fn, stopCond)
          }
        case _ => scala.util.Failure(x)
      }
      case f => f
    }
  }

  /** Starts an ActorSystem, the JobManager and all its components including the WebMonitor.
    *
    * @param configuration The configuration object for the JobManager
    * @param executionMode The execution mode in which to run. Execution mode LOCAL with spawn an
    *                      additional TaskManager in the same process.
    * @param listeningAddress The hostname where the JobManager should listen for messages.
    * @param listeningPort The port where the JobManager should listen for messages
    * @param jobManagerClass The class of the JobManager to be started
    * @param archiveClass The class of the Archivist to be started
    * @param resourceManagerClass Optional class of resource manager if one should be started
    * @return A tuple containing the started ActorSystem, ActorRefs to the JobManager and the
    *         Archivist and an Option containing a possibly started WebMonitor
    */
  def startActorSystemAndJobManagerActors(
      configuration: Configuration,
      executionMode: JobManagerMode,
      listeningAddress: String,
      listeningPort: Int,
      jobManagerClass: Class[_ <: JobManager],
      archiveClass: Class[_ <: MemoryArchivist],
      resourceManagerClass: Option[Class[_ <: FlinkResourceManager[_]]])
    : (ActorSystem, ActorRef, ActorRef, Option[WebMonitor], Option[ActorRef]) = {

    LOG.info("Starting JobManager")

    // Bring up the job manager actor system first, bind it to the given address.
    val hostPortUrl = NetUtils.hostAndPortToUrlString(listeningAddress, listeningPort)
    LOG.info(s"Starting JobManager actor system at $hostPortUrl")

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
      case t: Throwable =>
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

    val address = AkkaUtils.getAddress(jobManagerSystem)

    configuration.setString(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, address.host.get)
    configuration.setInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY, address.port.get)

    val webMonitor: Option[WebMonitor] =
      if (configuration.getInteger(ConfigConstants.JOB_MANAGER_WEB_PORT_KEY, 0) >= 0) {
        LOG.info("Starting JobManager web frontend")
        val leaderRetrievalService = LeaderRetrievalUtils
          .createLeaderRetrievalService(configuration)

        // start the web frontend. we need to load this dynamically
        // because it is not in the same project/dependencies
        val webServer = WebMonitorUtils.startWebRuntimeMonitor(
          configuration,
          leaderRetrievalService,
          jobManagerSystem)

        Option(webServer)
      }
      else {
        None
      }

    // Reset the port (necessary in case of automatic port selection)
    webMonitor.foreach{ monitor => configuration.setInteger(
      ConfigConstants.JOB_MANAGER_WEB_PORT_KEY, monitor.getServerPort) }

    try {
      // bring up the job manager actor
      LOG.info("Starting JobManager actor")
      val (jobManager, archive) = startJobManagerActors(
        configuration,
        jobManagerSystem,
        jobManagerClass,
        archiveClass)

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
          ResourceID.generate(),
          jobManagerSystem,
          listeningAddress,
          Some(TaskManager.TASK_MANAGER_NAME),
          None,
          localTaskManagerCommunication = true,
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

      // start web monitor
      webMonitor.foreach {
        monitor =>
          val jobManagerAkkaUrl = JobManager.getRemoteJobManagerAkkaURL(configuration)
          monitor.start(jobManagerAkkaUrl)
      }

      val resourceManager =
        resourceManagerClass match {
          case Some(rmClass) =>
            LOG.debug("Starting Resource manager actor")
            Option(
              FlinkResourceManager.startResourceManagerActors(
                configuration,
                jobManagerSystem,
                LeaderRetrievalUtils.createLeaderRetrievalService(configuration),
                rmClass))
          case None =>
            LOG.info("Resource Manager class not provided. No resource manager will be started.")
            None
        }

      (jobManagerSystem, jobManager, archive, webMonitor, resourceManager)
    }
    catch {
      case t: Throwable =>
        LOG.error("Error while starting up JobManager", t)
        try {
          jobManagerSystem.shutdown()
        } catch {
          case tt: Throwable => LOG.warn("Could not cleanly shut down actor system", tt)
        }
        throw t
    }
  }

  /**
   * Loads the configuration, execution mode and the listening address from the provided command
   * line arguments.
   *
   * @param args command line arguments
   * @return Quadruple of configuration, execution mode and an optional listening address
   */
  def parseArgs(args: Array[String])
    : (Configuration, JobManagerMode, String, java.util.Iterator[Integer]) = {
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

      opt[String]("host").optional().action { (arg, conf) =>
        conf.setHost(arg)
        conf
      } text {
        "Network address for communication with the job manager"
      }

      opt[Int]("webui-port").optional().action { (arg, conf) =>
        conf.setWebUIPort(arg)
        conf
      } text {
        "Port for the UI web server"
      }
    }

    val config = parser.parse(args, new JobManagerCliOptions()).getOrElse {
      throw new Exception(
        s"Invalid command line arguments: ${args.mkString(" ")}. Usage: ${parser.usage}")
    }
    
    val configDir = config.getConfigDir()
    
    if (configDir == null) {
      throw new Exception("Missing parameter '--configDir'")
    }
    if (config.getJobManagerMode() == null) {
      throw new Exception("Missing parameter '--executionMode'")
    }

    LOG.info("Loading configuration from " + configDir)
    val configuration = GlobalConfiguration.loadConfiguration(configDir)

    try {
      FileSystem.setDefaultScheme(configuration)
    }
    catch {
      case e: IOException => {
        throw new Exception("Error while setting the default " +
          "filesystem scheme from configuration.", e)
      }
    }

    if (new File(configDir).isDirectory) {
      configuration.setString(ConfigConstants.FLINK_BASE_DIR_PATH_KEY, configDir + "/..")
    }

    if (config.getWebUIPort() >= 0) {
      configuration.setInteger(ConfigConstants.JOB_MANAGER_WEB_PORT_KEY, config.getWebUIPort())
    }

    if (config.getHost() != null) {
      configuration.setString(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, config.getHost())
    }

    val host = configuration.getString(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, null)

    val portRange =
      // high availability mode
      if (ZooKeeperUtils.isZooKeeperRecoveryMode(configuration)) {
        LOG.info("Starting JobManager with high-availability")

        configuration.setInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY, 0)

        // The port range of allowed job manager ports or 0 for random
        configuration.getValue(HighAvailabilityOptions.HA_JOB_MANAGER_PORT_RANGE)
      }
      else {
        LOG.info("Starting JobManager without high-availability")

        // In standalone mode, we don't allow port ranges
        val listeningPort = configuration.getInteger(
          ConfigConstants.JOB_MANAGER_IPC_PORT_KEY,
          ConfigConstants.DEFAULT_JOB_MANAGER_IPC_PORT)

        if (listeningPort <= 0 || listeningPort >= 65536) {
          val message = "Config parameter '" + ConfigConstants.JOB_MANAGER_IPC_PORT_KEY +
            "' is invalid, it must be greater than 0 and less than 65536."
          LOG.error(message)
          System.exit(STARTUP_FAILURE_RETURN_CODE)
        }

        String.valueOf(listeningPort)
      }

    val executionMode = config.getJobManagerMode
    val hostUrl = NetUtils.ipAddressToUrlString(InetAddress.getByName(host))

    LOG.info(s"Starting JobManager on $hostUrl:$portRange with execution mode $executionMode")

    val portRangeIterator = NetUtils.getPortRangeFromString(portRange)

    (configuration, executionMode, host, portRangeIterator)
  }

  /**
   * Create the job manager components as (instanceManager, scheduler, libraryCacheManager,
   *              archiverProps, defaultExecutionRetries,
   *              delayBetweenRetries, timeout)
   *
   * @param configuration The configuration from which to parse the config values.
   * @param leaderElectionServiceOption LeaderElectionService which shall be returned if the option
   *                                    is defined
   * @return The members for a default JobManager.
   */
  def createJobManagerComponents(
      configuration: Configuration,
      leaderElectionServiceOption: Option[LeaderElectionService]) :
    (ExecutorService,
    InstanceManager,
    FlinkScheduler,
    BlobLibraryCacheManager,
    RestartStrategyFactory,
    FiniteDuration, // timeout
    Int, // number of archived jobs
    LeaderElectionService,
    SubmittedJobGraphStore,
    CheckpointRecoveryFactory,
    SavepointStore,
    FiniteDuration, // timeout for job recovery
    Option[FlinkMetricRegistry]
   ) = {

    val timeout: FiniteDuration = AkkaUtils.getTimeout(configuration)

    val cleanupInterval = configuration.getLong(
      ConfigConstants.LIBRARY_CACHE_MANAGER_CLEANUP_INTERVAL,
      ConfigConstants.DEFAULT_LIBRARY_CACHE_MANAGER_CLEANUP_INTERVAL) * 1000

    val restartStrategy = RestartStrategyFactory.createRestartStrategyFactory(configuration)

    val archiveCount = configuration.getInteger(ConfigConstants.JOB_MANAGER_WEB_ARCHIVE_COUNT,
      ConfigConstants.DEFAULT_JOB_MANAGER_WEB_ARCHIVE_COUNT)


    var blobServer: BlobServer = null
    var instanceManager: InstanceManager = null
    var scheduler: FlinkScheduler = null
    var libraryCacheManager: BlobLibraryCacheManager = null

    val executorService: ExecutorService = new ForkJoinPool()
    
    try {
      blobServer = new BlobServer(configuration)
      instanceManager = new InstanceManager()
      scheduler = new FlinkScheduler(ExecutionContext.fromExecutor(executorService))
      libraryCacheManager = new BlobLibraryCacheManager(blobServer, cleanupInterval)

      instanceManager.addInstanceListener(scheduler)
    }
    catch {
      case t: Throwable =>
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
        executorService.shutdownNow()
        
        throw t
    }

    // Create recovery related components
    val (leaderElectionService, submittedJobGraphs, checkpointRecoveryFactory) =
      HighAvailabilityMode.fromConfig(configuration) match {
        case HighAvailabilityMode.NONE =>
          val leaderElectionService = leaderElectionServiceOption match {
            case Some(les) => les
            case None => new StandaloneLeaderElectionService()
          }

          (leaderElectionService,
            new StandaloneSubmittedJobGraphStore(),
            new StandaloneCheckpointRecoveryFactory())

        case HighAvailabilityMode.ZOOKEEPER =>
          val client = ZooKeeperUtils.startCuratorFramework(configuration)

          val leaderElectionService = leaderElectionServiceOption match {
            case Some(les) => les
            case None => ZooKeeperUtils.createLeaderElectionService(client, configuration)
          }

          (leaderElectionService,
            ZooKeeperUtils.createSubmittedJobGraphs(client, configuration),
            new ZooKeeperCheckpointRecoveryFactory(client, configuration))
      }

    val savepointStore = SavepointStoreFactory.createFromConfig(configuration)

    val jobRecoveryTimeoutStr = configuration.getValue(HighAvailabilityOptions.HA_JOB_DELAY)

    val jobRecoveryTimeout = if (jobRecoveryTimeoutStr == null || jobRecoveryTimeoutStr.isEmpty) {
      timeout
    } else {
      try {
        FiniteDuration(Duration(jobRecoveryTimeoutStr).toMillis, TimeUnit.MILLISECONDS)
      } catch {
        case n: NumberFormatException =>
          throw new Exception(
            s"Invalid config value for ${HighAvailabilityOptions.HA_JOB_DELAY.key()}: " +
              s"$jobRecoveryTimeoutStr. Value must be a valid duration (such as '10 s' or '1 min')")
      }
    }

    val metricRegistry = try {
      Option(new FlinkMetricRegistry(MetricRegistryConfiguration.fromConfiguration(configuration)))
    } catch {
      case _: Exception =>
        None
    }

    (executorService,
      instanceManager,
      scheduler,
      libraryCacheManager,
      restartStrategy,
      timeout,
      archiveCount,
      leaderElectionService,
      submittedJobGraphs,
      checkpointRecoveryFactory,
      savepointStore,
      jobRecoveryTimeout,
      metricRegistry)
  }

  /**
   * Starts the JobManager and job archiver based on the given configuration, in
   * the given actor system.
   *
   * @param configuration The configuration for the JobManager
   * @param actorSystem The actor system running the JobManager
   * @param jobManagerClass The class of the JobManager to be started
   * @param archiveClass The class of the MemoryArchivist to be started
    * @return A tuple of references (JobManager Ref, Archiver Ref)
   */
  def startJobManagerActors(
      configuration: Configuration,
      actorSystem: ActorSystem,
      jobManagerClass: Class[_ <: JobManager],
      archiveClass: Class[_ <: MemoryArchivist])
    : (ActorRef, ActorRef) = {

    startJobManagerActors(
      configuration,
      actorSystem,
      Some(JOB_MANAGER_NAME),
      Some(ARCHIVE_NAME),
      jobManagerClass,
      archiveClass)
  }

  /**
   * Starts the JobManager and job archiver based on the given configuration, in the
   * given actor system.
   *
   * @param configuration The configuration for the JobManager
   * @param actorSystem The actor system running the JobManager
   * @param jobManagerActorName Optionally the name of the JobManager actor. If none is given,
   *                          the actor will have the name generated by the actor system.
   * @param archiveActorName Optionally the name of the archive actor. If none is given,
   *                          the actor will have the name generated by the actor system.
   * @param jobManagerClass The class of the JobManager to be started
   * @param archiveClass The class of the MemoryArchivist to be started
    * @return A tuple of references (JobManager Ref, Archiver Ref)
   */
  def startJobManagerActors(
      configuration: Configuration,
      actorSystem: ActorSystem,
      jobManagerActorName: Option[String],
      archiveActorName: Option[String],
      jobManagerClass: Class[_ <: JobManager],
      archiveClass: Class[_ <: MemoryArchivist])
    : (ActorRef, ActorRef) = {

    val (executorService: ExecutorService,
    instanceManager,
    scheduler,
    libraryCacheManager,
    restartStrategy,
    timeout,
    archiveCount,
    leaderElectionService,
    submittedJobGraphs,
    checkpointRecoveryFactory,
    savepointStore,
    jobRecoveryTimeout, 
    metricsRegistry) = createJobManagerComponents(
      configuration,
      None)

    val archiveProps = getArchiveProps(archiveClass, archiveCount)

    // start the archiver with the given name, or without (avoid name conflicts)
    val archive: ActorRef = archiveActorName match {
      case Some(actorName) => actorSystem.actorOf(archiveProps, actorName)
      case None => actorSystem.actorOf(archiveProps)
    }

    val jobManagerProps = getJobManagerProps(
      jobManagerClass,
      configuration,
      executorService,
      instanceManager,
      scheduler,
      libraryCacheManager,
      archive,
      restartStrategy,
      timeout,
      leaderElectionService,
      submittedJobGraphs,
      checkpointRecoveryFactory,
      savepointStore,
      jobRecoveryTimeout,
      metricsRegistry)

    val jobManager: ActorRef = jobManagerActorName match {
      case Some(actorName) => actorSystem.actorOf(jobManagerProps, actorName)
      case None => actorSystem.actorOf(jobManagerProps)
    }
    
    metricsRegistry match {
      case Some(registry) =>
        registry.startQueryService(actorSystem)
      case None =>
    }

    (jobManager, archive)
  }

  def getArchiveProps(archiveClass: Class[_ <: MemoryArchivist], archiveCount: Int): Props = {
    Props(archiveClass, archiveCount)
  }

  def getJobManagerProps(
    jobManagerClass: Class[_ <: JobManager],
    configuration: Configuration,
    executorService: ExecutorService,
    instanceManager: InstanceManager,
    scheduler: FlinkScheduler,
    libraryCacheManager: BlobLibraryCacheManager,
    archive: ActorRef,
    restartStrategyFactory: RestartStrategyFactory,
    timeout: FiniteDuration,
    leaderElectionService: LeaderElectionService,
    submittedJobGraphStore: SubmittedJobGraphStore,
    checkpointRecoveryFactory: CheckpointRecoveryFactory,
    savepointStore: SavepointStore,
    jobRecoveryTimeout: FiniteDuration,
    metricsRegistry: Option[FlinkMetricRegistry]): Props = {

    Props(
      jobManagerClass,
      configuration,
      executorService,
      instanceManager,
      scheduler,
      libraryCacheManager,
      archive,
      restartStrategyFactory,
      timeout,
      leaderElectionService,
      submittedJobGraphStore,
      checkpointRecoveryFactory,
      savepointStore,
      jobRecoveryTimeout,
      metricsRegistry)
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
  def getRemoteJobManagerAkkaURL(
      address: InetSocketAddress,
      name: Option[String] = None)
    : String = {
    val hostPort = NetUtils.socketAddressToUrlString(address)

    getJobManagerAkkaURLHelper(s"akka.tcp://flink@$hostPort", name)
  }

  /**
   * Returns the JobManager actor's remote Akka URL, given the configured hostname and port.
   *
   * @param config The configuration to parse
   * @return JobManager actor remote Akka URL
   */
  def getRemoteJobManagerAkkaURL(config: Configuration) : String = {
    val (hostname, port) = TaskManager.getAndCheckJobManagerAddress(config)

    var hostPort: InetSocketAddress = null

    try {
      val inetAddress: InetAddress = InetAddress.getByName(hostname)
      hostPort = new InetSocketAddress(inetAddress, port)
    }
    catch {
      case e: UnknownHostException =>
        throw new UnknownHostException(s"Cannot resolve the JobManager hostname '$hostname' " +
          s"specified in the configuration")
    }

    JobManager.getRemoteJobManagerAkkaURL(hostPort, Option.empty)
  }

  /**
   * Builds the akka actor path for the JobManager actor to address the actor within
   * its own actor system.
   *
   * @return The local akka URL of the JobManager actor.
   */
  def getLocalJobManagerAkkaURL(name: Option[String] = None): String = {
    getJobManagerAkkaURLHelper("akka://flink", name)
  }

  def getJobManagerAkkaURL(system: ActorSystem, name: Option[String] = None): String = {
    getJobManagerAkkaURLHelper(AkkaUtils.getAddress(system).toString, name)
  }

  private def getJobManagerAkkaURLHelper(address: String, name: Option[String]): String = {
    address + "/user/" + name.getOrElse(JOB_MANAGER_NAME)
  }

  def getJobManagerActorRefFuture(
      address: InetSocketAddress,
      system: ActorSystem,
      timeout: FiniteDuration)
    : Future[ActorRef] = {
    AkkaUtils.getActorRefFuture(getRemoteJobManagerAkkaURL(address), system, timeout)
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
  def getJobManagerActorRef(
      jobManagerUrl: String,
      system: ActorSystem,
      timeout: FiniteDuration)
    : ActorRef = {
    AkkaUtils.getActorRef(jobManagerUrl, system, timeout)
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
  def getJobManagerActorRef(
      address: InetSocketAddress,
      system: ActorSystem,
      timeout: FiniteDuration)
    : ActorRef = {

    val jmAddress = getRemoteJobManagerAkkaURL(address)
    getJobManagerActorRef(jmAddress, system, timeout)
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
  def getJobManagerActorRef(
      address: InetSocketAddress,
      system: ActorSystem,
      config: Configuration)
    : ActorRef = {

    val timeout = AkkaUtils.getLookupTimeout(config)
    getJobManagerActorRef(address, system, timeout)
  }
}
