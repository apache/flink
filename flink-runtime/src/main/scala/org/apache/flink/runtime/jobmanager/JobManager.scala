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

import java.io.{IOException, File}
import java.net.InetSocketAddress
import akka.actor.Status.Failure
import akka.actor._
import akka.pattern.ask
import org.apache.flink.configuration.{ConfigConstants, GlobalConfiguration, Configuration}
import org.apache.flink.core.io.InputSplitAssigner
import org.apache.flink.runtime.blob.BlobServer
import org.apache.flink.runtime.executiongraph.{ExecutionJobVertex, ExecutionGraph}
import org.apache.flink.runtime.messages.ArchiveMessages.ArchiveExecutionGraph
import org.apache.flink.runtime.messages.ExecutionGraphMessages.JobStatusChanged
import org.apache.flink.runtime.messages.Messages.Acknowledge
import org.apache.flink.runtime.taskmanager.TaskManager
import org.apache.flink.runtime.util.EnvironmentInformation
import org.apache.flink.runtime.{JobException, ActorLogMessages}
import org.apache.flink.runtime.akka.AkkaUtils
import org.apache.flink.runtime.execution.librarycache.BlobLibraryCacheManager
import org.apache.flink.runtime.instance.InstanceManager
import org.apache.flink.runtime.jobgraph.{JobGraph, JobStatus, JobID}
import org.apache.flink.runtime.jobmanager.accumulators.AccumulatorManager
import org.apache.flink.runtime.jobmanager.scheduler.{Scheduler => FlinkScheduler}
import org.apache.flink.runtime.messages.JobManagerMessages._
import org.apache.flink.runtime.messages.RegistrationMessages._
import org.apache.flink.runtime.messages.TaskManagerMessages.{NextInputSplit, Heartbeat}
import org.apache.flink.runtime.profiling.ProfilingUtils
import org.slf4j.LoggerFactory
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.language.postfixOps
import org.apache.flink.util.InstantiationUtil

/**
 * The job manager is responsible for receiving Flink jobs, scheduling the tasks, gathering the
 * job status and managing the task managers. It is realized as an actor and receives amongst others
 * the following messages:
 *
 *  - [[RegisterTaskManager]] is sent by a TaskManager which wants to registe at the job manager.
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
 * [[org.apache.flink.runtime.executiongraph.ExecutionVertex]] contained in the [[ExecutionGraph]].
 * A successful update is acknowledged by true and otherwise false.
 *
 * - [[RequestNextInputSplit]] requests the next input split for a running task on a
 * [[TaskManager]]. The assigned input split or null is sent to the sender in the form of the
 * message [[NextInputSplit]].
 *
 * - [[JobStatusChanged]] indicates that the status of job (RUNNING, CANCELING, FINISHED, etc.) has
 * changed. This message is sent by the ExecutionGraph.
 *
 * @param configuration object with user provided configuration values
 */
class JobManager(val configuration: Configuration) extends 
Actor with ActorLogMessages with ActorLogging {
  import context._
  import scala.collection.JavaConverters._

  implicit val timeout = AkkaUtils.getTimeout(configuration)

  log.info(s"Starting job manager at ${self.path}.")

  checkJavaVersion

  val (archiveCount,
    profiling,
    cleanupInterval,
    defaultExecutionRetries,
    delayBetweenRetries) = JobManager.parseConfiguration(configuration)

  // Props for the profiler actor
  def profilerProps: Props = Props(classOf[JobManagerProfiler])

  // Props for the archive actor
  def archiveProps: Props = Props(classOf[MemoryArchivist], archiveCount)

  val profiler = profiling match {
    case true => Some(context.actorOf(profilerProps, JobManager.PROFILER_NAME))
    case false => None
  }

  val archive = context.actorOf(archiveProps, JobManager.ARCHIVE_NAME)

  val accumulatorManager = new AccumulatorManager(Math.min(1, archiveCount))
  val instanceManager = new InstanceManager()
  val scheduler = new FlinkScheduler()
  val libraryCacheManager = new BlobLibraryCacheManager(new BlobServer(), cleanupInterval)

  // List of current jobs running
  val currentJobs = scala.collection.mutable.HashMap[JobID, (ExecutionGraph, JobInfo)]()

  // Map of actors which want to be notified once a specific job terminates
  val finalJobStatusListener = scala.collection.mutable.HashMap[JobID, Set[ActorRef]]()

  instanceManager.addInstanceListener(scheduler)

  log.info("Started job manager. Waiting for incoming messages.")

  override def postStop(): Unit = {
    log.info(s"Stopping job manager ${self.path}.")

    for((e,_) <- currentJobs.values){
      e.fail(new Exception("The JobManager is shutting down."))
    }

    instanceManager.shutdown()
    scheduler.shutdown()

    try {
      libraryCacheManager.shutdown()
    } catch {
      case e: IOException => log.error(e, "Could not properly shutdown the library cache manager.")
    }

    if(log.isDebugEnabled) {
      log.debug("Job manager {} is completely stopped.", self.path)
    }
  }

  override def receiveWithLogMessages: Receive = {
    case RegisterTaskManager(connectionInfo, hardwareInformation, numberOfSlots) =>
      val taskManager = sender
      val instanceID = instanceManager.registerTaskManager(taskManager, connectionInfo,
        hardwareInformation, numberOfSlots)

      // TaskManager is already registered
      if(instanceID == null){
        val instanceID = instanceManager.getRegisteredInstance(taskManager).getId
        taskManager ! AlreadyRegistered(instanceID, libraryCacheManager.getBlobServerPort)
      } else {
        // to be notified when the taskManager is no longer reachable
        context.watch(taskManager)

        taskManager ! AcknowledgeRegistration(instanceID, libraryCacheManager.getBlobServerPort)
      }


    case RequestNumberRegisteredTaskManager =>
      sender ! instanceManager.getNumberOfRegisteredTaskManagers

    case RequestTotalNumberOfSlots =>
      sender ! instanceManager.getTotalNumberOfSlots

    case SubmitJob(jobGraph, listen, d) =>
      submitJob(jobGraph, listenToEvents = listen, detached = d)

    case CancelJob(jobID) =>
      log.info("Trying to cancel job with ID {}.", jobID)

      currentJobs.get(jobID) match {
        case Some((executionGraph, _)) =>
          // execute the cancellation asynchronously
          Future {
            executionGraph.cancel()
          }

          sender ! CancellationSuccess(jobID)
        case None =>
          log.info("No job found with ID {}.", jobID)
          sender ! CancellationFailure(jobID, new IllegalArgumentException("No job found with " +
            s"ID $jobID."))
      }

    case UpdateTaskExecutionState(taskExecutionState) =>
      if(taskExecutionState == null){
        sender ! false
      }else {
        currentJobs.get(taskExecutionState.getJobID) match {
          case Some((executionGraph, _)) =>
            val originalSender = sender
            Future {
              originalSender ! executionGraph.updateState(taskExecutionState)
            }
          case None => log.error("Cannot find execution graph for ID {} to change state to {}.",
            taskExecutionState.getJobID, taskExecutionState.getExecutionState)
            sender ! false
        }
      }

    case RequestNextInputSplit(jobID, vertexID, executionAttempt) =>
      val nextInputSplit = currentJobs.get(jobID) match {
        case Some((executionGraph,_)) =>
          val execution = executionGraph.getRegisteredExecutions.get(executionAttempt)

          if(execution == null){
            log.error("Can not find Execution for attempt {}.", executionAttempt)
            null
          }else{
            val slot = execution.getAssignedResource
            val taskId = execution.getVertex.getParallelSubtaskIndex

            val host = if(slot != null){
              slot.getInstance().getInstanceConnectionInfo.getHostname
            }else{
              null
            }

            executionGraph.getJobVertex(vertexID) match {
              case vertex: ExecutionJobVertex => vertex.getSplitAssigner match {
                case splitAssigner: InputSplitAssigner =>
                  splitAssigner.getNextInputSplit(host, taskId)
                case _ =>
                  log.error("No InputSplitAssigner for vertex ID {}.", vertexID)
                  null
              }
              case _ =>
                log.error("Cannot find execution vertex for vertex ID {}.", vertexID)
                null
          }
        }
        case None =>
          log.error("Cannot find execution graph for job ID {}.", jobID)
          null
      }

      if(log.isDebugEnabled) {
        log.debug("Send next input split {}.", nextInputSplit)
      }
      
      val serializedData = InstantiationUtil.serializeObject(nextInputSplit)
      
      sender ! NextInputSplit(serializedData)

    case JobStatusChanged(jobID, newJobStatus, timeStamp, optionalMessage) =>
      currentJobs.get(jobID) match {
        case Some((executionGraph, jobInfo)) => executionGraph.getJobName
          log.info("Status of job {} ({}) changed to {}{}.",
            jobID, executionGraph.getJobName, newJobStatus,
            if(optionalMessage == null) "" else optionalMessage)

          if(newJobStatus.isTerminalState) {
            jobInfo.end = timeStamp

            // is the client waiting for the job result?
            if(!jobInfo.detached) {
              newJobStatus match {
                case JobStatus.FINISHED =>
                  val accumulatorResults = accumulatorManager.getJobAccumulatorResults(jobID)
                  jobInfo.client ! JobResultSuccess(jobID, jobInfo.duration, accumulatorResults)
                case JobStatus.CANCELED =>
                  jobInfo.client ! JobResultCanceled(jobID, optionalMessage)
                case JobStatus.FAILED =>
                  jobInfo.client ! JobResultFailed(jobID, optionalMessage)
                case x => throw new IllegalArgumentException(s"$x is not a terminal state.")
              }
            }

            finalJobStatusListener.get(jobID) foreach {
              _ foreach {
                _ ! CurrentJobStatus(jobID, newJobStatus)
              }
            }

            removeJob(jobID)
          }
        case None =>
          removeJob(jobID)
      }

    case RequestFinalJobStatus(jobID) =>
      currentJobs.get(jobID) match {
        case Some(_) =>
          val listeners = finalJobStatusListener.getOrElse(jobID, Set())
          finalJobStatusListener += jobID -> (listeners + sender)
        case None =>
          // There is no job running with this job ID. Check the archive.
          archive forward RequestJobStatus(jobID)
      }

    case ScheduleOrUpdateConsumers(jobId, executionId, partitionIndex) =>
      currentJobs.get(jobId) match {
        case Some((executionGraph, _)) =>
          sender ! Acknowledge
          executionGraph.scheduleOrUpdateConsumers(executionId, partitionIndex)
        case None =>
          log.error("Cannot find execution graph for job ID {} to schedule or update consumers",
            jobId);
          sender ! Failure(new IllegalStateException("Cannot find execution graph for job ID " +
            jobId + " to schedule or update consumers."))
      }

    case ReportAccumulatorResult(accumulatorEvent) =>
      accumulatorManager.processIncomingAccumulators(accumulatorEvent.getJobID,
        accumulatorEvent.getAccumulators(
          libraryCacheManager.getClassLoader(accumulatorEvent.getJobID)
        )
      )

    case RequestAccumulatorResults(jobID) =>
      import scala.collection.JavaConverters._
      sender ! AccumulatorResultsFound(jobID, accumulatorManager.getJobAccumulatorResults
        (jobID).asScala.toMap)

    case RequestJobStatus(jobID) =>
      currentJobs.get(jobID) match {
        case Some((executionGraph,_)) => sender ! CurrentJobStatus(jobID, executionGraph.getState)
        case None =>
          // check the archive
          archive forward RequestJobStatus(jobID)
      }

    case RequestRunningJobs =>
      val executionGraphs = currentJobs map {
        case (_, (eg, jobInfo)) => eg
      }

      sender ! RunningJobs(executionGraphs)

    case RequestJob(jobID) =>
      currentJobs.get(jobID) match {
        case Some((eg, _)) => sender ! JobFound(jobID, eg)
        case None =>
          // check the archive
          archive forward RequestJob(jobID)
      }

    case RequestBlobManagerPort =>
      sender ! libraryCacheManager.getBlobServerPort

    case RequestRegisteredTaskManagers =>
      import scala.collection.JavaConverters._
      sender ! RegisteredTaskManagers(instanceManager.getAllRegisteredInstances.asScala)

    case Heartbeat(instanceID) =>
      instanceManager.reportHeartBeat(instanceID)

    case Terminated(taskManager) =>
      log.info("Task manager {} terminated.", taskManager.path)
      JobManager.LOG.warn(s"Task manager ${taskManager.path} terminated.")
      instanceManager.unregisterTaskManager(taskManager)
      context.unwatch(taskManager)

    case RequestJobManagerStatus =>
      sender ! JobManagerStatusAlive
  }

  /**
   * Submits a job to the job manager. The job is registered at the libraryCacheManager which
   * creates the job's class loader. The job graph is appended to the corresponding execution
   * graph and the execution vertices are queued for scheduling.
   *
   * @param jobGraph representing the Flink job
   * @param listenToEvents true if the sender wants to listen to job status and execution state
   *                       change notificatinos. false if not.
   * @param detached true if the job runs in detached mode, meaning that the sender does not wait
   *                 for the result of the job. false otherwise.
   */
  private def submitJob(jobGraph: JobGraph, listenToEvents: Boolean, detached: Boolean): Unit = {
    try {
      if (jobGraph == null) {
        sender ! akka.actor.Status.Failure(new IllegalArgumentException("JobGraph must not be" +
          " null."))
      } else {
        log.info(s"Received job ${jobGraph.getJobID} (${jobGraph.getName}).")

        if (jobGraph.getNumberOfVertices == 0) {
          sender ! SubmissionFailure(jobGraph.getJobID, new IllegalArgumentException("Job is " +
            "empty."))
        } else {
          // Create the user code class loader
          libraryCacheManager.registerJob(jobGraph.getJobID, jobGraph.getUserJarBlobKeys)

          val userCodeLoader = libraryCacheManager.getClassLoader(jobGraph.getJobID)

          // see if there already exists an ExecutionGraph for the corresponding job ID
          val (executionGraph, jobInfo) = currentJobs.getOrElseUpdate(jobGraph.getJobID,
            (new ExecutionGraph(jobGraph.getJobID, jobGraph.getName,
              jobGraph.getJobConfiguration, timeout, jobGraph.getUserJarBlobKeys, userCodeLoader),
              JobInfo(sender, System.currentTimeMillis())))

          val jobNumberRetries = if (jobGraph.getNumberOfExecutionRetries >= 0) {
            jobGraph.getNumberOfExecutionRetries
          } else {
            defaultExecutionRetries
          }

          executionGraph.setNumberOfRetriesLeft(jobNumberRetries)
          executionGraph.setDelayBeforeRetrying(delayBetweenRetries)

          if (userCodeLoader == null) {
            throw new JobException("The user code class loader could not be initialized.")
          }

          if (log.isDebugEnabled) {
            log.debug(s"Running master initialization of job ${jobGraph.getJobID} (${
              jobGraph
                .getName
            }}).")
          }

          for (vertex <- jobGraph.getVertices.asScala) {
            val executableClass = vertex.getInvokableClassName
            if (executableClass == null || executableClass.length == 0) {
              throw new JobException(s"The vertex ${vertex.getID} (${vertex.getName}) has no " +
                s"invokable class.")
            }

            vertex.initializeOnMaster(userCodeLoader)
          }

          // topological sorting of the job vertices
          val sortedTopology = jobGraph.getVerticesSortedTopologicallyFromSources

          if (log.isDebugEnabled) {
            log.debug(s"Adding ${sortedTopology.size()} vertices from job graph " +
              s"${jobGraph.getJobID} (${jobGraph.getName}).")
          }

          executionGraph.attachJobGraph(sortedTopology)

          if (log.isDebugEnabled) {
            log.debug(s"Successfully created execution graph from job graph " +
              s"${jobGraph.getJobID} (${jobGraph.getName}).")
          }

          executionGraph.setScheduleMode(jobGraph.getScheduleMode)
          executionGraph.setQueuedSchedulingAllowed(jobGraph.getAllowQueuedScheduling)

          // get notified about job status changes
          executionGraph.registerJobStatusListener(self)

          if (listenToEvents) {
            // the sender wants to be notified about state changes
            executionGraph.registerExecutionListener(sender)
            executionGraph.registerJobStatusListener(sender)
          }

          jobInfo.detached = detached

          log.info(s"Scheduling job ${jobGraph.getName}.")

          executionGraph.scheduleForExecution(scheduler)

          sender ! SubmissionSuccess(jobGraph.getJobID)
        }
      }
    } catch {
      case t: Throwable =>
        log.error(t, "Job submission failed.")

        currentJobs.get(jobGraph.getJobID) match {
          case Some((executionGraph, jobInfo)) =>
            /*
             * Register self to be notified about job status changes in case that it did not happen
             * before. That way the proper cleanup of the job is triggered in the JobStatusChanged
             * handler.
             */
            val status = (self ? RequestFinalJobStatus(jobGraph.getJobID))(10 second)

            /*
             * if we cannot register as final job status listener, then send manually a
             * JobStatusChanged message with JobStatus.FAILED.
             */
            val selfActorRef = self
            status.onFailure{
              case _: Throwable => selfActorRef ! JobStatusChanged(executionGraph.getJobID,
                JobStatus.FAILED, System.currentTimeMillis(), s"Cleanup job ${jobGraph.getJobID}.")
            }

            /*
             * Don't send the client the final job status because we will send him a
             * SubmissionFailure.
             */
            jobInfo.detached = true

            executionGraph.fail(t)
          case None =>
            libraryCacheManager.unregisterJob(jobGraph.getJobID)
            currentJobs.remove(jobGraph.getJobID)
        }

        sender ! SubmissionFailure(jobGraph.getJobID, t)
    }
  }

  /**
   * Handle unmatched messages with an exception.
   */
  override def unhandled(message: Any): Unit = {
    throw new RuntimeException("Received unknown message " + message)
  }

  /**
   * Removes the job and sends it to the MemoryArchivist
   * @param jobID ID of the job to remove and archive
   */
  private def removeJob(jobID: JobID): Unit = {
    currentJobs.remove(jobID) match {
      case Some((eg, _)) =>
        eg.prepareForArchiving()
        archive ! ArchiveExecutionGraph(jobID, eg)

      case None =>
    }

    try {
      libraryCacheManager.unregisterJob(jobID)
    } catch {
      case t: Throwable =>
        log.error(t, "Could not properly unregister job {} form the library cache.", jobID)
    }
  }

  private def checkJavaVersion(): Unit = {
    if (System.getProperty("java.version").substring(0, 3).toDouble < 1.7) {
      log.warning("Warning: Flink is running with Java 6. " +
        "Java 6 is not maintained any more by Oracle or the OpenJDK community. " +
        "Flink currently supports Java 6, but may not in future releases," +
        " due to the unavailability of bug fixes security patched.")
    }
  }
}

object JobManager {
  import ExecutionMode._
  val LOG = LoggerFactory.getLogger(classOf[JobManager])
  val FAILURE_RETURN_CODE = 1
  val JOB_MANAGER_NAME = "jobmanager"
  val EVENT_COLLECTOR_NAME = "eventcollector"
  val ARCHIVE_NAME = "archive"
  val PROFILER_NAME = "profiler"

  def main(args: Array[String]): Unit = {
    EnvironmentInformation.logEnvironmentInfo(LOG, "JobManager")
    val (configuration, executionMode, listeningAddress) = parseArgs(args)

    val jobManagerSystem = AkkaUtils.createActorSystem(configuration, listeningAddress)

    startActor(Props(new JobManager(configuration) with WithWebServer))(jobManagerSystem)

    if(executionMode.equals(LOCAL)){
      TaskManager.startActorWithConfiguration("", configuration,
        localAkkaCommunication = false, localTaskManagerCommunication = true)(jobManagerSystem)
    }

    jobManagerSystem.awaitTermination()
  }

  /**
   * Loads the configuration, execution mode and the listening address from the provided command
   * line arguments.
   *
   * @param args command line arguments
   * @return triple of configuration, execution mode and an optional listening address
   */
  def parseArgs(args: Array[String]): (Configuration, ExecutionMode, Option[(String, Int)]) = {
    val parser = new scopt.OptionParser[JobManagerCLIConfiguration]("jobmanager") {
      head("flink jobmanager")
      opt[String]("configDir") action { (x, c) => c.copy(configDir = x) } text ("Specify " +
        "configuration directory.")
      opt[String]("executionMode") optional() action { (x, c) =>
        if(x.equals("local")){
          c.copy(executionMode = LOCAL)
        }else{
          c.copy(executionMode = CLUSTER)
        }
      } text {
        "Specify execution mode of job manager"
      }
    }

    parser.parse(args, JobManagerCLIConfiguration()) map {
      config =>
        GlobalConfiguration.loadConfiguration(config.configDir)

        val configuration = GlobalConfiguration.getConfiguration

        if (config.configDir != null && new File(config.configDir).isDirectory) {
          configuration.setString(ConfigConstants.FLINK_BASE_DIR_PATH_KEY, config.configDir + "/..")
        }

        val hostname = configuration.getString(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, null)
        val port = configuration.getInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY,
          ConfigConstants.DEFAULT_JOB_MANAGER_IPC_PORT)

        // Listening address on which the actor system listens for remote messages
        val listeningAddress = Some((hostname, port))

        (configuration, config.executionMode, listeningAddress)
    } getOrElse {
      LOG.error("CLI Parsing failed. Usage: " + parser.usage)
      sys.exit(FAILURE_RETURN_CODE)
    }
  }

  /**
   * Extracts the job manager configuration values from a configuration instance.
   *
   * @param configuration Object with the user provided configuration values
   * @return Tuple of (number of archived jobs, profiling enabled, cleanup interval of the library
   *         cache manager, default number of execution retries, delay between retries)
   */
  def parseConfiguration(configuration: Configuration): (Int, Boolean, Long, Int, Long) = {
    val archiveCount = configuration.getInteger(ConfigConstants.JOB_MANAGER_WEB_ARCHIVE_COUNT,
      ConfigConstants.DEFAULT_JOB_MANAGER_WEB_ARCHIVE_COUNT)
    val profilingEnabled = configuration.getBoolean(ProfilingUtils.PROFILE_JOB_KEY, true)

    val cleanupInterval = configuration.getLong(ConfigConstants
      .LIBRARY_CACHE_MANAGER_CLEANUP_INTERVAL,
      ConfigConstants.DEFAULT_LIBRARY_CACHE_MANAGER_CLEANUP_INTERVAL) * 1000

    val executionRetries = configuration.getInteger(ConfigConstants
      .DEFAULT_EXECUTION_RETRIES_KEY, ConfigConstants.DEFAULT_EXECUTION_RETRIES)

    val delayBetweenRetries = 2 * configuration.getLong(
      ConfigConstants.JOB_MANAGER_DEAD_TASKMANAGER_TIMEOUT_KEY,
      ConfigConstants.DEFAULT_JOB_MANAGER_DEAD_TASKMANAGER_TIMEOUT)

    (archiveCount, profilingEnabled, cleanupInterval, executionRetries, delayBetweenRetries)
  }

  def startActor(configuration: Configuration)(implicit actorSystem: ActorSystem): ActorRef = {
    startActor(Props(classOf[JobManager], configuration))
  }

  def startActor(props: Props)(implicit actorSystem: ActorSystem): ActorRef = {
    actorSystem.actorOf(props, JOB_MANAGER_NAME)
  }

  def getRemoteAkkaURL(address: String): String = {
    s"akka.tcp://flink@$address/user/$JOB_MANAGER_NAME"
  }

  def getLocalAkkaURL: String = {
    s"akka://flink/user/$JOB_MANAGER_NAME"
  }

  def getProfiler(jobManager: ActorRef)(implicit system: ActorSystem, timeout: FiniteDuration):
  ActorRef = {
    AkkaUtils.getChild(jobManager, PROFILER_NAME)
  }

  def getEventCollector(jobManager: ActorRef)(implicit system: ActorSystem, timeout:
  FiniteDuration): ActorRef = {
    AkkaUtils.getChild(jobManager, EVENT_COLLECTOR_NAME)
  }

  def getArchivist(jobManager: ActorRef)(implicit system: ActorSystem, timeout: FiniteDuration):
  ActorRef = {
    AkkaUtils.getChild(jobManager, ARCHIVE_NAME)
  }

  def getJobManager(address: InetSocketAddress)(implicit system: ActorSystem, timeout:
  FiniteDuration): ActorRef = {
    AkkaUtils.getReference(getRemoteAkkaURL(address.getHostName + ":" + address.getPort))
  }
}
