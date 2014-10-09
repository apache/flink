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

import java.io.File
import java.net.{InetSocketAddress}

import akka.actor._
import akka.pattern.Patterns
import akka.pattern.{ask, pipe}
import com.google.common.base.Preconditions
import org.apache.flink.configuration.{ConfigConstants, GlobalConfiguration, Configuration}
import org.apache.flink.core.io.InputSplitAssigner
import org.apache.flink.runtime.blob.BlobServer
import org.apache.flink.runtime.executiongraph.{ExecutionJobVertex, ExecutionGraph}
import org.apache.flink.runtime.io.network.ConnectionInfoLookupResponse
import org.apache.flink.runtime.messages.ArchiveMessages.ArchiveExecutionGraph
import org.apache.flink.runtime.messages.ExecutionGraphMessages.JobStatusChanged
import org.apache.flink.runtime.{JobException, ActorLogMessages}
import org.apache.flink.runtime.akka.AkkaUtils
import org.apache.flink.runtime.execution.librarycache.BlobLibraryCacheManager
import org.apache.flink.runtime.instance.{InstanceManager}
import org.apache.flink.runtime.jobgraph.{JobStatus, JobID}
import org.apache.flink.runtime.jobmanager.accumulators.AccumulatorManager
import org.apache.flink.runtime.jobmanager.scheduler.{Scheduler => FlinkScheduler}
import org.apache.flink.runtime.messages.JobManagerMessages._
import org.apache.flink.runtime.messages.RegistrationMessages._
import org.apache.flink.runtime.messages.TaskManagerMessages.{NextInputSplit, Heartbeat}
import org.apache.flink.runtime.profiling.ProfilingUtils
import org.slf4j.LoggerFactory

import scala.collection.convert.WrapAsScala
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._

class JobManager(val configuration: Configuration) extends
Actor with ActorLogMessages with ActorLogging with WrapAsScala {
  import context._
  import AkkaUtils.FUTURE_TIMEOUT

  val (archiveCount, profiling, cleanupInterval) = JobManager.parseConfiguration(configuration)

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

  log.info(s"Started job manager. Waiting for incoming messages.")

  override def postStop(): Unit = {
    log.info(s"Stopping job manager ${self.path}.")
    instanceManager.shutdown()
    scheduler.shutdown()
    libraryCacheManager.shutdown()
  }

  override def receiveWithLogMessages: Receive = {
    case RegisterTaskManager(connectionInfo, hardwareInformation, numberOfSlots) => {
      val taskManager = sender()
      val instanceID = instanceManager.registerTaskManager(taskManager, connectionInfo,
        hardwareInformation, numberOfSlots)

      // to be notified when the taskManager is no longer reachable
      context.watch(taskManager);

      taskManager ! AcknowledgeRegistration(instanceID, libraryCacheManager.getBlobServerPort)
    }

    case RequestNumberRegisteredTaskManager => {
      sender() ! instanceManager.getNumberOfRegisteredTaskManagers
    }

    case RequestTotalNumberOfSlots => {
      sender() ! instanceManager.getTotalNumberOfSlots
    }

    case SubmitJob(jobGraph, listenToEvents, detach) => {
      try {
        if (jobGraph == null) {
          sender() ! akka.actor.Status.Failure(new IllegalArgumentException("JobGraph must not be" +
            " null."))
        } else {

          log.info(s"Received job ${jobGraph.getJobID} (${jobGraph.getName}}).")

          // Create the user code class loader
          libraryCacheManager.register(jobGraph.getJobID, jobGraph.getUserJarBlobKeys)

          val (executionGraph, jobInfo) = currentJobs.getOrElseUpdate(jobGraph.getJobID(),
            (new ExecutionGraph(jobGraph.getJobID, jobGraph.getName,
              jobGraph.getJobConfiguration, jobGraph.getUserJarBlobKeys), JobInfo(sender(),
              System.currentTimeMillis())))

          val userCodeLoader = libraryCacheManager.getClassLoader(jobGraph.getJobID)

          if (userCodeLoader == null) {
            throw new JobException("The user code class loader could not be initialized.")
          }

          log.debug(s"Running master initialization of job ${jobGraph.getJobID} (${jobGraph
            .getName}).")

          for (vertex <- jobGraph.getVertices) {
            val executableClass = vertex.getInvokableClassName
            if (executableClass == null || executableClass.length == 0) {
              throw new JobException(s"The vertex ${vertex.getID} (${vertex.getName}) has no " +
                s"invokable class.")
            }

            vertex.initializeOnMaster(userCodeLoader)
          }

          // topological sorting of the job vertices
          val sortedTopology = jobGraph.getVerticesSortedTopologicallyFromSources

          log.debug(s"Adding ${sortedTopology.size()} vertices from job graph ${jobGraph
            .getJobID} (${jobGraph.getName}).")

          executionGraph.attachJobGraph(sortedTopology)

          log.debug(s"Successfully created execution graph from job graph ${jobGraph.getJobID} " +
            s"(${jobGraph.getName}).")

          executionGraph.setQueuedSchedulingAllowed(jobGraph.getAllowQueuedScheduling)

          // get notified about job status changes
          executionGraph.registerJobStatusListener(self)

          if(listenToEvents){
            // the sender will be notified about state changes
            executionGraph.registerExecutionListener(sender())
            executionGraph.registerJobStatusListener(sender())
          }

          jobInfo.detach = detach

          log.info(s"Scheduling job ${jobGraph.getName}.")

          executionGraph.scheduleForExecution(scheduler)

          sender() ! SubmissionSuccess(jobGraph.getJobID)
        }
      } catch {
        case t: Throwable =>
          log.error(t, "Job submission failed.")

          currentJobs.get(jobGraph.getJobID) match {
            case Some((executionGraph, jobInfo)) =>
              executionGraph.fail(t)

              // don't send the client the final job status because we already send him
              // SubmissionFailure
              jobInfo.detach = true

              val status = Patterns.ask(self, RequestFinalJobStatus(jobGraph.getJobID), 10 second)
              status.onFailure{
                case _: Throwable => self ! JobStatusChanged(executionGraph.getJobID,
                  JobStatus.FAILED, System.currentTimeMillis(),
                  s"Cleanup job ${jobGraph.getJobID}.")
              }
            case None =>
              libraryCacheManager.unregister(jobGraph.getJobID)
              currentJobs.remove(jobGraph.getJobID)

          }

          sender() ! SubmissionFailure(jobGraph.getJobID, t)
      }
    }

    case CancelJob(jobID) => {
      log.info(s"Trying to cancel job with ID ${jobID}.")

      currentJobs.get(jobID) match {
        case Some((executionGraph, _)) =>
          Future {
            executionGraph.cancel()
          }
          sender() ! CancellationSuccess(jobID)
        case None =>
          log.info(s"No job found with ID ${jobID}.")
          sender() ! CancellationFailure(jobID, new IllegalArgumentException(s"No job found with " +
            s"ID ${jobID}."))
      }
    }

    case UpdateTaskExecutionState(taskExecutionState) => {
      if(taskExecutionState == null){
        sender() ! false
      }else {
        currentJobs.get(taskExecutionState.getJobID) match {
          case Some((executionGraph, _)) =>
            sender() ! executionGraph.updateState(taskExecutionState)
          case None => log.error(s"Cannot find execution graph for ID ${taskExecutionState
            .getJobID} to change state to ${taskExecutionState.getExecutionState}.")
            sender() ! false
        }
      }
    }

    case RequestNextInputSplit(jobID, vertexID) => {
      val nextInputSplit = currentJobs.get(jobID) match {
        case Some((executionGraph,_)) => executionGraph.getJobVertex(vertexID) match {
          case vertex: ExecutionJobVertex => vertex.getSplitAssigner match {
            case splitAssigner: InputSplitAssigner => splitAssigner.getNextInputSplit(null)
            case _ =>
              log.error(s"No InputSplitAssigner for vertex ID ${vertexID}.")
              null
          }
          case _ =>
            log.error(s"Cannot find execution vertex for vertex ID ${vertexID}.")
            null
        }
        case None =>
          log.error(s"Cannot find execution graph for job ID ${jobID}.")
          null
      }

      sender() ! NextInputSplit(nextInputSplit)
    }

    case JobStatusChanged(jobID, newJobStatus, timeStamp, optionalMessage) => {
      currentJobs.get(jobID) match {
        case Some((executionGraph, jobInfo)) => executionGraph.getJobName
          log.info(s"Status of job ${jobID} (${executionGraph.getJobName}) changed to " +
            s"${newJobStatus}${optionalMessage}.")

          if(newJobStatus.isTerminalState) {
            jobInfo.end = timeStamp

            if(!jobInfo.detach) {
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
    }

    case RequestFinalJobStatus(jobID) => {
      currentJobs.get(jobID) match {
        case Some(_) =>
          val listeners = finalJobStatusListener.getOrElse(jobID, Set())
          finalJobStatusListener += jobID -> (listeners + sender())
        case None =>
          archive ! RequestJobStatus(jobID)
      }
    }

    case LookupConnectionInformation(connectionInformation, jobID, sourceChannelID) => {
      currentJobs.get(jobID) match {
        case Some((executionGraph, _)) =>
          sender() ! ConnectionInformation(executionGraph.lookupConnectionInfoAndDeployReceivers
            (connectionInformation, sourceChannelID))
        case None =>
          log.error(s"Cannot find execution graph for job ID ${jobID}.")
          sender() ! ConnectionInformation(ConnectionInfoLookupResponse.createReceiverNotFound())
      }
    }

    case ReportAccumulatorResult(accumulatorEvent) => {
      accumulatorManager.processIncomingAccumulators(accumulatorEvent.getJobID,
        accumulatorEvent.getAccumulators
        (libraryCacheManager.getClassLoader(accumulatorEvent.getJobID)))
    }

    case RequestAccumulatorResults(jobID) => {
      import scala.collection.JavaConverters._
      sender() ! AccumulatorResultsFound(jobID, accumulatorManager.getJobAccumulatorResults
        (jobID).asScala.toMap)
    }

    case RequestJobStatus(jobID) => {
      currentJobs.get(jobID) match {
        case Some((executionGraph,_)) => sender() ! CurrentJobStatus(jobID, executionGraph.getState)
        case None => archive ? RequestJobStatus(jobID) pipeTo sender()
      }
    }

    case RequestRunningJobs => {
      val executionGraphs = currentJobs map {
        case (_, (eg, jobInfo)) => eg
      }

      sender() ! RunningJobs(executionGraphs)
    }

    case RequestJob(jobID) => {
      currentJobs.get(jobID) match {
        case Some((eg, _)) => sender() ! JobFound(jobID, eg)
        case None => archive ? RequestJob(jobID) pipeTo sender()
      }
    }

    case RequestBlobManagerPort => {
      sender() ! libraryCacheManager.getBlobServerPort
    }

    case RequestRegisteredTaskManagers => {
      import scala.collection.JavaConverters._
      sender() ! RegisteredTaskManagers(instanceManager.getAllRegisteredInstances.asScala)
    }

    case Heartbeat(instanceID) => {
      instanceManager.reportHeartBeat(instanceID)
    }

    case Terminated(taskManager) => {
      log.info(s"Task manager ${taskManager.path} terminated.")
      instanceManager.unregisterTaskManager(taskManager)
      context.unwatch(taskManager)
    }
  }

  private def removeJob(jobID: JobID): Unit = {
    currentJobs.remove(jobID) match {
      case Some((eg, _)) => archive ! ArchiveExecutionGraph(jobID, eg)
      case None =>
    }

    try {
      libraryCacheManager.unregister(jobID)
    } catch {
      case t: Throwable =>
        log.error(t, s"Could not properly unregister job ${jobID} form the library cache.")
    }
  }
}

object JobManager {
  val LOG = LoggerFactory.getLogger(classOf[JobManager])
  val FAILURE_RETURN_CODE = 1
  val JOB_MANAGER_NAME = "jobmanager"
  val EVENT_COLLECTOR_NAME = "eventcollector"
  val ARCHIVE_NAME = "archive"
  val PROFILER_NAME = "profiler"

  def main(args: Array[String]): Unit = {
    val (hostname, port, configuration) = initialize(args)

    val jobManagerSystem = AkkaUtils.createActorSystem(hostname, port, configuration)

    startActor(Props(new JobManager(configuration) with WithWebServer))(jobManagerSystem)
    jobManagerSystem.awaitTermination()
  }

  def initialize(args: Array[String]): (String, Int, Configuration) = {
    val parser = new scopt.OptionParser[JobManagerCLIConfiguration]("jobmanager") {
      head("flink jobmanager")
      opt[String]("configDir") action { (x, c) => c.copy(configDir = x) } text ("Specify " +
        "configuration directory.")
    }

    parser.parse(args, JobManagerCLIConfiguration()) map {
      config =>
        GlobalConfiguration.loadConfiguration(config.configDir)
        val configuration = GlobalConfiguration.getConfiguration()
        if (config.configDir != null && new File(config.configDir).isDirectory) {
          configuration.setString(ConfigConstants.FLINK_BASE_DIR_PATH_KEY, config.configDir + "/..")
        }

        val hostname = configuration.getString(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, null)
        val port = configuration.getInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY,
          ConfigConstants.DEFAULT_JOB_MANAGER_IPC_PORT)

        (hostname, port, configuration)
    } getOrElse {
      LOG.error("CLI Parsing failed. Usage: " + parser.usage)
      sys.exit(FAILURE_RETURN_CODE)
    }
  }

  def startActorSystemAndActor(hostname: String, port: Int, configuration: Configuration):
  (ActorSystem, ActorRef) = {
    implicit val actorSystem = AkkaUtils.createActorSystem(hostname, port, configuration)
    (actorSystem, startActor(configuration))
  }

  def parseConfiguration(configuration: Configuration): (Int, Boolean, Long) = {
    val archiveCount = configuration.getInteger(ConfigConstants.JOB_MANAGER_WEB_ARCHIVE_COUNT,
      ConfigConstants.DEFAULT_JOB_MANAGER_WEB_ARCHIVE_COUNT)
    val profilingEnabled = configuration.getBoolean(ProfilingUtils.PROFILE_JOB_KEY, true)

    val cleanupInterval = configuration.getLong(ConfigConstants
      .LIBRARY_CACHE_MANAGER_CLEANUP_INTERVAL,
      ConfigConstants.DEFAULT_LIBRARY_CACHE_MANAGER_CLEANUP_INTERVAL) * 1000

    (archiveCount, profilingEnabled, cleanupInterval)
  }

  def startActor(configuration: Configuration)(implicit actorSystem: ActorSystem): ActorRef = {
    startActor(Props(classOf[JobManager], configuration))
  }

  def startActor(props: Props)(implicit actorSystem: ActorSystem): ActorRef = {
    actorSystem.actorOf(props, JOB_MANAGER_NAME)
  }

  def getAkkaURL(address: String): String = {
    s"akka.tcp://flink@${address}/user/${JOB_MANAGER_NAME}"
  }

  def getProfiler(jobManager: ActorRef)(implicit system: ActorSystem): ActorRef = {
    AkkaUtils.getChild(jobManager, PROFILER_NAME)
  }

  def getEventCollector(jobManager: ActorRef)(implicit system: ActorSystem): ActorRef = {
    AkkaUtils.getChild(jobManager, EVENT_COLLECTOR_NAME)
  }

  def getArchivist(jobManager: ActorRef)(implicit system: ActorSystem): ActorRef = {
    AkkaUtils.getChild(jobManager, ARCHIVE_NAME)
  }

  def getJobManager(address: InetSocketAddress)(implicit system: ActorSystem): ActorRef = {
    AkkaUtils.getReference(getAkkaURL(address.getHostName + ":" + address.getPort))
  }
}
