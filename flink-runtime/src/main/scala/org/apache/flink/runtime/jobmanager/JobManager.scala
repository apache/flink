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

import java.io.{FileNotFoundException, File}
import java.net.InetAddress

import akka.actor._
import akka.pattern.{ask, pipe}
import com.google.common.base.Preconditions
import org.apache.flink.configuration.{ConfigConstants, GlobalConfiguration, Configuration}
import org.apache.flink.core.io.InputSplitAssigner
import org.apache.flink.runtime.accumulators.AccumulatorEvent
import org.apache.flink.runtime.event.job.AbstractEvent
import org.apache.flink.runtime.io.network.ConnectionInfoLookupResponse
import org.apache.flink.runtime.messages.ExecutionGraphMessages.JobStatusChanged
import org.apache.flink.runtime.messages.JobResult
import org.apache.flink.runtime.messages.JobResult.{JobProgressResult, JobCancelResult, JobSubmissionResult}
import org.apache.flink.runtime.{JobException, ActorLogMessages}
import org.apache.flink.runtime.akka.AkkaUtils
import org.apache.flink.runtime.execution.librarycache.LibraryCacheManager
import org.apache.flink.runtime.executiongraph.{ExecutionJobVertex, ExecutionGraph}
import org.apache.flink.runtime.instance.{InstanceManager}
import org.apache.flink.runtime.jobgraph.{JobStatus, JobID}
import org.apache.flink.runtime.jobmanager.accumulators.AccumulatorManager
import org.apache.flink.runtime.jobmanager.scheduler.DefaultScheduler
import org.apache.flink.runtime.messages.EventCollectorMessages.{RequestJobEvents, RegisterJob, RegisterArchiveListener}
import org.apache.flink.runtime.messages.JobManagerMessages._
import org.apache.flink.runtime.messages.RegistrationMessages._
import org.apache.flink.runtime.messages.TaskManagerMessages.{NextInputSplit, Heartbeat}
import org.apache.flink.runtime.profiling.ProfilingUtils
import org.apache.flink.runtime.profiling.impl.JobManagerProfilerImpl
import org.apache.flink.util.StringUtils
import org.slf4j.LoggerFactory

import scala.collection.convert.{WrapAsScala}
import scala.concurrent.Future

class JobManager(archiveCount: Int, profiling: Boolean, recommendedPollingInterval: Int) extends Actor with
ActorLogMessages with ActorLogging with WrapAsScala {

  val profiler = if(profiling){
    new JobManagerProfilerImpl(InetAddress.getByName(self.path.address.host.getOrElse("localhost")))
  }else{
    null
  }

  // will be removed
  val archive = context.actorOf(Props(classOf[MemoryArchivist], archiveCount), "archive")
  val eventCollector = context.actorOf(Props(classOf[EventCollector], recommendedPollingInterval), "eventcollector")


  val accumulatorManager = new AccumulatorManager(Math.min(1, archiveCount))
  val instanceManager = new InstanceManager()
  val scheduler = new DefaultScheduler()
  val webserver = null

  val currentJobs = scala.collection.concurrent.TrieMap[JobID, ExecutionGraph]()

  eventCollector ! RegisterArchiveListener(archive)

  instanceManager.addInstanceListener(scheduler)

  override def postStop(): Unit = {
    instanceManager.shutdown()
    scheduler.shutdown()
  }

  override def receiveWithLogMessages: Receive = {
    case RegisterTaskManager(hardwareInformation, numberOfSlots) => {
      val taskManager = sender()
      val instanceID = instanceManager.registerTaskManager(taskManager, hardwareInformation, numberOfSlots)
      context.watch(taskManager);
      taskManager ! AcknowledgeRegistration(instanceID)
    }

    case RequestNumberRegisteredTaskManager => {
      sender() ! instanceManager.getNumberOfRegisteredTaskManagers
    }

    case RequestAvailableSlots => {
      sender() ! instanceManager.getTotalNumberOfSlots
    }

    case SubmitJob(jobGraph) => {
      var success = false

      try {
        if (jobGraph == null) {
          JobSubmissionResult(JobResult.ERROR, "Submitted job is null.")
        } else {

          log.info(s"Received job ${jobGraph.getJobID} (${jobGraph.getName}}).")

          val executionGraph = currentJobs.getOrElseUpdate(jobGraph.getJobID(), new ExecutionGraph(jobGraph.getJobID,
            jobGraph.getName, jobGraph.getJobConfiguration))

          val userCodeLoader = LibraryCacheManager.getClassLoader(jobGraph.getJobID)

          if (userCodeLoader == null) {
            throw new JobException("The user code class loader could not be initialized.")
          }

          val jarFilesForJob = LibraryCacheManager.getRequiredJarFiles(jobGraph.getJobID)

          jarFilesForJob foreach {
            executionGraph.addUserCodeJarFile(_)
          }

          log.debug(s"Running master initialization of job ${jobGraph.getJobID} (${jobGraph.getName}).")

          try {
            for (vertex <- jobGraph.getVertices) {
              val executableClass = vertex.getInvokableClassName
              if (executableClass == null || executableClass.length == 0) {
                throw new JobException(s"The vertex ${vertex.getID} (${vertex.getName}) has no invokable class.")
              }

              vertex.initializeOnMaster(userCodeLoader)
            }
          } catch {
            case e: FileNotFoundException =>
              log.error(s"File not found: ${e.getMessage}.")
              JobSubmissionResult(JobResult.ERROR, e.getMessage)
          }

          // topological sorting of the job vertices
          val sortedTopology = jobGraph.getVerticesSortedTopologicallyFromSources

          log.debug(s"Adding ${sortedTopology.size()} vertices from job graph ${jobGraph.getJobID} (${jobGraph.getName}).")

          executionGraph.attachJobGraph(sortedTopology)

          log.debug(s"Successfully created execution graph from job graph ${jobGraph.getJobID} (${jobGraph.getName}).")

          eventCollector ! RegisterJob(executionGraph, false, System.currentTimeMillis())

          executionGraph.registerJobStatusListener(self)

          log.info(s"Scheduling job ${jobGraph.getName}.")

          executionGraph.scheduleForExecution(scheduler)

          success = true
          JobSubmissionResult(JobResult.SUCCESS, null)
        }
      }catch{
        case t: Throwable =>
          log.error(t, "Job submission failed.")
          JobSubmissionResult(JobResult.ERROR, StringUtils.stringifyException(t))
      }finally{
        if(!success){
          this.currentJobs.remove(jobGraph.getJobID)

          try{
            LibraryCacheManager.unregister(jobGraph.getJobID)
          }catch{
            case e: IllegalStateException =>
            case t: Throwable =>
              log.error(t, "Error while de-registering job at library cache manager.")
          }
        }
      }
    }

    case CancelJob(jobID) => {
      log.info(s"Trying to cancel job with ID ${jobID}.")

      currentJobs.get(jobID) match {
        case Some(executionGraph) =>
          Future{
            executionGraph.cancel()
          }
          JobCancelResult(JobResult.SUCCESS, null)
        case None =>
          log.info(s"No job found with ID ${jobID}.")
          JobCancelResult(JobResult.ERROR, s"Cannot find job with ID ${jobID}")
      }
    }

    case UpdateTaskExecutionState(taskExecutionState) => {
      Preconditions.checkNotNull(taskExecutionState)

      currentJobs.get(taskExecutionState.getJobID) match {
        case Some(executionGraph) => executionGraph.updateState(taskExecutionState)
        case None => log.error(s"Cannot find execution graph for ID ${taskExecutionState.getJobID} to change state to" +
          s" ${taskExecutionState.getExecutionState}.")
      }
    }

    case RequestNextInputSplit(jobID, vertexID) => {
      val nextInputSplit = currentJobs.get(jobID) match {
        case Some(executionGraph) => executionGraph.getJobVertex(vertexID) match {
          case vertex: ExecutionJobVertex => vertex.getSplitAssigner match{
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

    case JobStatusChanged(executionGraph, newJobStatus, optionalMessage) => {
      val jobID = executionGraph.getJobID

      log.info(s"Status of job ${jobID} (${executionGraph.getJobName}) changed to ${newJobStatus}${optionalMessage}.")

      if(Set(JobStatus.FINISHED, JobStatus.CANCELED, JobStatus.FAILED) contains newJobStatus){
        currentJobs.remove(jobID)

        try{
          LibraryCacheManager.unregister(jobID)
        }catch{
          case t: Throwable =>
            log.error(t, s"Could not properly unregister job ${jobID} form the library cache.")
        }
      }
    }

    case LookupConnectionInformation(jobID, sourceChannelID) =>{
      currentJobs.get(jobID) match {
        case Some(executionGraph) =>
          sender() ! ConnectionInformation(executionGraph.lookupConnectionInfoAndDeployReceivers(sender(),
            sourceChannelID))
        case None =>
          log.error(s"Cannot find execution graph for job ID ${jobID}.")
          sender() ! ConnectionInformation(ConnectionInfoLookupResponse.createReceiverNotFound())
      }
    }

    case ReportAccumulatorResult(accumulatorEvent) => {
      accumulatorManager.processIncomingAccumulators(accumulatorEvent.getJobID, accumulatorEvent.getAccumulators
        (LibraryCacheManager.getClassLoader(accumulatorEvent.getJobID)))
    }

    case RequestAccumulatorResult(jobID) => {
      sender() ! new AccumulatorEvent(jobID, accumulatorManager.getJobAccumulators(jobID))
    }

    case Heartbeat(instanceID) => {
      instanceManager.reportHeartBeat(instanceID)
    }
  }
}

object JobManager{
  val LOG = LoggerFactory.getLogger(classOf[JobManager])
  val FAILURE_RETURN_CODE = 1

  def main(args: Array[String]):Unit = {
    val (hostname, port, configuration) = initialize(args)

    val jobManagerSystem = startActorSystemAndActor(hostname, port, configuration)
    jobManagerSystem.awaitTermination()
  }

  def initialize(args: Array[String]):(String, Int, Configuration) = {
    val parser = new scopt.OptionParser[JobManagerCLIConfiguration]("jobmanager"){
      head("flink jobmanager")
      opt[String]("configDir") action { (x, c) =>
        c.copy(configDir = x)
      } text("Specify configuration directory.")
    }

    parser.parse(args, JobManagerCLIConfiguration()) map {
      config =>
        GlobalConfiguration.loadConfiguration(config.configDir)
        val configuration = GlobalConfiguration.getConfiguration()
        if(config.configDir != null && new File(config.configDir).isDirectory){
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

  def startActorSystemAndActor(hostname: String, port: Int, configuration: Configuration): ActorSystem = {
    val actorSystem = AkkaUtils.createActorSystem(hostname, port, configuration)
    startActor(actorSystem, configuration)
    actorSystem
  }

  def startActor(actorSystem: ActorSystem, configuration: Configuration): ActorRef = {
    val archiveCount = configuration.getInteger(ConfigConstants.JOB_MANAGER_WEB_ARCHIVE_COUNT,
      ConfigConstants.DEFAULT_JOB_MANAGER_WEB_ARCHIVE_COUNT)
    val profilingEnabled = configuration.getBoolean(ProfilingUtils.PROFILE_JOB_KEY, true)
    val recommendedPollingInterval = configuration.getInteger(ConfigConstants.JOBCLIENT_POLLING_INTERVAL_KEY,
      ConfigConstants.DEFAULT_JOBCLIENT_POLLING_INTERVAL)

    actorSystem.actorOf(Props(classOf[JobManager], archiveCount, profilingEnabled, recommendedPollingInterval), "jobmanager")
  }

  def getAkkaURL(address: String): String = {
    s"akka.tcp://flink@${address}/user/jobmanager"
  }
}
