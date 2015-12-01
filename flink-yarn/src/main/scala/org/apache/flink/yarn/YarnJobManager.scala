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

package org.apache.flink.yarn

import java.io.File
import java.lang.reflect.Method
import java.nio.ByteBuffer
import java.util.Collections
import java.util.{List => JavaList}

import akka.actor.ActorRef
import grizzled.slf4j.Logger
import org.apache.flink.api.common.JobID
import org.apache.flink.configuration.{Configuration => FlinkConfiguration, ConfigConstants}
import org.apache.flink.runtime.akka.AkkaUtils
import org.apache.flink.runtime.checkpoint.CheckpointRecoveryFactory
import org.apache.flink.runtime.jobgraph.JobStatus
import org.apache.flink.runtime.jobmanager.{SubmittedJobGraphStore, JobManager}
import org.apache.flink.runtime.leaderelection.LeaderElectionService
import org.apache.flink.runtime.messages.JobManagerMessages.{RequestJobStatus, CurrentJobStatus, JobNotFound}
import org.apache.flink.runtime.messages.Messages.Acknowledge
import org.apache.flink.runtime.yarn.FlinkYarnClusterStatus
import org.apache.flink.runtime.execution.librarycache.BlobLibraryCacheManager
import org.apache.flink.runtime.instance.InstanceManager
import org.apache.flink.runtime.jobmanager.scheduler.{Scheduler => FlinkScheduler}
import org.apache.flink.yarn.YarnMessages._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.DataOutputBuffer
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.yarn.api.ApplicationConstants
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.client.api.{NMClient, AMRMClient}
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.exceptions.YarnException
import org.apache.hadoop.yarn.util.Records

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.Try

/** JobManager actor for execution on Yarn. It enriches the [[JobManager]] with additional messages
  * to start/administer/stop the Yarn session.
  *
  * @param flinkConfiguration Configuration object for the actor
  * @param executionContext Execution context which is used to execute concurrent tasks in the
  *                         [[org.apache.flink.runtime.executiongraph.ExecutionGraph]]
  * @param instanceManager Instance manager to manage the registered
  *                        [[org.apache.flink.runtime.taskmanager.TaskManager]]
  * @param scheduler Scheduler to schedule Flink jobs
  * @param libraryCacheManager Manager to manage uploaded jar files
  * @param archive Archive for finished Flink jobs
  * @param defaultExecutionRetries Number of default execution retries
  * @param delayBetweenRetries Delay between retries
  * @param timeout Timeout for futures
  * @param leaderElectionService LeaderElectionService to participate in the leader election
  */
class YarnJobManager(
    flinkConfiguration: FlinkConfiguration,
    executionContext: ExecutionContext,
    instanceManager: InstanceManager,
    scheduler: FlinkScheduler,
    libraryCacheManager: BlobLibraryCacheManager,
    archive: ActorRef,
    defaultExecutionRetries: Int,
    delayBetweenRetries: Long,
    timeout: FiniteDuration,
    leaderElectionService: LeaderElectionService,
    submittedJobGraphs : SubmittedJobGraphStore,
    checkpointRecoveryFactory : CheckpointRecoveryFactory)
  extends JobManager(
    flinkConfiguration,
    executionContext,
    instanceManager,
    scheduler,
    libraryCacheManager,
    archive,
    defaultExecutionRetries,
    delayBetweenRetries,
    timeout,
    leaderElectionService,
    submittedJobGraphs,
    checkpointRecoveryFactory) {

  import context._
  import scala.collection.JavaConverters._

  val FAST_YARN_HEARTBEAT_DELAY: FiniteDuration = 500 milliseconds
  val DEFAULT_YARN_HEARTBEAT_DELAY: FiniteDuration = 5 seconds
  val YARN_HEARTBEAT_DELAY: FiniteDuration =
    if(flinkConfiguration.getString(ConfigConstants.YARN_HEARTBEAT_DELAY_SECONDS, null) == null) {
      DEFAULT_YARN_HEARTBEAT_DELAY
    } else {
      FiniteDuration(
        flinkConfiguration.getInteger(ConfigConstants.YARN_HEARTBEAT_DELAY_SECONDS, 5), SECONDS)
    }

  val taskManagerRunnerClass: Class[_] = classOf[YarnTaskManagerRunner]

  private def env = System.getenv()

  // indicates if this AM has been started in a detached mode.
  val detached = java.lang.Boolean.valueOf(env.get(FlinkYarnClientBase.ENV_DETACHED))
  var stopWhenJobFinished: JobID = null

  var rmClientOption: Option[AMRMClient[ContainerRequest]] = None
  var nmClientOption: Option[NMClient] = None
  var messageListener:Option[ActorRef] = None
  var containerLaunchContext: Option[ContainerLaunchContext] = None

  var runningContainers = 0 // number of currently running containers
  var failedContainers = 0 // failed container count
  var numTaskManagers = 0 // the requested number of TMs
  var maxFailedContainers = 0
  var numPendingRequests = 0 // number of currently pending container allocation requests.

  var memoryPerTaskManager = 0

  // list of containers available for starting
  var allocatedContainersList = List[Container]()
  var runningContainersList = List[Container]()

  override def handleMessage: Receive = {
    handleYarnMessage orElse super.handleMessage
  }

  def handleYarnMessage: Receive = {
    case StopYarnSession(status, diag) =>
      log.info(s"Stopping YARN JobManager with status $status and diagnostic $diag.")

      instanceManager.getAllRegisteredInstances.asScala foreach {
        instance =>
          instance.getActorGateway.tell(StopYarnSession(status, diag))
      }

      rmClientOption foreach {
        rmClient =>
          Try(rmClient.unregisterApplicationMaster(status, diag, "")).recover{
            case t: Throwable => log.error("Could not unregister the application master.", t)
          }

          Try(rmClient.close()).recover{
            case t:Throwable => log.error("Could not close the AMRMClient.", t)
          }
      }

      rmClientOption = None

      nmClientOption foreach {
        nmClient =>
        Try(nmClient.close()).recover{
          case t: Throwable => log.error("Could not close the NMClient.", t)
        }
      }

      nmClientOption = None
      messageListener foreach {
          _ ! decorateMessage(JobManagerStopped)
      }

      context.system.shutdown()

    case RegisterApplicationClient =>
      val client = sender()

      log.info(s"Register ${client.path} as client.")
      messageListener = Some(client)
      sender ! decorateMessage(AcknowledgeApplicationClientRegistration)

    case UnregisterClient =>
      messageListener = None

    case msg: StopAMAfterJob =>
      val jobId = msg.jobId
      log.info(s"ApplicatonMaster will shut down YARN session when job $jobId has finished.")
      stopWhenJobFinished = jobId
      sender() ! decorateMessage(Acknowledge)


    case PollYarnClusterStatus =>
      sender() ! decorateMessage(
        new FlinkYarnClusterStatus(
          instanceManager.getNumberOfRegisteredTaskManagers,
          instanceManager.getTotalNumberOfSlots)
      )

    case StartYarnSession(hadoopConfig, webServerPort) =>
      startYarnSession(hadoopConfig, webServerPort)

    case jnf: JobNotFound =>
      log.warn(s"Job with ID ${jnf.jobID} not found in JobManager")
      if(stopWhenJobFinished == null) {
        log.warn("The ApplicationMaster didn't expect to receive this message")
      }

    case jobStatus: CurrentJobStatus =>
      if(stopWhenJobFinished == null) {
        log.warn(s"Received job status $jobStatus which wasn't requested.")
      } else {
        if(stopWhenJobFinished != jobStatus.jobID) {
          log.warn(s"Received job status for job ${jobStatus.jobID} but expected status for " +
            s"job $stopWhenJobFinished")
        } else {
          if(jobStatus.status.isTerminalState) {
            log.info(s"Job with ID ${jobStatus.jobID} is in terminal state ${jobStatus.status}. " +
              s"Shutting down YARN session")
            if (jobStatus.status == JobStatus.FINISHED) {
              self ! decorateMessage(
                StopYarnSession(
                  FinalApplicationStatus.SUCCEEDED,
                  s"The monitored job with ID ${jobStatus.jobID} has finished.")
              )
            } else {
              self ! decorateMessage(
                StopYarnSession(
                  FinalApplicationStatus.FAILED,
                  s"The monitored job with ID ${jobStatus.jobID} has failed to complete.")
              )
            }
          } else {
            log.debug(s"Monitored job with ID ${jobStatus.jobID} is in state ${jobStatus.status}")
          }
        }
      }

    case HeartbeatWithYarn =>
      // piggyback on the YARN heartbeat to check if the job has finished
      if(stopWhenJobFinished != null) {
        self ! decorateMessage(RequestJobStatus(stopWhenJobFinished))
      }
      rmClientOption match {
        case Some(rmClient) =>
          log.debug("Send heartbeat to YARN")
          val response = rmClient.allocate(runningContainers.toFloat / numTaskManagers)

          // ---------------------------- handle YARN responses -------------

          val newlyAllocatedContainers = response.getAllocatedContainers.asScala

          newlyAllocatedContainers.foreach {
            container => log.info(s"Got new container for allocation: ${container.getId}")
          }

          allocatedContainersList ++= newlyAllocatedContainers
          numPendingRequests = math.max(0, numPendingRequests - newlyAllocatedContainers.length)

          val completedContainerStatuses = response.getCompletedContainersStatuses.asScala
          val idStatusMap = completedContainerStatuses
            .map(status => (status.getContainerId, status)).toMap

          completedContainerStatuses.foreach {
            status => log.info(s"Container ${status.getContainerId} is completed " +
              s"with diagnostics: ${status.getDiagnostics}")
          }

          // get failed containers (returned containers are also completed, so we have to
          // distinguish if it was running before).
          val (completedContainers, remainingRunningContainers) = runningContainersList
            .partition(idStatusMap contains _.getId)

          completedContainers.foreach {
            container =>
              val status = idStatusMap(container.getId)
              failedContainers += 1
              runningContainers -= 1
              log.info(s"Container ${status.getContainerId} was a running container. " +
                s"Total failed containers $failedContainers.")
              val detail = status.getExitStatus match {
                case -103 => "Vmem limit exceeded";
                case -104 => "Pmem limit exceeded";
                case _ => ""
              }
              messageListener foreach {
                _ ! decorateMessage(
                  YarnMessage(s"Diagnostics for containerID=${status.getContainerId} in " +
                    s"state=${status.getState}.\n${status.getDiagnostics} $detail")
                )
              }
          }

          runningContainersList = remainingRunningContainers

          // return containers if the RM wants them and we haven't allocated them yet.
          val preemptionMessage = response.getPreemptionMessage
          if(preemptionMessage != null) {
            log.info(s"Received preemtion message from YARN $preemptionMessage.")
            val contract = preemptionMessage.getContract
            if(contract != null) {
              tryToReturnContainers(contract.getContainers.asScala.toSet)
            }
            val strictContract = preemptionMessage.getStrictContract
            if(strictContract != null) {
              tryToReturnContainers(strictContract.getContainers.asScala.toSet)
            }
          }

          // ---------------------------- decide if we need to do anything ---------

          // check if we want to start some of our allocated containers.
          if(runningContainers < numTaskManagers) {
            val missingContainers = numTaskManagers - runningContainers
            log.info(s"The user requested $numTaskManagers containers, $runningContainers " +
              s"running. $missingContainers containers missing")

            val numStartedContainers = startTMsInAllocatedContainers(missingContainers)

            // if there are still containers missing, request them from YARN
            val toAllocateFromYarn = Math.max(
              missingContainers - numStartedContainers - numPendingRequests,
              0)

            if (toAllocateFromYarn > 0) {
              val reallocate = flinkConfiguration
                .getBoolean(ConfigConstants.YARN_REALLOCATE_FAILED_CONTAINERS, true)
              log.info(s"There are $missingContainers containers missing." +
                s" $numPendingRequests are already requested. " +
                s"Requesting $toAllocateFromYarn additional container(s) from YARN. " +
                s"Reallocation of failed containers is enabled=$reallocate " +
                s"('${ConfigConstants.YARN_REALLOCATE_FAILED_CONTAINERS}')")
              // there are still containers missing. Request them from YARN
              if (reallocate) {
                for(i <- 1 to toAllocateFromYarn) {
                  val containerRequest = getContainerRequest(memoryPerTaskManager)
                  rmClient.addContainerRequest(containerRequest)
                  numPendingRequests += 1
                  log.info("Requested additional container from YARN. Pending requests " +
                    s"$numPendingRequests.")
                }
              }
            }
          }

          if(runningContainers >= numTaskManagers && allocatedContainersList.nonEmpty) {
            log.info(s"Flink has ${allocatedContainersList.size} allocated containers which " +
              s"are not needed right now. Returning them")
            for(container <- allocatedContainersList) {
              rmClient.releaseAssignedContainer(container.getId)
            }
            allocatedContainersList = List()
          }

          // maxFailedContainers == -1 is infinite number of retries.
          if(maxFailedContainers != -1 && failedContainers >= maxFailedContainers) {
            val msg = s"Stopping YARN session because the number of failed " +
              s"containers ($failedContainers) exceeded the maximum failed container " +
              s"count ($maxFailedContainers). This number is controlled by " +
              s"the '${ConfigConstants.YARN_MAX_FAILED_CONTAINERS}' configuration " +
              s"setting. By default its the number of requested containers"
            log.error(msg)
            self ! decorateMessage(StopYarnSession(FinalApplicationStatus.FAILED, msg))
          }

          // schedule next heartbeat:
          if (runningContainers < numTaskManagers) {
            // we don't have the requested number of containers. Do fast polling
            context.system.scheduler.scheduleOnce(
              FAST_YARN_HEARTBEAT_DELAY,
              self,
              decorateMessage(HeartbeatWithYarn))
          } else {
            // everything is good, slow down polling
            context.system.scheduler.scheduleOnce(
              YARN_HEARTBEAT_DELAY,
              self,
              decorateMessage(HeartbeatWithYarn))
          }
        case None =>
          log.error("The AMRMClient was not set.")
          self ! decorateMessage(
            StopYarnSession(
              FinalApplicationStatus.FAILED,
              "Fatal error in AM: AMRMClient was not set")
          )
      }
      log.debug(s"Processed Heartbeat with RMClient. Running containers $runningContainers, " +
        s"failed containers $failedContainers, " +
        s"allocated containers ${allocatedContainersList.size}.")
  }

  /** Starts min(numTMsToStart, allocatedContainersList.size) TaskManager in the available
    * allocated containers. The number of successfully started TaskManagers is returned.
    *
    * @param numTMsToStart Number of TaskManagers to start if enough allocated containers are
    *                      available. If not, then all allocated containers are used
    * @return Number of successfully started TaskManagers
    */
  private def startTMsInAllocatedContainers(numTMsToStart: Int): Int = {
    // not enough containers running
    if (allocatedContainersList.nonEmpty) {
      log.info(s"${allocatedContainersList.size} containers already allocated by YARN. " +
        "Starting...")

      nmClientOption match {
        case Some(nmClient) =>
          containerLaunchContext match {
            case Some(ctx) =>
              val (containersToBeStarted, remainingContainers) = allocatedContainersList
                .splitAt(numTMsToStart)

              val startedContainers = containersToBeStarted.flatMap {
                container =>
                  try {
                    nmClient.startContainer(container, ctx)
                    val message = s"Launching container (${container.getId} on host " +
                      s"${container.getNodeId.getHost})."
                    log.info(message)

                    messageListener foreach {
                      _ ! decorateMessage(YarnMessage(message))
                    }

                    Some(container)
                  } catch {
                    case e: YarnException =>
                      log.error(s"Exception while starting YARN " +
                        s"container ${container.getId} on " +
                        s"host ${container.getNodeId.getHost}", e)
                      None
                  }
              }

              runningContainers += startedContainers.length
              runningContainersList :::= startedContainers

              allocatedContainersList = remainingContainers

              startedContainers.length
            case None =>
              log.error("The ContainerLaunchContext was not set.")
              self ! decorateMessage(
                StopYarnSession(
                  FinalApplicationStatus.FAILED,
                  "Fatal error in AM: The ContainerLaunchContext was not set."))
              0
          }
        case None =>
          log.error("The NMClient was not set.")
          self ! decorateMessage(
            StopYarnSession(
              FinalApplicationStatus.FAILED,
              "Fatal error in AM: The NMClient was not set."))
          0
      }
    } else {
      0
    }
  }

  private def runningContainerIds(): List[ContainerId] = {
    runningContainersList map { runningCont => runningCont.getId}
  }
  private def allocatedContainerIds(): List[ContainerId] = {
    allocatedContainersList map { runningCont => runningCont.getId}
  }

  /** Starts the Yarn session by connecting to the RessourceManager and the NodeManager. After
    * a connection has been established, the number of missing containers is requested from Yarn.
    *
    * @param conf Hadoop configuration object
    * @param webServerPort The port on which the web server is listening
    */
  private def startYarnSession(conf: Configuration, webServerPort: Int): Unit = {
    Try {
      log.info("Start yarn session.")
      memoryPerTaskManager = env.get(FlinkYarnClientBase.ENV_TM_MEMORY).toInt

      val memoryLimit = Utils.calculateHeapSize(memoryPerTaskManager, flinkConfiguration)

      val applicationMasterHost = env.get(Environment.NM_HOST.key)
      require(applicationMasterHost != null, s"Application master (${Environment.NM_HOST} not set.")

      val yarnExpiryInterval: FiniteDuration = FiniteDuration(
        conf.getInt(
          YarnConfiguration.RM_AM_EXPIRY_INTERVAL_MS,
          YarnConfiguration.DEFAULT_RM_AM_EXPIRY_INTERVAL_MS),
        MILLISECONDS)

      if(YARN_HEARTBEAT_DELAY.gteq(yarnExpiryInterval)) {
        log.warn(s"The heartbeat interval of the Flink Application master " +
          s"($YARN_HEARTBEAT_DELAY) is greater than YARN's expiry interval " +
          s"($yarnExpiryInterval). The application is likely to be killed by YARN.")
      }

      numTaskManagers = env.get(FlinkYarnClientBase.ENV_TM_COUNT).toInt
      maxFailedContainers = flinkConfiguration.
        getInteger(ConfigConstants.YARN_MAX_FAILED_CONTAINERS, numTaskManagers)

      log.info(s"Yarn session with $numTaskManagers TaskManagers. Tolerating " +
        s"$maxFailedContainers failed TaskManagers")

      val remoteFlinkJarPath = env.get(FlinkYarnClientBase.FLINK_JAR_PATH)
      val fs = FileSystem.get(conf)
      val appId = env.get(FlinkYarnClientBase.ENV_APP_ID)
      val currDir = env.get(Environment.PWD.key())
      val clientHomeDir = env.get(FlinkYarnClientBase.ENV_CLIENT_HOME_DIR)
      val shipListString = env.get(FlinkYarnClientBase.ENV_CLIENT_SHIP_FILES)
      val yarnClientUsername = env.get(FlinkYarnClientBase.ENV_CLIENT_USERNAME)

      val rm = AMRMClient.createAMRMClient[ContainerRequest]()
      rm.init(conf)
      rm.start()

      rmClientOption = Some(rm)

      val nm = NMClient.createNMClient()
      nm.init(conf)
      nm.start()
      nm.cleanupRunningContainersOnStop(true)

      nmClientOption = Some(nm)

      // Register with ResourceManager
      val url = s"http://$applicationMasterHost:$webServerPort"
      log.info(s"Registering ApplicationMaster with tracking url $url.")

      val actorSystemPort = AkkaUtils.getAddress(system).port.getOrElse(-1)

      val response = rm.registerApplicationMaster(
        applicationMasterHost,
        actorSystemPort,
        url)

      val containersFromPreviousAttempts = getContainersFromPreviousAttempts(response)

      log.info(s"Retrieved ${containersFromPreviousAttempts.length} TaskManagers from previous " +
        s"attempts.")

      runningContainersList ++= containersFromPreviousAttempts
      runningContainers = runningContainersList.length

      // Make missing container requests to ResourceManager
      runningContainers until numTaskManagers foreach {
        i =>
          val containerRequest = getContainerRequest(memoryPerTaskManager)
          log.info(s"Requesting initial TaskManager container $i.")
          numPendingRequests += 1
          // these are initial requests. The reallocation setting doesn't affect this.
          rm.addContainerRequest(containerRequest)
      }

      val flinkJar = Records.newRecord(classOf[LocalResource])
      val flinkConf = Records.newRecord(classOf[LocalResource])

      // register Flink Jar with remote HDFS
      val remoteJarPath = new Path(remoteFlinkJarPath)
      Utils.registerLocalResource(fs, remoteJarPath, flinkJar)

      // register conf with local fs
      Utils.setupLocalResource(conf, fs, appId, new Path(s"file://$currDir/flink-conf-modified" +
        s".yaml"), flinkConf, new Path(clientHomeDir))
      log.info(s"Prepared local resource for modified yaml: $flinkConf")

      val hasLogback = new File(s"$currDir/logback.xml").exists()
      val hasLog4j = new File(s"$currDir/log4j.properties").exists()

      // prepare files to be shipped
      val resources = shipListString.split(",") flatMap {
        pathStr =>
          if (pathStr.isEmpty) {
            None
          } else {
            val resource = Records.newRecord(classOf[LocalResource])
            val path = new Path(pathStr)
            Utils.registerLocalResource(fs, path, resource)
            Some((path.getName, resource))
          }
      } toList

      val taskManagerLocalResources = ("flink.jar", flinkJar) ::("flink-conf.yaml",
        flinkConf) :: resources toMap

      failedContainers = 0

      containerLaunchContext = Some(
        createContainerLaunchContext(
          memoryLimit,
          hasLogback,
          hasLog4j,
          yarnClientUsername,
          conf,
          taskManagerLocalResources)
      )

      context.system.scheduler.scheduleOnce(
        FAST_YARN_HEARTBEAT_DELAY,
        self,
        decorateMessage(HeartbeatWithYarn))
    } recover {
      case t: Throwable =>
        log.error("Could not start yarn session.", t)
        self ! decorateMessage(
          StopYarnSession(
            FinalApplicationStatus.FAILED,
            s"ApplicationMaster failed while starting. Exception Message: ${t.getMessage}")
        )
    }
  }

  /** Returns all still living containers from previous application attempts.
    *
    * @param response RegisterApplicationMasterResponse which contains the information about
    *                 living containers.
    * @return Seq of living containers which could be retrieved
    */
  private def getContainersFromPreviousAttempts(
      response: RegisterApplicationMasterResponse)
    : Seq[Container] = {

    RegisterApplicationMasterResponseReflector.getContainersFromPreviousAttempts(response)
  }

  private def tryToReturnContainers(returnRequest: Set[PreemptionContainer]): Unit = {
    for(requestedBackContainers <- returnRequest) {
      allocatedContainersList = allocatedContainersList.dropWhile( container => {
        val result = requestedBackContainers.getId.equals(container.getId)
        if(result) {
          log.info(s"Returning container $container back to ResourceManager.")
        }
        result
      })
    }
  }

  private def getContainerRequest(memoryPerTaskManager: Int): ContainerRequest = {
    // Priority for worker containers - priorities are intra-application
    val priority = Records.newRecord(classOf[Priority])
    priority.setPriority(0)

    // Resource requirements for worker containers
    val capability = Records.newRecord(classOf[Resource])
    capability.setMemory(memoryPerTaskManager)
    capability.setVirtualCores(1) // hard-code that number (YARN is not accounting for CPUs)
    new ContainerRequest(capability, null, null, priority)
  }

  private def createContainerLaunchContext(
      memoryLimit: Int,
      hasLogback: Boolean,
      hasLog4j: Boolean,
      yarnClientUsername: String,
      yarnConf: Configuration,
      taskManagerLocalResources: Map[String, LocalResource]) : ContainerLaunchContext = {
    log.info("Create container launch context.")
    val ctx = Records.newRecord(classOf[ContainerLaunchContext])

    val heapLimit = calculateMemoryLimits(memoryLimit)

    val javaOpts = flinkConfiguration.getString(ConfigConstants.FLINK_JVM_OPTIONS, "")
    val tmCommand = new StringBuilder(s"$$JAVA_HOME/bin/java -Xms${heapLimit}m " +
      s"-Xmx${heapLimit}m -XX:MaxDirectMemorySize=${memoryLimit}m $javaOpts")

    if (hasLogback || hasLog4j) {
      tmCommand ++=
        s""" -Dlog.file="${ApplicationConstants.LOG_DIR_EXPANSION_VAR}/taskmanager.log""""
    }

    if (hasLogback) {
      tmCommand ++= s" -Dlogback.configurationFile=file:logback.xml"
    }

    if (hasLog4j) {
      tmCommand ++= s" -Dlog4j.configuration=file:log4j.properties"
    }

    tmCommand ++= s" ${taskManagerRunnerClass.getName} --configDir . 1> " +
      s"${ApplicationConstants.LOG_DIR_EXPANSION_VAR}/taskmanager.out 2> " +
      s"${ApplicationConstants.LOG_DIR_EXPANSION_VAR}/taskmanager.err"

    ctx.setCommands(Collections.singletonList(tmCommand.toString()))

    log.info(s"Starting TM with command=${tmCommand.toString()}")

    ctx.setLocalResources(taskManagerLocalResources.asJava)

    // Setup classpath and environment variables for container ( = TaskManager )
    val containerEnv = new java.util.HashMap[String, String]()
    // user defined TaskManager environment variables
    containerEnv.putAll(Utils.getEnvironmentVariables(ConfigConstants.YARN_TASK_MANAGER_ENV_PREFIX,
      flinkConfiguration))
    // YARN classpath
    Utils.setupEnv(yarnConf, containerEnv)
    containerEnv.put(FlinkYarnClientBase.ENV_CLIENT_USERNAME, yarnClientUsername)
    ctx.setEnvironment(containerEnv)

    val user = UserGroupInformation.getCurrentUser

    try {
      val credentials = user.getCredentials
      val dob = new DataOutputBuffer()
      credentials.writeTokenStorageToStream(dob)
      val securityTokens = ByteBuffer.wrap(dob.getData, 0, dob.getLength)
      ctx.setTokens(securityTokens)
    } catch {
      case t: Throwable =>
        log.error("Getting current user info failed when trying to launch the container", t)
    }

    ctx
  }

  /**
   * Calculate the correct JVM heap memory limit.
   * @param memoryLimit The maximum memory in megabytes.
   * @return A Tuple2 containing the heap and the offHeap limit in megabytes.
   */
  private def calculateMemoryLimits(memoryLimit: Long): Long = {
    val eagerAllocation = flinkConfiguration.getBoolean(
      ConfigConstants.TASK_MANAGER_MEMORY_PRE_ALLOCATE_KEY,
      ConfigConstants.DEFAULT_TASK_MANAGER_MEMORY_PRE_ALLOCATE);
    if (eagerAllocation) {
      log.info("Heap limits calculated with eager memory allocation.")
    } else {
      log.info("Heap limits calculated with lazy memory allocation.")
    }

    val useOffHeap = flinkConfiguration.getBoolean(
      ConfigConstants.TASK_MANAGER_MEMORY_OFF_HEAP_KEY, false)

    if (useOffHeap && eagerAllocation){
      val fixedOffHeapSize = flinkConfiguration.getLong(
        ConfigConstants.TASK_MANAGER_MEMORY_SIZE_KEY, -1L)

      if (fixedOffHeapSize > 0) {
        memoryLimit - fixedOffHeapSize
      } else {
        val fraction = flinkConfiguration.getFloat(
          ConfigConstants.TASK_MANAGER_MEMORY_FRACTION_KEY,
          ConfigConstants.DEFAULT_MEMORY_MANAGER_MEMORY_FRACTION)
        val offHeapSize = (fraction * memoryLimit).toLong
        memoryLimit - offHeapSize
      }

    } else {
      memoryLimit
    }
  }
}

/** Singleton object to reflect the getContainersFromPreviousAttempts method for
  * [[RegisterApplicationMasterResponse]].
  *
  * Only Hadoop versions (>= 2.4.0) support
  * [[RegisterApplicationMasterResponse#getContainersFromPreviousAttempts]]. Therefore, it's checked
  * at runtime whether the RegisterApplicationMasterResponse supports this method. If not, then an
  * empty Seq[Container] is returned upon calling getContainersFromPreviousAttempts.
  *
  */
private object RegisterApplicationMasterResponseReflector {
  val log = Logger(getClass)

  // Use reflection to find the method
  val methodOption: Option[Method] = Try {
    classOf[RegisterApplicationMasterResponse].getMethod("getContainersFromPreviousAttempts")
  }.toOption

  def getContainersFromPreviousAttempts(
      response: RegisterApplicationMasterResponse)
    : Seq[Container] = {
    import scala.collection.JavaConverters._

    // if the method was defined call it, if not, then return an empty List
    methodOption match {
      case Some(method) =>
        log.debug(s"Calling method ${method.getName} of ${response.getClass.getCanonicalName}.")
        method.invoke(response).asInstanceOf[JavaList[Container]].asScala
      case None =>
        log.debug(s"${response.getClass.getCanonicalName} does not support the method " +
          "getContainersFromPreviousAttempts. Returning empty list.")
        List()
    }
  }
}
