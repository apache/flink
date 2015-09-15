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
import java.nio.ByteBuffer
import java.util.Collections

import akka.actor.ActorRef
import org.apache.flink.api.common.JobID
import org.apache.flink.configuration.ConfigConstants
import org.apache.flink.runtime.FlinkActor
import org.apache.flink.runtime.jobgraph.JobStatus
import org.apache.flink.runtime.jobmanager.JobManager
import org.apache.flink.runtime.messages.JobManagerMessages.{CurrentJobStatus, JobNotFound, RequestJobStatus}
import org.apache.flink.runtime.messages.Messages.Acknowledge
import org.apache.flink.runtime.yarn.FlinkYarnClusterStatus
import org.apache.flink.yarn.Messages._
import org.apache.flink.yarn.appMaster.YarnTaskManagerRunner
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.DataOutputBuffer
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.yarn.api.ApplicationConstants
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.client.api.{NMClient, AMRMClient}
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.exceptions.YarnException
import org.apache.hadoop.yarn.util.Records

import scala.collection.mutable
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.Try


trait ApplicationMasterActor extends FlinkActor {
  that: JobManager =>

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

  private def env = System.getenv()

  // indicates if this AM has been started in a detached mode.
  val detached = java.lang.Boolean.valueOf(env.get(FlinkYarnClient.ENV_DETACHED))
  var stopWhenJobFinished: JobID = null

  var rmClientOption: Option[AMRMClient[ContainerRequest]] = None
  var nmClientOption: Option[NMClient] = None
  var messageListener:Option[ActorRef] = None
  var containerLaunchContext: Option[ContainerLaunchContext] = None

  var runningContainers = 0 // number of currently running containers
  var failedContainers = 0 // failed container count
  var numTaskManager = 0 // the requested number of TMs
  var maxFailedContainers = 0
  var containersLaunched = 0
  var numPendingRequests = 0 // number of currently pending container allocation requests.

  var memoryPerTaskManager = 0

  // list of containers available for starting
  var allocatedContainersList: mutable.MutableList[Container] = new mutable.MutableList[Container]
  var runningContainersList: mutable.MutableList[Container] = new mutable.MutableList[Container]

  abstract override def handleMessage: Receive = {
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

    case RegisterClient(client) =>
      log.info(s"Register ${client.path} as client.")
      messageListener = Some(client)
      sender ! decorateMessage(Acknowledge)

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

    case StartYarnSession(conf, actorSystemPort, webServerPort) =>
      startYarnSession(conf, actorSystemPort, webServerPort)

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
          val response = rmClient.allocate(runningContainers.toFloat / numTaskManager)


          // ---------------------------- handle YARN responses -------------

          // get new containers from YARN
          for (container <- response.getAllocatedContainers.asScala) {
            log.info(s"Got new container for allocation: ${container.getId}")
            allocatedContainersList += container
            if(numPendingRequests > 0) {
              numPendingRequests -= 1
            }
          }

          // get failed containers (returned containers are also completed, so we have to
          // distinguish if it was running before).
          for (status <- response.getCompletedContainersStatuses.asScala) {
            log.info(s"Container ${status.getContainerId} is completed " +
              s"with diagnostics: ${status.getDiagnostics}")
            // remove failed container from running containers
            runningContainersList = runningContainersList.filter(runningContainer => {
              val wasRunningContainer = runningContainer.getId.equals(status.getContainerId)
              if(wasRunningContainer) {
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
              // return
              !wasRunningContainer
            })
          }
          // return containers if the RM wants them and we haven't allocated them yet.
          val preemptionMessage = response.getPreemptionMessage
          if(preemptionMessage != null) {
            log.info(s"Received preemtion message from YARN $preemptionMessage.")
            val contract = preemptionMessage.getContract
            if(contract != null) {
              tryToReturnContainers(contract.getContainers.asScala)
            }
            val strictContract = preemptionMessage.getStrictContract
            if(strictContract != null) {
              tryToReturnContainers(strictContract.getContainers.asScala)
            }
          }

          // ---------------------------- decide if we need to do anything ---------

          // check if we want to start some of our allocated containers.
          if(runningContainers < numTaskManager) {
            var missingContainers = numTaskManager - runningContainers
            log.info(s"The user requested $numTaskManager containers, $runningContainers " +
              s"running. $missingContainers containers missing")

            // not enough containers running
            if(allocatedContainersList.size > 0) {
              log.info(s"${allocatedContainersList.size} containers already allocated by YARN. " +
                "Starting...")
              // we have some containers allocated to us --> start them
              allocatedContainersList = allocatedContainersList.dropWhile(container => {
                if (missingContainers <= 0) {
                  require(missingContainers == 0, "The variable can not be negative. Illegal state")
                  false
                } else {
                  // start the container
                  nmClientOption match {
                    case Some(nmClient) =>
                      containerLaunchContext match {
                        case Some(ctx) => {
                          try {
                            nmClient.startContainer(container, ctx)
                            runningContainers += 1
                            missingContainers -= 1
                            val message = s"Launching container $containersLaunched " +
                              s"(${container.getId} on host ${container.getNodeId.getHost})."
                            log.info(message)
                            containersLaunched += 1
                            runningContainersList += container
                            messageListener foreach {
                              _ ! decorateMessage(YarnMessage(message))
                            }
                          } catch {
                            case e: YarnException =>
                              log.error("Exception while starting YARN container", e)
                          }
                        }
                        case None =>
                          log.error("The ContainerLaunchContext was not set.")
                          self ! decorateMessage(
                            StopYarnSession(
                              FinalApplicationStatus.FAILED,
                              "Fatal error in AM: The ContainerLaunchContext was not set.")
                          )
                      }
                    case None =>
                      log.error("The NMClient was not set.")
                      self ! decorateMessage(
                        StopYarnSession(
                          FinalApplicationStatus.FAILED,
                          "Fatal error in AM: The NMClient was not set.")
                      )
                  }
                  // dropping condition
                  true
                }
              })
            }
            // if there are still containers missing, request them from YARN
            val toAllocateFromYarn = Math.max(missingContainers - numPendingRequests, 0)
            if(toAllocateFromYarn > 0) {
              val reallocate = flinkConfiguration
                .getBoolean(ConfigConstants.YARN_REALLOCATE_FAILED_CONTAINERS, true)
              log.info(s"There are $missingContainers containers missing." +
                s" $numPendingRequests are already requested. " +
                s"Requesting $toAllocateFromYarn additional container(s) from YARN. " +
                s"Reallocation of failed containers is enabled=$reallocate " +
                s"('${ConfigConstants.YARN_REALLOCATE_FAILED_CONTAINERS}')")
              // there are still containers missing. Request them from YARN
              if(reallocate) {
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

          if(runningContainers >= numTaskManager && allocatedContainersList.size > 0) {
            log.info(s"Flink has ${allocatedContainersList.size} allocated containers which " +
              s"are not needed right now. Returning them")
            for(container <- allocatedContainersList) {
              rmClient.releaseAssignedContainer(container.getId)
            }
            allocatedContainersList.clear()
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
          if (runningContainers < numTaskManager) {
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

  private def runningContainerIds(): mutable.MutableList[ContainerId] = {
    runningContainersList map { runningCont => runningCont.getId}
  }
  private def allocatedContainerIds(): mutable.MutableList[ContainerId] = {
    allocatedContainersList map { runningCont => runningCont.getId}
  }

  private def startYarnSession(
      conf: Configuration,
      actorSystemPort: Int,
      webServerPort: Int)
    : Unit = {

    Try {
      log.info("Start yarn session.")
      memoryPerTaskManager = env.get(FlinkYarnClient.ENV_TM_MEMORY).toInt

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

      numTaskManager = env.get(FlinkYarnClient.ENV_TM_COUNT).toInt
      maxFailedContainers = flinkConfiguration.
        getInteger(ConfigConstants.YARN_MAX_FAILED_CONTAINERS, numTaskManager)
      log.info(s"Requesting $numTaskManager TaskManagers. Tolerating $maxFailedContainers failed " +
        "TaskManagers")

      val remoteFlinkJarPath = env.get(FlinkYarnClient.FLINK_JAR_PATH)
      val fs = FileSystem.get(conf)
      val appId = env.get(FlinkYarnClient.ENV_APP_ID)
      val currDir = env.get(Environment.PWD.key())
      val clientHomeDir = env.get(FlinkYarnClient.ENV_CLIENT_HOME_DIR)
      val shipListString = env.get(FlinkYarnClient.ENV_CLIENT_SHIP_FILES)
      val yarnClientUsername = env.get(FlinkYarnClient.ENV_CLIENT_USERNAME)

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
      rm.registerApplicationMaster(applicationMasterHost, actorSystemPort, url)

      // Make container requests to ResourceManager
      for (i <- 0 until numTaskManager) {
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

      runningContainers = 0
      failedContainers = 0

      val hs = ApplicationMaster.hasStreamingMode(env)
      containerLaunchContext = Some(
        createContainerLaunchContext(
          memoryLimit,
          hasLogback,
          hasLog4j,
          yarnClientUsername,
          conf,
          taskManagerLocalResources,
          hs)
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

  private def tryToReturnContainers(returnRequest: mutable.Set[PreemptionContainer]): Unit = {
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
      taskManagerLocalResources: Map[String, LocalResource],
      streamingMode: Boolean)
    : ContainerLaunchContext = {
    log.info("Create container launch context.")
    val ctx = Records.newRecord(classOf[ContainerLaunchContext])

    val (heapLimit, offHeapLimit) = calculateMemoryLimits(memoryLimit, streamingMode)

    val javaOpts = flinkConfiguration.getString(ConfigConstants.FLINK_JVM_OPTIONS, "")
    val tmCommand = new StringBuilder(s"$$JAVA_HOME/bin/java -Xms${heapLimit}m " +
      s"-Xmx${heapLimit}m -XX:MaxDirectMemorySize=${offHeapLimit}m $javaOpts")

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

    tmCommand ++= s" ${classOf[YarnTaskManagerRunner].getName} --configDir . 1> " +
      s"${ApplicationConstants.LOG_DIR_EXPANSION_VAR}/taskmanager-stdout.log 2> " +
      s"${ApplicationConstants.LOG_DIR_EXPANSION_VAR}/taskmanager-stderr.log"

    tmCommand ++= " --streamingMode"
    if(streamingMode) {
      tmCommand ++= " streaming"
    } else {
      tmCommand ++= " batch"
    }

    ctx.setCommands(Collections.singletonList(tmCommand.toString()))

    log.info(s"Starting TM with command=${tmCommand.toString()}")

    ctx.setLocalResources(taskManagerLocalResources.asJava)

    // Setup classpath for container ( = TaskManager )
    val containerEnv = new java.util.HashMap[String, String]()
    Utils.setupEnv(yarnConf, containerEnv)
    containerEnv.put(FlinkYarnClient.ENV_CLIENT_USERNAME, yarnClientUsername)
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
   * Calculate the correct JVM heap and off-heap memory limits.
   * @param memoryLimit The maximum memory in megabytes.
   * @param streamingMode True if this is a streaming cluster.
   * @return A Tuple2 containing the heap and the offHeap limit in megabytes.
   */
  private def calculateMemoryLimits(memoryLimit: Long, streamingMode: Boolean): (Long, Long) = {

    // The new config entry overrides the old one
    val networkBufferSizeOld = flinkConfiguration.getLong(
      ConfigConstants.TASK_MANAGER_NETWORK_BUFFER_SIZE_KEY,
      ConfigConstants.DEFAULT_TASK_MANAGER_MEMORY_SEGMENT_SIZE)

    val networkBufferSize = flinkConfiguration.getLong(
      ConfigConstants.TASK_MANAGER_MEMORY_SEGMENT_SIZE_KEY,
      networkBufferSizeOld)

    val numNetworkBuffers = flinkConfiguration.getLong(
      ConfigConstants.TASK_MANAGER_NETWORK_NUM_BUFFERS_KEY,
      ConfigConstants.DEFAULT_TASK_MANAGER_NETWORK_NUM_BUFFERS)

    // direct memory for Netty's off-heap buffers
    val networkMemory = ((numNetworkBuffers * networkBufferSize) >> 20) + 1

    val useOffHeap = flinkConfiguration.getBoolean(
      ConfigConstants.TASK_MANAGER_MEMORY_OFF_HEAP_KEY, false)

    if (useOffHeap && !streamingMode){
      val fixedOffHeapSize = flinkConfiguration.getLong(
        ConfigConstants.TASK_MANAGER_MEMORY_SIZE_KEY, -1L)
      if (fixedOffHeapSize > 0) {
        (memoryLimit - fixedOffHeapSize - networkMemory, fixedOffHeapSize + networkMemory)
      } else {
        val fraction = flinkConfiguration.getFloat(
          ConfigConstants.TASK_MANAGER_MEMORY_FRACTION_KEY,
          ConfigConstants.DEFAULT_MEMORY_MANAGER_MEMORY_FRACTION)
        val offHeapSize = (fraction * memoryLimit).toLong
        (memoryLimit - offHeapSize - networkMemory, offHeapSize + networkMemory)
      }
    } else {
      (memoryLimit - networkMemory, networkMemory)
    }
  }
}
