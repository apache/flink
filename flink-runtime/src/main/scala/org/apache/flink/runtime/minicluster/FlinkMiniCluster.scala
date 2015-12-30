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

package org.apache.flink.runtime.minicluster

import java.net.InetAddress
import java.util.UUID

import akka.pattern.Patterns.gracefulStop
import akka.pattern.ask
import akka.actor.{ActorRef, ActorSystem}

import com.typesafe.config.Config

import org.apache.flink.api.common.{JobID, JobExecutionResult, JobSubmissionResult}
import org.apache.flink.configuration.{ConfigConstants, Configuration}
import org.apache.flink.runtime.akka.AkkaUtils
import org.apache.flink.runtime.client.{JobExecutionException, JobClient}
import org.apache.flink.runtime.instance.{AkkaActorGateway, ActorGateway}
import org.apache.flink.runtime.jobgraph.JobGraph
import org.apache.flink.runtime.jobmanager.RecoveryMode
import org.apache.flink.runtime.leaderretrieval.{LeaderRetrievalService, LeaderRetrievalListener,
StandaloneLeaderRetrievalService}
import org.apache.flink.runtime.messages.TaskManagerMessages.NotifyWhenRegisteredAtAnyJobManager
import org.apache.flink.runtime.util.ZooKeeperUtils
import org.apache.flink.runtime.webmonitor.{WebMonitorUtils, WebMonitor}

import org.slf4j.LoggerFactory

import scala.concurrent.duration.FiniteDuration
import scala.concurrent._

/**
 * Abstract base class for Flink's mini cluster. The mini cluster starts a
 * [[org.apache.flink.runtime.jobmanager.JobManager]] and one or multiple
 * [[org.apache.flink.runtime.taskmanager.TaskManager]]. Depending on the settings, the different
 * actors can all be run in the same [[ActorSystem]] or each one in its own.
 *
 * @param userConfiguration Configuration object with the user provided configuration values
 * @param useSingleActorSystem true if all actors (JobManager and TaskManager) shall be run in the
 *                             same [[ActorSystem]], otherwise false
 */
abstract class FlinkMiniCluster(
    val userConfiguration: Configuration,
    val useSingleActorSystem: Boolean)
  extends LeaderRetrievalListener {

  protected val LOG = LoggerFactory.getLogger(classOf[FlinkMiniCluster])

  // --------------------------------------------------------------------------
  //                           Construction
  // --------------------------------------------------------------------------

  // NOTE: THIS MUST BE getByName("localhost"), which is 127.0.0.1 and
  // not getLocalHost(), which may be 127.0.1.1
  val hostname = userConfiguration.getString(
    ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY,
    InetAddress.getByName("localhost").getHostAddress())

  val configuration = generateConfiguration(userConfiguration)

  /** Future to the [[ActorGateway]] of the current leader */
  var leaderGateway: Promise[ActorGateway] = Promise()

  /** Future to the index of the current leader */
  var leaderIndex: Promise[Int] = Promise()

  /** Future lock */
  val futureLock = new Object()

  implicit val executionContext = ExecutionContext.global

  implicit val timeout = AkkaUtils.getTimeout(userConfiguration)

  val recoveryMode = RecoveryMode.fromConfig(configuration)

  val numJobManagers = getNumberOfJobManagers

  val numTaskManagers = configuration.getInteger(
    ConfigConstants.LOCAL_NUMBER_TASK_MANAGER,
    ConfigConstants.DEFAULT_LOCAL_NUMBER_TASK_MANAGER)

  var jobManagerActorSystems: Option[Seq[ActorSystem]] = None
  var jobManagerActors: Option[Seq[ActorRef]] = None
  var webMonitor: Option[WebMonitor] = None
  var taskManagerActorSystems: Option[Seq[ActorSystem]] = None
  var taskManagerActors: Option[Seq[ActorRef]] = None

  protected var leaderRetrievalService: Option[LeaderRetrievalService] = None
  
  private var isRunning = false

  // --------------------------------------------------------------------------
  //                           Abstract Methods
  // --------------------------------------------------------------------------

  def generateConfiguration(userConfiguration: Configuration): Configuration

  def startJobManager(index: Int, system: ActorSystem): ActorRef

  def startTaskManager(index: Int, system: ActorSystem): ActorRef

  // --------------------------------------------------------------------------
  //                           Getters/Setters
  // --------------------------------------------------------------------------

  def getNumberOfJobManagers: Int = {
    if(recoveryMode == RecoveryMode.STANDALONE) {
      1
    } else {
      configuration.getInteger(
        ConfigConstants.LOCAL_NUMBER_JOB_MANAGER,
        ConfigConstants.DEFAULT_LOCAL_NUMBER_JOB_MANAGER
      )
    }
  }

  def getJobManagersAsJava = {
    import collection.JavaConverters._
    jobManagerActors.getOrElse(Seq()).asJava
  }

  def getTaskManagers = {
    taskManagerActors.getOrElse(Seq())
  }

  def getTaskManagersAsJava = {
    import collection.JavaConverters._
    taskManagerActors.getOrElse(Seq()).asJava
  }

  def getLeaderGatewayFuture: Future[ActorGateway] = {
    leaderGateway.future
  }

  def getLeaderGateway(timeout: FiniteDuration): ActorGateway = {
    val jmFuture = getLeaderGatewayFuture

    Await.result(jmFuture, timeout)
  }

  def getLeaderIndexFuture: Future[Int] = {
    leaderIndex.future
  }

  def getLeaderIndex(timeout: FiniteDuration): Int = {
    val indexFuture = getLeaderIndexFuture

    Await.result(indexFuture, timeout)
  }

  def getJobManagerAkkaConfig(index: Int): Config = {
    if (useSingleActorSystem) {
      AkkaUtils.getAkkaConfig(configuration, None)
    }
    else {
      val port = configuration.getInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY,
        ConfigConstants.DEFAULT_JOB_MANAGER_IPC_PORT)

      val resolvedPort = if(port != 0) port + index else port

      AkkaUtils.getAkkaConfig(configuration, Some((hostname, resolvedPort)))
    }
  }

  def getTaskManagerAkkaConfig(index: Int): Config = {
    val port = configuration.getInteger(ConfigConstants.TASK_MANAGER_IPC_PORT_KEY,
      ConfigConstants.DEFAULT_TASK_MANAGER_IPC_PORT)

    val resolvedPort = if(port != 0) port + index else port

    AkkaUtils.getAkkaConfig(configuration, Some((hostname, resolvedPort)))
  }

  // --------------------------------------------------------------------------
  //                          Start/Stop Methods
  // --------------------------------------------------------------------------

  def startJobManagerActorSystem(index: Int): ActorSystem = {
    val config = getJobManagerAkkaConfig(index)
    AkkaUtils.createActorSystem(config)
  }

  def startTaskManagerActorSystem(index: Int): ActorSystem = {
    val config = getTaskManagerAkkaConfig(index)

    AkkaUtils.createActorSystem(config)
  }

  def startJobClientActorSystem(jobID: JobID): ActorSystem = {
    if (useSingleActorSystem) {
      jobManagerActorSystems match {
        case Some(jmActorSystems) => jmActorSystems(0)
        case None => throw new JobExecutionException(
          jobID,
          "The FlinkMiniCluster has not been started yet.")
      }
    } else {
      JobClient.startJobClientActorSystem(configuration)
    }
  }

  def start(): Unit = {
    start(true)
  }

  def start(waitForTaskManagerRegistration: Boolean): Unit = {
    LOG.info("Starting FlinkMiniCluster.")

    lazy val singleActorSystem = startJobManagerActorSystem(0)

    val (jmActorSystems, jmActors) =
      (for(i <- 0 until numJobManagers) yield {
        val actorSystem = if(useSingleActorSystem) {
          singleActorSystem
        } else {
          startJobManagerActorSystem(i)
        }
        (actorSystem, startJobManager(i, actorSystem))
      }).unzip

    jobManagerActorSystems = Some(jmActorSystems)
    jobManagerActors = Some(jmActors)

    val lrs = createLeaderRetrievalService()

    leaderRetrievalService = Some(lrs)
    lrs.start(this)

    val (tmActorSystems, tmActors) =
      (for(i <- 0 until numTaskManagers) yield {
        val actorSystem = if(useSingleActorSystem) {
          jmActorSystems(0)
        } else {
          startTaskManagerActorSystem(i)
        }

        (actorSystem, startTaskManager(i, actorSystem))
      }).unzip

    taskManagerActorSystems = Some(tmActorSystems)
    taskManagerActors = Some(tmActors)

    val jobManagerAkkaURL = AkkaUtils.getAkkaURL(jmActorSystems(0), jmActors(0))

    webMonitor = startWebServer(configuration, jmActorSystems(0), jobManagerAkkaURL)

    if(waitForTaskManagerRegistration) {
      waitForTaskManagersToBeRegistered()
    }

    isRunning = true
  }

  def startWebServer(
      config: Configuration,
      actorSystem: ActorSystem,
      jobManagerAkkaURL: String)
    : Option[WebMonitor] = {
    if(
      config.getBoolean(ConfigConstants.LOCAL_START_WEBSERVER, false) &&
        config.getInteger(ConfigConstants.JOB_MANAGER_WEB_PORT_KEY, 0) >= 0) {
      
      // TODO: Add support for HA: Make web server work independently from the JM
      val leaderRetrievalService = new StandaloneLeaderRetrievalService(jobManagerAkkaURL)

      LOG.info("Starting JobManger web frontend")
      // start the new web frontend. we need to load this dynamically
      // because it is not in the same project/dependencies
      val webServer = WebMonitorUtils.startWebRuntimeMonitor(
        config, leaderRetrievalService, actorSystem)

      webServer.start(jobManagerAkkaURL)

      Option(webServer)
    } else {
      None
    }
  }

  def stop(): Unit = {
    LOG.info("Stopping FlinkMiniCluster.")
    shutdown()
    awaitTermination()

    leaderRetrievalService.foreach(_.stop())
    isRunning = false
  }

  protected def shutdown(): Unit = {
    webMonitor foreach {
      _.stop()
    }
    
    val tmFutures = taskManagerActors map {
      _.map(gracefulStop(_, timeout))
    } getOrElse(Seq())


    val jmFutures = jobManagerActors map {
      _.map(gracefulStop(_, timeout))
    } getOrElse(Seq())

    implicit val executionContext = ExecutionContext.global

    Await.ready(Future.sequence(jmFutures ++ tmFutures), timeout)

    if (!useSingleActorSystem) {
      taskManagerActorSystems foreach {
        _ foreach(_.shutdown())
      }
    }

    jobManagerActorSystems foreach {
      _ foreach(_.shutdown())
    }
  }

  def awaitTermination(): Unit = {
    jobManagerActorSystems foreach {
      _ foreach(_.awaitTermination())
    }

    taskManagerActorSystems foreach {
      _ foreach(_.awaitTermination())
    }
  }
  
  def running = isRunning

  // --------------------------------------------------------------------------
  //                          Utility Methods
  // --------------------------------------------------------------------------

  /** Waits with the default timeout until all task managers have registered at the job manager
    *
    * @throws java.util.concurrent.TimeoutException
    * @throws java.lang.InterruptedException
    */
  @throws(classOf[TimeoutException])
  @throws(classOf[InterruptedException])
  def waitForTaskManagersToBeRegistered(): Unit = {
    waitForTaskManagersToBeRegistered(timeout)
  }

  /** Waits until all task managers have registered at the job manager until the timeout is reached.
    *
    * @param timeout
    * @throws java.util.concurrent.TimeoutException
    * @throws java.lang.InterruptedException
    */
  @throws(classOf[TimeoutException])
  @throws(classOf[InterruptedException])
  def waitForTaskManagersToBeRegistered(timeout: FiniteDuration): Unit = {
    val futures = taskManagerActors map {
      _ map(taskManager => (taskManager ? NotifyWhenRegisteredAtAnyJobManager)(timeout))
    } getOrElse(Seq())

    Await.ready(Future.sequence(futures), timeout)
  }

  @throws(classOf[JobExecutionException])
  def submitJobAndWait(
      jobGraph: JobGraph,
      printUpdates: Boolean)
    : JobExecutionResult = {
    submitJobAndWait(jobGraph, printUpdates, timeout)
  }

  def submitJobAndWait(
    jobGraph: JobGraph,
    printUpdates: Boolean,
    timeout: FiniteDuration)
  : JobExecutionResult = {
    submitJobAndWait(jobGraph, printUpdates, timeout, createLeaderRetrievalService())
  }
  
  @throws(classOf[JobExecutionException])
  def submitJobAndWait(
      jobGraph: JobGraph,
      printUpdates: Boolean,
      timeout: FiniteDuration,
      leaderRetrievalService: LeaderRetrievalService)
    : JobExecutionResult = {

    val clientActorSystem = startJobClientActorSystem(jobGraph.getJobID)

     try {
     JobClient.submitJobAndWait(
       clientActorSystem,
       leaderRetrievalService,
       jobGraph,
       timeout,
       printUpdates,
       this.getClass.getClassLoader())
    } finally {
       if(!useSingleActorSystem) {
         // we have to shutdown the just created actor system
         shutdownJobClientActorSystem(clientActorSystem)
       }
     }
  }

  @throws(classOf[JobExecutionException])
  def submitJobDetached(jobGraph: JobGraph) : JobSubmissionResult = {

    val jobManagerGateway = try {
      getLeaderGateway(timeout)
    } catch {
      case t: Throwable =>
        throw new JobExecutionException(
          jobGraph.getJobID,
          "Could not retrieve JobManager ActorRef.",
          t
        )
    }

    JobClient.submitJobDetached(jobManagerGateway,
      jobGraph,
      timeout,
      this.getClass.getClassLoader())

    new JobSubmissionResult(jobGraph.getJobID)
  }

  def shutdownJobClientActorSystem(actorSystem: ActorSystem): Unit = {
    if(!useSingleActorSystem) {
      actorSystem.shutdown()
    }
  }

  protected def createLeaderRetrievalService(): LeaderRetrievalService = {
    (jobManagerActorSystems, jobManagerActors) match {
      case (Some(jmActorSystems), Some(jmActors)) =>
        if (recoveryMode == RecoveryMode.STANDALONE) {
          new StandaloneLeaderRetrievalService(
            AkkaUtils.getAkkaURL(jmActorSystems(0), jmActors(0)))
        } else {
          ZooKeeperUtils.createLeaderRetrievalService(configuration)
        }

      case _ => throw new Exception("The FlinkMiniCluster has not been started properly.")
    }
  }

  protected def clearLeader(): Unit = {
    futureLock.synchronized{
      leaderGateway = Promise()
      leaderIndex = Promise()
    }
  }

  override def notifyLeaderAddress(address: String, leaderSessionID: UUID): Unit = {
    if (address != null && !address.equals("")) {
      // only accept leader addresses which are not null and non-empty

      val selectedLeader = (jobManagerActorSystems, jobManagerActors) match {
        case (Some(systems), Some(actors)) =>
          val actorPaths = systems.zip(actors).zipWithIndex.map {
            case ((system, actor), index) => (AkkaUtils.getAkkaURL(system, actor), actor, index)
          }

          actorPaths.find {
            case (path, actor, index) => path.equals(address)
          }.map(x => (x._2, x._3))
        case _ => None
      }

      futureLock.synchronized {
        if (leaderGateway.isCompleted) {
          // assignments happen atomically and only here
          leaderGateway = Promise()
          leaderIndex = Promise()
        }

        selectedLeader match {
          case Some((leader, index)) =>
            leaderGateway.success(new AkkaActorGateway(leader, leaderSessionID))
            leaderIndex.success(index)
          case None =>
            leaderGateway.failure(
              new Exception(s"Could not find job manager with address ${address}."))
            leaderIndex.failure(
              new Exception(s"Could not find job manager index with address ${address}.")
            )
        }
      }
    }
  }

  override def handleError(exception: Exception): Unit = {
    futureLock.synchronized {
      if(leaderGateway.isCompleted) {
        leaderGateway = Promise.failed(exception)
        leaderIndex = Promise.failed(exception)
      } else{
        leaderGateway.failure(exception)
        leaderIndex.failure(exception)
      }
    }
  }
}
