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

import java.net.{URL, URLClassLoader}
import java.util.UUID
import java.util.concurrent.{CompletableFuture, Executors, TimeUnit}

import akka.pattern.Patterns.gracefulStop
import akka.pattern.ask
import akka.actor.{ActorRef, ActorSystem}
import com.typesafe.config.Config
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.common.{JobExecutionResult, JobID, JobSubmissionResult}
import org.apache.flink.configuration._
import org.apache.flink.core.fs.Path
import org.apache.flink.runtime.akka.{AkkaJobManagerGateway, AkkaUtils}
import org.apache.flink.runtime.client.{JobClient, JobExecutionException}
import org.apache.flink.runtime.concurrent.{FutureUtils, ScheduledExecutorServiceAdapter}
import org.apache.flink.runtime.execution.librarycache.FlinkUserCodeClassLoaders
import org.apache.flink.runtime.highavailability.{HighAvailabilityServices, HighAvailabilityServicesUtils}
import org.apache.flink.runtime.instance.{ActorGateway, AkkaActorGateway}
import org.apache.flink.runtime.jobgraph.JobGraph
import org.apache.flink.runtime.jobmanager.HighAvailabilityMode
import org.apache.flink.runtime.leaderretrieval.{LeaderRetrievalListener, LeaderRetrievalService}
import org.apache.flink.runtime.messages.TaskManagerMessages.NotifyWhenRegisteredAtJobManager
import org.apache.flink.runtime.metrics.{MetricRegistryConfiguration, MetricRegistryImpl}
import org.apache.flink.runtime.util.{ExecutorThreadFactory, Hardware}
import org.apache.flink.runtime.webmonitor.retriever.impl.{AkkaJobManagerRetriever, AkkaQueryServiceRetriever}
import org.apache.flink.runtime.webmonitor.{WebMonitor, WebMonitorUtils}
import org.apache.flink.util.{ExecutorUtils, NetUtils}
import org.slf4j.LoggerFactory

import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.concurrent._
import scala.util.{Failure, Success}

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
    val highAvailabilityServices: HighAvailabilityServices,
    val useSingleActorSystem: Boolean)
  extends LeaderRetrievalListener
  with JobExecutorService {

  protected val LOG = LoggerFactory.getLogger(classOf[FlinkMiniCluster])

  // --------------------------------------------------------------------------
  //                           Construction
  // --------------------------------------------------------------------------

  // NOTE: THIS MUST BE getByName("localhost"), which is 127.0.0.1 and
  // not getLocalHost(), which may be 127.0.1.1
  val hostname = userConfiguration.getString(
    JobManagerOptions.ADDRESS,
    "localhost")

  protected val originalConfiguration = generateConfiguration(userConfiguration)

  /** Future to the [[ActorGateway]] of the current leader */
  var leaderGateway: Promise[ActorGateway] = Promise()

  /** Future to the index of the current leader */
  var leaderIndex: Promise[Int] = Promise()

  /** Future lock */
  val futureLock = new Object()

  implicit val executionContext = ExecutionContext.global

  implicit val timeout = AkkaUtils.getTimeout(originalConfiguration)

  val haMode = HighAvailabilityMode.fromConfig(originalConfiguration)

  val numJobManagers = getNumberOfJobManagers

  var numTaskManagers = originalConfiguration.getInteger(
    ConfigConstants.LOCAL_NUMBER_TASK_MANAGER,
    ConfigConstants.DEFAULT_LOCAL_NUMBER_TASK_MANAGER)

  var jobManagerActorSystems: Option[Seq[ActorSystem]] = None
  var jobManagerActors: Option[Seq[ActorRef]] = None
  var webMonitor: Option[WebMonitor] = None
  var taskManagerActorSystems: Option[Seq[ActorSystem]] = None
  var taskManagerActors: Option[Seq[ActorRef]] = None

  var resourceManagerActorSystems: Option[Seq[ActorSystem]] = None
  var resourceManagerActors: Option[Seq[ActorRef]] = None

  protected var jobManagerLeaderRetrievalService: Option[LeaderRetrievalService] = None

  private var isRunning = false

  val futureExecutor = Executors.newScheduledThreadPool(
    Hardware.getNumberCPUCores(),
    new ExecutorThreadFactory("mini-cluster-future"))

  val ioExecutor = Executors.newFixedThreadPool(
    Hardware.getNumberCPUCores(),
    new ExecutorThreadFactory("mini-cluster-io"))

  protected var metricRegistryOpt: Option[MetricRegistryImpl] = None

  def this(configuration: Configuration, useSingleActorSystem: Boolean) {
    this(
      configuration,
      HighAvailabilityServicesUtils.createAvailableOrEmbeddedServices(
        configuration,
        ExecutionContext.global),
      useSingleActorSystem)
  }

  def configuration: Configuration = {
    if (originalConfiguration.getInteger(JobManagerOptions.PORT) == 0) {
      val leaderConfiguration = new Configuration(originalConfiguration)

      val leaderPort = getLeaderRPCPort

      leaderConfiguration.setInteger(JobManagerOptions.PORT, leaderPort)

      leaderConfiguration
    } else {
      originalConfiguration
    }
  }

  // --------------------------------------------------------------------------
  //                           Abstract Methods
  // --------------------------------------------------------------------------

  def generateConfiguration(userConfiguration: Configuration): Configuration

  def startResourceManager(index: Int, system: ActorSystem): ActorRef

  def startJobManager(
    index: Int,
    system: ActorSystem,
    optRestAddress: Option[String])
  : ActorRef

  def startTaskManager(index: Int, system: ActorSystem): ActorRef

  // --------------------------------------------------------------------------
  //                           Getters/Setters
  // --------------------------------------------------------------------------

  def getNumberOfJobManagers: Int = {
    originalConfiguration.getInteger(
      ConfigConstants.LOCAL_NUMBER_JOB_MANAGER,
      ConfigConstants.DEFAULT_LOCAL_NUMBER_JOB_MANAGER
    )
  }

  def getNumberOfResourceManagers: Int = {
    originalConfiguration.getInteger(
      ResourceManagerOptions.LOCAL_NUMBER_RESOURCE_MANAGER
    )
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

  def getLeaderRPCPort: Int = {
    val index = getLeaderIndex(timeout)

    jobManagerActorSystems match {
      case Some(jmActorSystems) =>
        AkkaUtils.getAddress(jmActorSystems(index)).port match {
          case Some(p) => p
          case None => -1
        }

      case None => throw new Exception("The JobManager of the LocalFlinkMiniCluster has not been " +
                                         "started properly.")
    }
  }

  def getResourceManagerAkkaConfig(index: Int): Config = {
    if (useSingleActorSystem) {
      AkkaUtils.getAkkaConfig(originalConfiguration, None)
    } else {
      val port = originalConfiguration.getInteger(
        ResourceManagerOptions.IPC_PORT)

      val resolvedPort = if(port != 0) port + index else port

      AkkaUtils.getAkkaConfig(originalConfiguration, Some((hostname, resolvedPort)))
    }
  }

  def getJobManagerAkkaConfig(index: Int): Config = {
    if (useSingleActorSystem) {
      AkkaUtils.getAkkaConfig(originalConfiguration, None)
    }
    else {
      val port = originalConfiguration.getInteger(JobManagerOptions.PORT)

      val resolvedPort = if(port != 0) port + index else port

      AkkaUtils.getAkkaConfig(originalConfiguration, Some((hostname, resolvedPort)))
    }
  }

  def getTaskManagerAkkaConfig(index: Int): Config = {
    val portRange = originalConfiguration.getString(TaskManagerOptions.RPC_PORT)

    val portRangeIterator = NetUtils.getPortRangeFromString(portRange)

    val resolvedPort = if (portRangeIterator.hasNext) {
      val port = portRangeIterator.next()
      if (port > 0) port + index else 0
    } else 0

    AkkaUtils.getAkkaConfig(originalConfiguration, Some((hostname, resolvedPort)))
  }

  /**
    * Sets CI environment (Travis) specific config defaults.
    */
  def setDefaultCiConfig(config: Configuration) : Unit = {
    // https://docs.travis-ci.com/user/environment-variables#Default-Environment-Variables
    if (sys.env.contains("CI")) {
      // Only set if nothing specified in config
      if (!config.contains(AkkaOptions.ASK_TIMEOUT)) {
        val duration = Duration(AkkaOptions.ASK_TIMEOUT.defaultValue()) * 10
        config.setString(AkkaOptions.ASK_TIMEOUT, s"${duration.toSeconds}s")

        LOG.info(s"Akka ask timeout set to ${duration.toSeconds}s")
      }
    }
  }

  // --------------------------------------------------------------------------
  //                          Start/Stop Methods
  // --------------------------------------------------------------------------

  def startResourceManagerActorSystem(index: Int): ActorSystem = {
    val config = getResourceManagerAkkaConfig(index)
    val testConfig = AkkaUtils.testDispatcherConfig.withFallback(config)
    AkkaUtils.createActorSystem(testConfig)
  }

  def startJobManagerActorSystem(index: Int): ActorSystem = {
    val config = getJobManagerAkkaConfig(index)
    val testConfig = AkkaUtils.testDispatcherConfig.withFallback(config)
    AkkaUtils.createActorSystem(testConfig)
  }

  def startTaskManagerActorSystem(index: Int): ActorSystem = {
    val config = getTaskManagerAkkaConfig(index)
    val testConfig = AkkaUtils.testDispatcherConfig.withFallback(config)
    AkkaUtils.createActorSystem(testConfig)
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
      JobClient.startJobClientActorSystem(originalConfiguration, hostname)
    }
  }

  def start(): Unit = {
    start(true)
  }

  def start(waitForTaskManagerRegistration: Boolean): Unit = {
    LOG.info("Starting FlinkMiniCluster.")

    lazy val singleActorSystem = startJobManagerActorSystem(0)

    val metricRegistry = new MetricRegistryImpl(
      MetricRegistryConfiguration.fromConfiguration(originalConfiguration))

    metricRegistryOpt = Some(metricRegistry)

    if (originalConfiguration.getBoolean(ConfigConstants.LOCAL_START_WEBSERVER, false)) {
      metricRegistry.startQueryService(singleActorSystem, null)
    }

    val (jmActorSystems, jmActors) =
      (for(i <- 0 until numJobManagers) yield {
        val actorSystem = if(useSingleActorSystem) {
          singleActorSystem
        } else {
          startJobManagerActorSystem(i)
        }

        if (i == 0) {
          webMonitor = startWebServer(originalConfiguration, actorSystem)
        }

        (actorSystem, startJobManager(i, actorSystem, webMonitor.map(_.getRestAddress)))
      }).unzip

    jobManagerActorSystems = Some(jmActorSystems)
    jobManagerActors = Some(jmActors)

    // find out which job manager the leader is
    jobManagerLeaderRetrievalService = Option(highAvailabilityServices.getJobManagerLeaderRetriever(
      HighAvailabilityServices.DEFAULT_JOB_ID))

    jobManagerLeaderRetrievalService.foreach(_.start(this))

    // start as many resource managers as job managers
    val (rmActorSystems, rmActors) =
      (for(i <- 0 until getNumberOfResourceManagers) yield {
        val actorSystem = if(useSingleActorSystem) {
          jmActorSystems(0)
        } else {
          startResourceManagerActorSystem(i)
        }

        (actorSystem, startResourceManager(i, actorSystem))
      }).unzip

    resourceManagerActorSystems = Some(rmActorSystems)
    resourceManagerActors = Some(rmActors)

    // start task managers
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

    if(waitForTaskManagerRegistration) {
      waitForTaskManagersToBeRegistered()
    }

    isRunning = true
  }

  def startWebServer(
      config: Configuration,
      actorSystem: ActorSystem)
    : Option[WebMonitor] = {
    if(
      config.getBoolean(ConfigConstants.LOCAL_START_WEBSERVER, false) &&
        config.getInteger(WebOptions.PORT, 0) >= 0) {

      val flinkTimeout = FutureUtils.toTime(timeout)

      LOG.info("Starting JobManger web frontend")
      // start the new web frontend. we need to load this dynamically
      // because it is not in the same project/dependencies
      val webServer = Option(
        WebMonitorUtils.startWebRuntimeMonitor(
          config,
          highAvailabilityServices,
          new AkkaJobManagerRetriever(actorSystem, flinkTimeout, 10, Time.milliseconds(50L)),
          new AkkaQueryServiceRetriever(actorSystem, flinkTimeout),
          flinkTimeout,
          new ScheduledExecutorServiceAdapter(futureExecutor))
      )

      webServer.foreach(_.start())

      webServer
    } else {
      None
    }
  }

  def stop(): Unit = {
    LOG.info("Stopping FlinkMiniCluster.")
    startInternalShutdown()
    awaitTermination()

    jobManagerLeaderRetrievalService.foreach(_.stop())

    highAvailabilityServices.closeAndCleanupAllData()

    isRunning = false

    ExecutorUtils.gracefulShutdown(
      timeout.toMillis,
      TimeUnit.MILLISECONDS,
      futureExecutor,
      ioExecutor)
  }

  def firstActorSystem(): Option[ActorSystem] = {
    jobManagerActorSystems match {
      case Some(jmActorSystems) => Some(jmActorSystems.head)
      case None => None
    }
  }

  protected def startInternalShutdown(): Unit = {
    webMonitor foreach {
      _.stop()
    }

    val tmFutures = taskManagerActors map {
      _.map(gracefulStop(_, timeout))
    } getOrElse(Seq())


    val jmFutures = jobManagerActors map {
      _.map(gracefulStop(_, timeout))
    } getOrElse(Seq())

    val rmFutures = resourceManagerActors map {
      _.map(gracefulStop(_, timeout))
    } getOrElse(Seq())

    Await.ready(Future.sequence(jmFutures ++ tmFutures ++ rmFutures), timeout)

    metricRegistryOpt.foreach(_.shutdown().get())

    if (!useSingleActorSystem) {
      taskManagerActorSystems foreach {
        _ foreach(_.terminate())
      }

      resourceManagerActorSystems foreach {
        _ foreach(_.terminate())
      }
    }

    jobManagerActorSystems foreach {
      _ foreach(_.terminate())
    }
  }

  def awaitTermination(): Unit = {
    jobManagerActorSystems foreach {
      _ foreach(s => Await.ready(s.whenTerminated, Duration.Inf))
    }

    resourceManagerActorSystems foreach {
      _ foreach(s => Await.ready(s.whenTerminated, Duration.Inf))
    }

    taskManagerActorSystems foreach {
      _ foreach(s => Await.ready(s.whenTerminated, Duration.Inf))
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
      _ map(taskManager => (taskManager ? NotifyWhenRegisteredAtJobManager)(timeout))
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

  @throws(classOf[JobExecutionException])
  def submitJobAndWait(
      jobGraph: JobGraph,
      printUpdates: Boolean,
      timeout: FiniteDuration)
    : JobExecutionResult = {

    val clientActorSystem = startJobClientActorSystem(jobGraph.getJobID)

    val userCodeClassLoader =
      try {
        createUserCodeClassLoader(
          jobGraph.getUserJars,
          jobGraph.getClasspaths,
          Thread.currentThread().getContextClassLoader)
      } catch {
        case e: Exception => throw new JobExecutionException(
          jobGraph.getJobID,
          "Could not create the user code class loader.",
          e)
      }

    try {
      JobClient.submitJobAndWait(
       clientActorSystem,
       configuration,
       highAvailabilityServices,
       jobGraph,
       timeout,
       printUpdates,
       userCodeClassLoader)
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

    val userCodeClassLoader =
      try {
        createUserCodeClassLoader(
          jobGraph.getUserJars,
          jobGraph.getClasspaths,
          Thread.currentThread().getContextClassLoader)
      } catch {
        case e: Exception => throw new JobExecutionException(
          jobGraph.getJobID,
          "Could not create the user code class loader.",
          e)
      }

    JobClient.submitJobDetached(
      new AkkaJobManagerGateway(jobManagerGateway),
      configuration,
      jobGraph,
      Time.milliseconds(timeout.toMillis),
      userCodeClassLoader)

    new JobSubmissionResult(jobGraph.getJobID)
  }

  def shutdownJobClientActorSystem(actorSystem: ActorSystem): Unit = {
    if(!useSingleActorSystem) {
      actorSystem.terminate().onComplete {
        case Success(_) =>
        case Failure(t) => LOG.warn("Could not cleanly shut down the job client actor system.", t)
      }
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

  private def createUserCodeClassLoader(
      jars: java.util.List[Path],
      classPaths: java.util.List[URL],
      parentClassLoader: ClassLoader): URLClassLoader = {

    val urls = new Array[URL](jars.size() + classPaths.size())

    import scala.collection.JavaConverters._

    var counter = 0

    for (path <- jars.asScala) {
      urls(counter) = path.makeQualified(path.getFileSystem).toUri.toURL
      counter += 1
    }

    for (classPath <- classPaths.asScala) {
      urls(counter) = classPath
      counter += 1
    }

    FlinkUserCodeClassLoaders.parentFirst(urls, parentClassLoader)
  }

  /**
    * Run the given job and block until its execution result can be returned.
    *
    * @param jobGraph to execute
    * @return Execution result of the executed job
    * @throws JobExecutionException if the job failed to execute
    */
  override def executeJobBlocking(jobGraph: JobGraph) = {
    submitJobAndWait(jobGraph, false)
  }

  override def closeAsync() = {
    try {
      stop()
      CompletableFuture.completedFuture(null)
    } catch {
      case e: Exception =>
        FutureUtils.completedExceptionally(e)
    }
  }
}
