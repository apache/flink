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
import java.util.concurrent.ExecutorService

import akka.actor.{ActorRef, ActorSystem, Props}
import org.apache.flink.api.common.JobID
import org.apache.flink.api.common.io.FileOutputFormat
import org.apache.flink.configuration.{ConfigConstants, Configuration}
import org.apache.flink.runtime.checkpoint.CheckpointRecoveryFactory
import org.apache.flink.runtime.checkpoint.savepoint.SavepointStore
import org.apache.flink.runtime.clusterframework.FlinkResourceManager
import org.apache.flink.runtime.clusterframework.standalone.StandaloneResourceManager
import org.apache.flink.runtime.clusterframework.types.{ResourceID, ResourceIDRetrievable}
import org.apache.flink.runtime.execution.librarycache.BlobLibraryCacheManager
import org.apache.flink.runtime.executiongraph.restart.RestartStrategyFactory
import org.apache.flink.runtime.instance.InstanceManager
import org.apache.flink.runtime.io.disk.iomanager.IOManager
import org.apache.flink.runtime.io.network.NetworkEnvironment
import org.apache.flink.runtime.io.network.netty.NettyConfig
import org.apache.flink.runtime.jobmanager.scheduler.Scheduler
import org.apache.flink.runtime.jobmanager.{JobManager, MemoryArchivist, SubmittedJobGraphStore}
import org.apache.flink.runtime.leaderelection.LeaderElectionService
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService
import org.apache.flink.runtime.memory.MemoryManager
import org.apache.flink.runtime.messages.JobManagerMessages
import org.apache.flink.runtime.messages.JobManagerMessages.{RunningJobsStatus, StoppingFailure, StoppingResponse}
import org.apache.flink.runtime.metrics.MetricRegistry
import org.apache.flink.runtime.taskexecutor.{TaskManagerConfiguration, TaskManagerServices, TaskManagerServicesConfiguration}
import org.apache.flink.runtime.taskmanager.{TaskManager, TaskManagerLocation}
import org.apache.flink.runtime.util.{EnvironmentInformation, LeaderRetrievalUtils}

import scala.concurrent.Await
import scala.concurrent.duration.FiniteDuration

/**
 * Local Flink mini cluster which executes all [[TaskManager]]s and the [[JobManager]] in the same
 * JVM. It extends the [[FlinkMiniCluster]] by having convenience functions to setup Flink's
 * configuration and implementations to create [[JobManager]] and [[TaskManager]].
 *
 * @param userConfiguration Configuration object with the user provided configuration values
 * @param singleActorSystem true if all actors (JobManager and TaskManager) shall be run in the same
 *                          [[ActorSystem]], otherwise false
 */
class LocalFlinkMiniCluster(
    userConfiguration: Configuration,
    singleActorSystem: Boolean) extends FlinkMiniCluster(userConfiguration, singleActorSystem) {

  def this(userConfiguration: Configuration) = this(userConfiguration, true)

  // --------------------------------------------------------------------------

  override def generateConfiguration(userConfiguration: Configuration): Configuration = {
    val config = getDefaultConfig

    setDefaultCiConfig(config)

    config.addAll(userConfiguration)
    setMemory(config)
    initializeIOFormatClasses(config)

    config
  }

  //------------------------------------------------------------------------------------------------
  // Actor classes
  //------------------------------------------------------------------------------------------------

  val jobManagerClass: Class[_ <: JobManager] = classOf[JobManager]

  val taskManagerClass: Class[_ <: TaskManager] = classOf[TaskManager]

  val memoryArchivistClass: Class[_ <: MemoryArchivist] = classOf[MemoryArchivist]

  val resourceManagerClass: Class[_ <: FlinkResourceManager[_ <: ResourceIDRetrievable]] =
    classOf[StandaloneResourceManager]

  //------------------------------------------------------------------------------------------------
  // Start methods for the distributed components
  //------------------------------------------------------------------------------------------------

  override def startJobManager(index: Int, system: ActorSystem): ActorRef = {
    val config = originalConfiguration.clone()

    val jobManagerName = getJobManagerName(index)
    val archiveName = getArchiveName(index)

    val jobManagerPort = config.getInteger(
      ConfigConstants.JOB_MANAGER_IPC_PORT_KEY,
      ConfigConstants.DEFAULT_JOB_MANAGER_IPC_PORT)

    if(jobManagerPort > 0) {
      config.setInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY, jobManagerPort + index)
    }

    val (executorService,
    instanceManager,
    scheduler,
    libraryCacheManager,
    restartStrategyFactory,
    timeout,
    archiveCount,
    leaderElectionService,
    submittedJobGraphStore,
    checkpointRecoveryFactory,
    savepointStore,
    jobRecoveryTimeout,
    metricsRegistry) = JobManager.createJobManagerComponents(config, createLeaderElectionService())

    val archive = system.actorOf(
      getArchiveProps(
        memoryArchivistClass,
        archiveCount),
      archiveName)

    system.actorOf(
      getJobManagerProps(
        jobManagerClass,
        config,
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
        metricsRegistry),
      jobManagerName)
  }

  override def startResourceManager(index: Int, system: ActorSystem): ActorRef = {
    val config = originalConfiguration.clone()

    val resourceManagerName = getResourceManagerName(index)

    val resourceManagerPort = config.getInteger(
      ConfigConstants.RESOURCE_MANAGER_IPC_PORT_KEY,
      ConfigConstants.DEFAULT_RESOURCE_MANAGER_IPC_PORT)

    if(resourceManagerPort > 0) {
      config.setInteger(ConfigConstants.RESOURCE_MANAGER_IPC_PORT_KEY, resourceManagerPort + index)
    }

    val resourceManagerProps = getResourceManagerProps(
      resourceManagerClass,
      config,
      createLeaderRetrievalService())

    system.actorOf(resourceManagerProps, resourceManagerName)
  }

  override def startTaskManager(index: Int, system: ActorSystem): ActorRef = {
    val config = originalConfiguration.clone()

    val rpcPort = config.getInteger(
      ConfigConstants.TASK_MANAGER_IPC_PORT_KEY,
      ConfigConstants.DEFAULT_TASK_MANAGER_IPC_PORT)

    val dataPort = config.getInteger(
      ConfigConstants.TASK_MANAGER_DATA_PORT_KEY,
      ConfigConstants.DEFAULT_TASK_MANAGER_DATA_PORT)

    if (rpcPort > 0) {
      config.setInteger(ConfigConstants.TASK_MANAGER_IPC_PORT_KEY, rpcPort + index)
    }
    if (dataPort > 0) {
      config.setInteger(ConfigConstants.TASK_MANAGER_DATA_PORT_KEY, dataPort + index)
    }

    val localExecution = numTaskManagers == 1

    val taskManagerActorName = if (singleActorSystem) {
      TaskManager.TASK_MANAGER_NAME + "_" + (index + 1)
    } else {
      TaskManager.TASK_MANAGER_NAME
    }

    val resourceID = ResourceID.generate() // generate random resource id

    val taskManagerAddress = InetAddress.getByName(hostname)

    val taskManagerConfiguration = TaskManagerConfiguration.fromConfiguration(config)
    val taskManagerServicesConfiguration = TaskManagerServicesConfiguration.fromConfiguration(
      config,
      taskManagerAddress,
      localExecution)

    val taskManagerServices = TaskManagerServices.fromConfiguration(
      taskManagerServicesConfiguration,
      resourceID)

    val metricRegistry = taskManagerServices.getMetricRegistry()

    val props = getTaskManagerProps(
      taskManagerClass,
      taskManagerConfiguration,
      resourceID,
      taskManagerServices.getTaskManagerLocation(),
      taskManagerServices.getMemoryManager(),
      taskManagerServices.getIOManager(),
      taskManagerServices.getNetworkEnvironment,
      createLeaderRetrievalService(),
      metricRegistry)

    metricRegistry.startQueryService(system)

    system.actorOf(props, taskManagerActorName)
  }

  //------------------------------------------------------------------------------------------------
  // Props for the distributed components
  //------------------------------------------------------------------------------------------------

  def getArchiveProps(archiveClass: Class[_ <: MemoryArchivist], archiveCount: Int): Props = {
    JobManager.getArchiveProps(archiveClass, archiveCount)
  }

  def getJobManagerProps(
    jobManagerClass: Class[_ <: JobManager],
    configuration: Configuration,
    executorService: ExecutorService,
    instanceManager: InstanceManager,
    scheduler: Scheduler,
    libraryCacheManager: BlobLibraryCacheManager,
    archive: ActorRef,
    restartStrategyFactory: RestartStrategyFactory,
    timeout: FiniteDuration,
    leaderElectionService: LeaderElectionService,
    submittedJobGraphStore: SubmittedJobGraphStore,
    checkpointRecoveryFactory: CheckpointRecoveryFactory,
    savepointStore: SavepointStore,
    jobRecoveryTimeout: FiniteDuration,
    metricsRegistry: Option[MetricRegistry]): Props = {

    JobManager.getJobManagerProps(
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

  def getTaskManagerProps(
    taskManagerClass: Class[_ <: TaskManager],
    taskManagerConfig: TaskManagerConfiguration,
    resourceID: ResourceID,
    taskManagerLocation: TaskManagerLocation,
    memoryManager: MemoryManager,
    ioManager: IOManager,
    networkEnvironment: NetworkEnvironment,
    leaderRetrievalService: LeaderRetrievalService,
    metricsRegistry: MetricRegistry): Props = {

    TaskManager.getTaskManagerProps(
      taskManagerClass,
      taskManagerConfig,
      resourceID,
      taskManagerLocation,
      memoryManager,
      ioManager,
      networkEnvironment,
      leaderRetrievalService,
      metricsRegistry)
  }

  def getResourceManagerProps(
    resourceManagerClass: Class[_ <: FlinkResourceManager[_ <: ResourceIDRetrievable]],
    configuration: Configuration,
    leaderRetrievalService: LeaderRetrievalService): Props = {

    FlinkResourceManager.getResourceManagerProps(
      resourceManagerClass,
      configuration,
      leaderRetrievalService)
  }

  //------------------------------------------------------------------------------------------------
  // Helper methods
  //------------------------------------------------------------------------------------------------

  def createLeaderElectionService(): Option[LeaderElectionService] = {
    None
  }

  def initializeIOFormatClasses(configuration: Configuration): Unit = {
    try {
      val om = classOf[FileOutputFormat[_]].getDeclaredMethod("initDefaultsFromConfiguration",
        classOf[Configuration])
      om.setAccessible(true)
      om.invoke(null, configuration)
    } catch {
      case e: Exception =>
        LOG.error("Cannot (re) initialize the globally loaded defaults. Some classes might not " +
          "follow the specified default behaviour.")
    }
  }

  def setMemory(config: Configuration): Unit = {
    // set this only if no memory was pre-configured
    if (config.getInteger(ConfigConstants.TASK_MANAGER_MEMORY_SIZE_KEY, -1) == -1) {

      val bufferSize: Int = config.getInteger(
        ConfigConstants.TASK_MANAGER_MEMORY_SEGMENT_SIZE_KEY,
        ConfigConstants.DEFAULT_TASK_MANAGER_MEMORY_SEGMENT_SIZE)

      val bufferMem: Long = config.getLong(
        ConfigConstants.TASK_MANAGER_NETWORK_NUM_BUFFERS_KEY,
        ConfigConstants.DEFAULT_TASK_MANAGER_NETWORK_NUM_BUFFERS) * bufferSize.toLong

      val numTaskManager = config.getInteger(
        ConfigConstants.LOCAL_NUMBER_TASK_MANAGER,
        ConfigConstants.DEFAULT_LOCAL_NUMBER_TASK_MANAGER)

      val memoryFraction = config.getFloat(
        ConfigConstants.TASK_MANAGER_MEMORY_FRACTION_KEY,
        ConfigConstants.DEFAULT_MEMORY_MANAGER_MEMORY_FRACTION)

      // full memory size
      var memorySize: Long = EnvironmentInformation.getSizeOfFreeHeapMemoryWithDefrag

      // compute the memory size per task manager. we assume equally much memory for
      // each TaskManagers and each JobManager
      memorySize /= numTaskManager + 1 // the +1 is the job manager

      // for each TaskManager, subtract the memory needed for memory buffers
      memorySize -= bufferMem
      memorySize = (memorySize * memoryFraction).toLong
      memorySize >>>= 20 // bytes to megabytes
      config.setLong(ConfigConstants.TASK_MANAGER_MEMORY_SIZE_KEY, memorySize)
    }
  }

  def getDefaultConfig: Configuration = {
    val config: Configuration = new Configuration()

    config.setString(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, hostname)
    config.setInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY, 0)

    config.setInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER,
      ConfigConstants.DEFAULT_LOCAL_NUMBER_TASK_MANAGER)

    // Reduce number of threads for local execution
    config.setInteger(NettyConfig.NUM_THREADS_CLIENT, 1)
    config.setInteger(NettyConfig.NUM_THREADS_SERVER, 2)

    config
  }

  protected def getJobManagerName(index: Int): String = {
    if(singleActorSystem) {
      JobManager.JOB_MANAGER_NAME + "_" + (index + 1)
    } else {
      JobManager.JOB_MANAGER_NAME
    }
  }

  protected def getResourceManagerName(index: Int): String = {
    if(singleActorSystem) {
      FlinkResourceManager.RESOURCE_MANAGER_NAME + "_" + (index + 1)
    } else {
      FlinkResourceManager.RESOURCE_MANAGER_NAME
    }
  }

  protected def getArchiveName(index: Int): String = {
    if(singleActorSystem) {
      JobManager.ARCHIVE_NAME + "_" + (index + 1)
    } else {
      JobManager.ARCHIVE_NAME
    }
  }

  // --------------------------------------------------------------------------
  //  Actions on running jobs
  // --------------------------------------------------------------------------

  def currentlyRunningJobs: Iterable[JobID] = {
    val leader = getLeaderGateway(timeout)
    val future = leader.ask(JobManagerMessages.RequestRunningJobsStatus, timeout)
                       .mapTo[RunningJobsStatus]
    Await.result(future, timeout).runningJobs.map(_.getJobId)
  }

  def getCurrentlyRunningJobsJava(): java.util.List[JobID] = {
    val list = new java.util.ArrayList[JobID]()
    currentlyRunningJobs.foreach(list.add)
    list
  }

  def stopJob(id: JobID) : Unit = {
    val leader = getLeaderGateway(timeout)
    val response = leader.ask(new JobManagerMessages.StopJob(id), timeout)
                         .mapTo[StoppingResponse]
    val rc = Await.result(response, timeout)

    rc match {
      case failure: StoppingFailure =>
        throw new Exception(s"Stopping the job with ID $id failed.", failure.cause)
      case _ =>
    }
  }
}
