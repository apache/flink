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
import java.util.concurrent.{Executor, ScheduledExecutorService}

import akka.actor.{ActorRef, ActorSystem, Props}
import org.apache.flink.api.common.JobID
import org.apache.flink.api.common.io.FileOutputFormat
import org.apache.flink.configuration._
import org.apache.flink.core.fs.Path
import org.apache.flink.runtime.blob.BlobServer
import org.apache.flink.runtime.checkpoint.CheckpointRecoveryFactory
import org.apache.flink.runtime.clusterframework.FlinkResourceManager
import org.apache.flink.runtime.clusterframework.standalone.StandaloneResourceManager
import org.apache.flink.runtime.clusterframework.types.{ResourceID, ResourceIDRetrievable}
import org.apache.flink.runtime.execution.librarycache.BlobLibraryCacheManager
import org.apache.flink.runtime.executiongraph.restart.RestartStrategyFactory
import org.apache.flink.runtime.highavailability.{HighAvailabilityServices, HighAvailabilityServicesUtils}
import org.apache.flink.runtime.instance.InstanceManager
import org.apache.flink.runtime.io.disk.iomanager.IOManager
import org.apache.flink.runtime.io.network.NetworkEnvironment
import org.apache.flink.runtime.io.network.netty.NettyConfig
import org.apache.flink.runtime.jobmanager.scheduler.Scheduler
import org.apache.flink.runtime.jobmanager.{JobManager, MemoryArchivist, SubmittedJobGraphStore}
import org.apache.flink.runtime.jobmaster.JobMaster
import org.apache.flink.runtime.leaderelection.LeaderElectionService
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService
import org.apache.flink.runtime.memory.MemoryManager
import org.apache.flink.runtime.messages.JobManagerMessages
import org.apache.flink.runtime.messages.JobManagerMessages.{RunningJobsStatus, StoppingFailure, StoppingResponse}
import org.apache.flink.runtime.metrics.groups.{JobManagerMetricGroup, TaskManagerMetricGroup}
import org.apache.flink.runtime.metrics.util.MetricUtils
import org.apache.flink.runtime.state.TaskExecutorLocalStateStoresManager
import org.apache.flink.runtime.taskexecutor.{TaskExecutor, TaskManagerConfiguration, TaskManagerServices, TaskManagerServicesConfiguration}
import org.apache.flink.runtime.taskmanager.{TaskManager, TaskManagerLocation}
import org.apache.flink.runtime.util.EnvironmentInformation
import org.apache.flink.util.NetUtils

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{Await, ExecutionContext}

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
    highAvailabilityServices: HighAvailabilityServices,
    singleActorSystem: Boolean) extends FlinkMiniCluster(
  userConfiguration,
  highAvailabilityServices,
  singleActorSystem) {

  def this(userConfiguration: Configuration, useSingleActorSystem: Boolean) = {
    this(
      userConfiguration,
       HighAvailabilityServicesUtils.createAvailableOrEmbeddedServices(
         userConfiguration,
         ExecutionContext.global),
      useSingleActorSystem)
  }

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

  override def startJobManager(
      index: Int,
      system: ActorSystem,
      optRestAddress: Option[String])
    : ActorRef = {
    val config = originalConfiguration.clone()

    val jobManagerName = getJobManagerName(index)
    val archiveName = getArchiveName(index)

    val jobManagerPort = config.getInteger(JobManagerOptions.PORT)

    if(jobManagerPort > 0) {
      config.setInteger(JobManagerOptions.PORT, jobManagerPort + index)
    }

    val (instanceManager,
      scheduler,
      blobServer,
      libraryCacheManager,
      restartStrategyFactory,
      timeout,
      archiveCount,
      archivePath,
      jobRecoveryTimeout,
      jobManagerMetricGroup) = JobManager.createJobManagerComponents(
      config,
      futureExecutor,
      ioExecutor,
      highAvailabilityServices.createBlobStore(),
      metricRegistryOpt.get)

    val archive = system.actorOf(
      getArchiveProps(
        memoryArchivistClass,
        archiveCount,
        archivePath),
      archiveName)

    system.actorOf(
      getJobManagerProps(
        jobManagerClass,
        config,
        futureExecutor,
        ioExecutor,
        instanceManager,
        scheduler,
        blobServer,
        libraryCacheManager,
        archive,
        restartStrategyFactory,
        timeout,
        highAvailabilityServices.getJobManagerLeaderElectionService(
          HighAvailabilityServices.DEFAULT_JOB_ID),
        highAvailabilityServices.getSubmittedJobGraphStore(),
        highAvailabilityServices.getCheckpointRecoveryFactory(),
        jobRecoveryTimeout,
        jobManagerMetricGroup,
        optRestAddress),
      jobManagerName)
  }

  override def startResourceManager(index: Int, system: ActorSystem): ActorRef = {
    val config = originalConfiguration.clone()

    val resourceManagerName = getResourceManagerName(index)

    val resourceManagerPort = config.getInteger(
      ResourceManagerOptions.IPC_PORT)

    if(resourceManagerPort > 0) {
      config.setInteger(ResourceManagerOptions.IPC_PORT,
        resourceManagerPort + index)
    }

    val resourceManagerProps = getResourceManagerProps(
      resourceManagerClass,
      config,
      highAvailabilityServices.getJobManagerLeaderRetriever(
        HighAvailabilityServices.DEFAULT_JOB_ID))

    system.actorOf(resourceManagerProps, resourceManagerName)
  }

  override def startTaskManager(index: Int, system: ActorSystem): ActorRef = {
    val config = originalConfiguration.clone()

    val rpcPortRange = config.getString(TaskManagerOptions.RPC_PORT)

    val rpcPortIterator = NetUtils.getPortRangeFromString(rpcPortRange)

    val dataPort = config.getInteger(TaskManagerOptions.DATA_PORT)

    if (rpcPortIterator.hasNext) {
      val rpcPort = rpcPortIterator.next()
      if (rpcPort > 0) {
        config.setInteger(ConfigConstants.TASK_MANAGER_IPC_PORT_KEY, rpcPort + index)
      }
    }
    if (dataPort > 0) {
      config.setInteger(TaskManagerOptions.DATA_PORT, dataPort + index)
    }

    val localExecution = numTaskManagers == 1

    val taskManagerActorName = if (singleActorSystem) {
      TaskExecutor.TASK_MANAGER_NAME + "_" + (index + 1)
    } else {
      TaskExecutor.TASK_MANAGER_NAME
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
      resourceID,
      ioExecutor,
      EnvironmentInformation.getSizeOfFreeHeapMemoryWithDefrag,
      EnvironmentInformation.getMaxJvmHeapMemory)

    val taskManagerMetricGroup = MetricUtils.instantiateTaskManagerMetricGroup(
      metricRegistryOpt.get,
      taskManagerServices.getTaskManagerLocation(),
      taskManagerServices.getNetworkEnvironment())

    val props = getTaskManagerProps(
      taskManagerClass,
      taskManagerConfiguration,
      resourceID,
      taskManagerServices.getTaskManagerLocation(),
      taskManagerServices.getMemoryManager(),
      taskManagerServices.getIOManager(),
      taskManagerServices.getNetworkEnvironment,
      taskManagerServices.getTaskManagerStateStore,
      taskManagerMetricGroup)

    system.actorOf(props, taskManagerActorName)
  }

  //------------------------------------------------------------------------------------------------
  // Props for the distributed components
  //------------------------------------------------------------------------------------------------

  def getArchiveProps(
      archiveClass: Class[_ <: MemoryArchivist],
      archiveCount: Int,
      arhivePath: Option[Path]): Props = {
    JobManager.getArchiveProps(archiveClass, archiveCount, Option.empty)
  }

  def getJobManagerProps(
      jobManagerClass: Class[_ <: JobManager],
      configuration: Configuration,
      futureExecutor: ScheduledExecutorService,
      ioExecutor: Executor,
      instanceManager: InstanceManager,
      scheduler: Scheduler,
      blobServer: BlobServer,
      libraryCacheManager: BlobLibraryCacheManager,
      archive: ActorRef,
      restartStrategyFactory: RestartStrategyFactory,
      timeout: FiniteDuration,
      leaderElectionService: LeaderElectionService,
      submittedJobGraphStore: SubmittedJobGraphStore,
      checkpointRecoveryFactory: CheckpointRecoveryFactory,
      jobRecoveryTimeout: FiniteDuration,
      jobManagerMetricGroup: JobManagerMetricGroup,
      optRestAddress: Option[String])
    : Props = {

    JobManager.getJobManagerProps(
      jobManagerClass,
      configuration,
      futureExecutor,
      ioExecutor,
      instanceManager,
      scheduler,
      blobServer,
      libraryCacheManager,
      archive,
      restartStrategyFactory,
      timeout,
      leaderElectionService,
      submittedJobGraphStore,
      checkpointRecoveryFactory,
      jobRecoveryTimeout,
      jobManagerMetricGroup,
      optRestAddress)
  }

  def getTaskManagerProps(
    taskManagerClass: Class[_ <: TaskManager],
    taskManagerConfig: TaskManagerConfiguration,
    resourceID: ResourceID,
    taskManagerLocation: TaskManagerLocation,
    memoryManager: MemoryManager,
    ioManager: IOManager,
    networkEnvironment: NetworkEnvironment,
    taskManagerLocalStateStoresManager: TaskExecutorLocalStateStoresManager,
    taskManagerMetricGroup: TaskManagerMetricGroup): Props = {

    TaskManager.getTaskManagerProps(
      taskManagerClass,
      taskManagerConfig,
      resourceID,
      taskManagerLocation,
      memoryManager,
      ioManager,
      networkEnvironment,
      taskManagerLocalStateStoresManager,
      highAvailabilityServices,
      taskManagerMetricGroup)
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
    if (config.getString(TaskManagerOptions.MANAGED_MEMORY_SIZE).equals(
      TaskManagerOptions.MANAGED_MEMORY_SIZE.defaultValue())) {

      val numTaskManager = config.getInteger(
        ConfigConstants.LOCAL_NUMBER_TASK_MANAGER,
        ConfigConstants.DEFAULT_LOCAL_NUMBER_TASK_MANAGER)

      val memoryFraction = config.getFloat(TaskManagerOptions.MANAGED_MEMORY_FRACTION)

      // full memory size
      var memorySize: Long = EnvironmentInformation.getSizeOfFreeHeapMemoryWithDefrag

      // compute the memory size per task manager. we assume equally much memory for
      // each TaskManagers and each JobManager
      memorySize /= numTaskManager + 1 // the +1 is the job manager

      // for each TaskManager, subtract the memory needed for network memory buffers
      memorySize -= TaskManagerServices.calculateNetworkBufferMemory(memorySize, config)
      memorySize = (memorySize * memoryFraction).toLong
      memorySize >>= 20 // bytes to megabytes
      config.setString(TaskManagerOptions.MANAGED_MEMORY_SIZE, memorySize + "m")
    }
  }

  def getDefaultConfig: Configuration = {
    val config: Configuration = new Configuration()

    config.setString(JobManagerOptions.ADDRESS, hostname)
    config.setInteger(JobManagerOptions.PORT, 0)

    config.setInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER,
      ConfigConstants.DEFAULT_LOCAL_NUMBER_TASK_MANAGER)

    // Reduce number of threads for local execution
    config.setInteger(NettyConfig.NUM_THREADS_CLIENT, 1)
    config.setInteger(NettyConfig.NUM_THREADS_SERVER, 2)

    config
  }

  protected def getJobManagerName(index: Int): String = {
    if(singleActorSystem) {
      JobMaster.JOB_MANAGER_NAME + "_" + (index + 1)
    } else {
      JobMaster.JOB_MANAGER_NAME
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
      JobMaster.ARCHIVE_NAME + "_" + (index + 1)
    } else {
      JobMaster.ARCHIVE_NAME
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
