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

package org.apache.flink.runtime.taskmanager

import java.io.{IOException, File}
import java.lang.management.{GarbageCollectorMXBean, MemoryMXBean, ManagementFactory}
import java.net.{InetAddress, InetSocketAddress}
import java.util
import java.util.concurrent.{FutureTask, TimeUnit}
import scala.collection.JavaConverters._
import akka.actor._
import akka.pattern.ask
import org.apache.flink.api.common.cache.DistributedCache
import org.apache.flink.configuration.{GlobalConfiguration, ConfigConstants, Configuration}
import org.apache.flink.core.fs.Path
import org.apache.flink.runtime.ActorLogMessages
import org.apache.flink.runtime.akka.AkkaUtils
import org.apache.flink.runtime.blob.BlobCache
import org.apache.flink.runtime.broadcast.BroadcastVariableManager
import org.apache.flink.runtime.execution.{CancelTaskException, ExecutionState, RuntimeEnvironment}
import org.apache.flink.runtime.execution.librarycache.{BlobLibraryCacheManager, FallbackLibraryCacheManager, LibraryCacheManager}
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID
import org.apache.flink.runtime.filecache.FileCache
import org.apache.flink.runtime.instance.{InstanceConnectionInfo, HardwareDescription, InstanceID}
import org.apache.flink.runtime.io.disk.iomanager.{IOManagerAsync}
import org.apache.flink.runtime.io.network.netty.NettyConnectionManager
import org.apache.flink.runtime.io.network.{NetworkConnectionManager, LocalConnectionManager, ChannelManager}
import org.apache.flink.runtime.jobgraph.JobID
import org.apache.flink.runtime.jobmanager.JobManager
import org.apache.flink.runtime.memorymanager.DefaultMemoryManager
import org.apache.flink.runtime.messages.JobManagerMessages.UpdateTaskExecutionState
import org.apache.flink.runtime.messages.RegistrationMessages.{RegisterTaskManager, AcknowledgeRegistration}
import org.apache.flink.runtime.messages.TaskManagerMessages._
import org.apache.flink.runtime.messages.TaskManagerProfilerMessages.{UnmonitorTask, MonitorTask, RegisterProfilingListener}
import org.apache.flink.runtime.net.NetUtils
import org.apache.flink.runtime.profiling.ProfilingUtils
import org.apache.flink.runtime.util.EnvironmentInformation
import org.apache.flink.util.ExceptionUtils
import org.slf4j.LoggerFactory
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.Failure
import scala.util.Success
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor

/**
 * 
 * 
 * The TaskManager has the following phases:
 * 
 *  - Waiting to be registered with its JobManager. In that phase, it periodically sends 
 *    [[RegisterAtJobManager]] messages to itself, which trigger the sending of
 *    a [[RegisterTaskManager]] message to the JobManager.
 *  
 *  - Upon successful registration, the JobManager replies with an [[AcknowledgeRegistration]]
 *    message. This stops the registration messages and initializes all fields
 *    that require the JobManager's actor reference
 *    
 *  - ...
 */
class TaskManager(val connectionInfo: InstanceConnectionInfo, val jobManagerAkkaURL: String,
                  val taskManagerConfig: TaskManagerConfiguration,
                  val networkConnectionConfig: NetworkConnectionConfiguration)
  extends Actor with ActorLogMessages with ActorLogging {

  import context._
  import taskManagerConfig.{timeout => tmTimeout, _}
  implicit val timeout = tmTimeout

  log.info(s"Starting task manager at ${self.path}.")

  TaskManager.checkTempDirs(tmpDirPaths)
   
  val ioManager = new IOManagerAsync(tmpDirPaths)
  val memoryManager = new DefaultMemoryManager(memorySize, numberOfSlots, pageSize)
  val bcVarManager = new BroadcastVariableManager();
  val hardwareDescription = HardwareDescription.extractFromSystem(memoryManager.getMemorySize)
  val fileCache = new FileCache()
  val runningTasks = scala.collection.mutable.HashMap[ExecutionAttemptID, Task]()

  // Actors which want to be notified once this task manager has been registered at the job manager
  val waitForRegistration = scala.collection.mutable.Set[ActorRef]();

  val profiler = profilingInterval match {
    case Some(interval) => 
              Some(TaskManager.startProfiler(self.path.toSerializationFormat, interval))
    case None => None
  }

  var libraryCacheManager: LibraryCacheManager = _
  var channelManager: Option[ChannelManager] = None
  var registrationScheduler: Option[Cancellable] = None
  var registrationAttempts: Int = 0
  var registered: Boolean = false
  var currentJobManager = ActorRef.noSender
  var instanceID: InstanceID = null;
  var heartbeatScheduler: Option[Cancellable] = None

  if (log.isDebugEnabled) {
    memoryLogggingIntervalMs.foreach {
      interval =>
        val d = FiniteDuration(interval, TimeUnit.MILLISECONDS)
        context.system.scheduler.schedule(d, d, self, LogMemoryUsage)
    }
  }

  override def preStart(): Unit = {
    tryJobManagerRegistration()
  }

  override def postStop(): Unit = {
    log.info(s"Stopping task manager ${self.path}.")

    cancelAndClearEverything(new Exception("Task Manager is shutting down."))

    heartbeatScheduler foreach {
      _.cancel()
    }

    channelManager foreach {
      channelManager =>
        try {
          channelManager.shutdown()
        } catch {
          case t: Throwable => log.error(t, "ChannelManager did not shutdown properly.")
        }
    }

    ioManager.shutdown()
    memoryManager.shutdown()
    fileCache.shutdown()

    if (libraryCacheManager != null) {
      try {
        libraryCacheManager.shutdown()
      }
      catch {
        case t: Throwable => log.error(t, "LibraryCacheManager did not shutdown properly.")
      }
    }
  }

  private def tryJobManagerRegistration(): Unit = {
    registrationAttempts = 0
    import context.dispatcher
    registrationScheduler = Some(context.system.scheduler.schedule(
        TaskManager.REGISTRATION_DELAY, TaskManager.REGISTRATION_INTERVAL,
        self, RegisterAtJobManager))
  }


  override def receiveWithLogMessages: Receive = {
    case RegisterAtJobManager => {
      registrationAttempts += 1

      if (registered) {
        registrationScheduler.foreach(_.cancel())
      }
      else if (registrationAttempts <= TaskManager.MAX_REGISTRATION_ATTEMPTS) {

        log.info(s"Try to register at master ${jobManagerAkkaURL}. ${registrationAttempts}. " +
          s"Attempt")
        val jobManager = context.actorSelection(jobManagerAkkaURL)

        jobManager ! RegisterTaskManager(connectionInfo, hardwareDescription, numberOfSlots)
      }
      else {
        log.error("TaskManager could not register at JobManager.");
        self ! PoisonPill
      }
    }

    case AcknowledgeRegistration(id, blobPort) => {
      if (!registered) {
        registered = true
        currentJobManager = sender
        instanceID = id

        context.watch(currentJobManager)

        log.info(s"TaskManager successfully registered at JobManager ${
          currentJobManager.path.toString }.")

        setupChannelManager()
        setupLibraryCacheManager(blobPort)

        heartbeatScheduler = Some(context.system.scheduler.schedule(
            TaskManager.HEARTBEAT_INTERVAL, TaskManager.HEARTBEAT_INTERVAL, self, SendHeartbeat))

        profiler foreach {
          _.tell(RegisterProfilingListener, JobManager.getProfiler(currentJobManager))
        }

        for (listener <- waitForRegistration) {
          listener ! RegisteredAtJobManager
        }

        waitForRegistration.clear()
      }
    }
    
    case SubmitTask(tdd) => {
      submitTask(tdd)
    }

    case CancelTask(executionID) => {
      runningTasks.get(executionID) match {
        case Some(task) =>
          Future {
            task.cancelExecution()
          }
          sender ! new TaskOperationResult(executionID, true)
        case None =>
          sender ! new TaskOperationResult(executionID, false,
                                           "No task with that execution ID was found.")
      }
    }

    case UnregisterTask(executionID) => {
      unregisterTask(executionID)
    }

    case SendHeartbeat => {
      currentJobManager ! Heartbeat(instanceID)
    }

    case LogMemoryUsage => {
      logMemoryStats()
    }

    case NotifyWhenRegisteredAtJobManager => {
       if (registered) {
         sender ! RegisteredAtJobManager
       } else {
         waitForRegistration += sender
      }
    }

    case FailTask(executionID, cause) => {
      runningTasks.get(executionID) match {
        case Some(task) =>
          Future{
            task.failExternally(cause)
          }
        case None =>
      }
    }

    case Terminated(jobManager) => {
      log.info(s"Job manager ${jobManager.path} is no longer reachable. "
                 + "Cancelling all tasks and trying to reregister.")
      
      cancelAndClearEverything(new Throwable("Lost connection to JobManager"))
      tryJobManagerRegistration()
    }
  }

  def notifyExecutionStateChange(jobID: JobID, executionID: ExecutionAttemptID,
                                 executionState: ExecutionState,
                                 optionalError: Throwable): Unit = {
    log.info(s"Update execution state to ${executionState}.")
    val futureResponse = (currentJobManager ? UpdateTaskExecutionState(new TaskExecutionState
    (jobID, executionID, executionState, optionalError)))(timeout)

    futureResponse.mapTo[Boolean].onComplete {
      case Success(result) =>
        if(!result){
          self ! FailTask(executionID, new IllegalStateException("Task has been disposed on " +
            "JobManager."))
        }

        if (!result || executionState == ExecutionState.FINISHED || executionState ==
          ExecutionState.CANCELED || executionState == ExecutionState.FAILED) {
          self ! UnregisterTask(executionID)
        }
      case Failure(t) => {
        log.warning(s"Execution state change notification failed for task ${executionID} " +
          s"of job ${jobID}. Cause ${t.getMessage}.")
        self ! UnregisterTask(executionID)
      }
    }
  }

  private def submitTask(tdd: TaskDeploymentDescriptor) : Unit = {
    val jobID = tdd.getJobID
    val vertexID = tdd.getVertexID
    val executionID = tdd.getExecutionId
    val taskIndex = tdd.getIndexInSubtaskGroup
    val numSubtasks = tdd.getCurrentNumberOfSubtasks
    var startRegisteringTask = 0L
    var task: Task = null

    try {
      if (log.isDebugEnabled) {
        startRegisteringTask = System.currentTimeMillis()
      }
      libraryCacheManager.registerTask(jobID, executionID, tdd.getRequiredJarFiles());

      if (log.isDebugEnabled) {
        log.debug(s"Register task ${executionID} took ${
          (System.currentTimeMillis() - startRegisteringTask)/1000.0}s")
      }

      val userCodeClassLoader = libraryCacheManager.getClassLoader(jobID)

      if (userCodeClassLoader == null) {
        throw new RuntimeException("No user code Classloader available.")
      }

      task = new Task(jobID, vertexID, taskIndex, numSubtasks, executionID,
        tdd.getTaskName, this)

      runningTasks.put(executionID, task) match {
        case Some(_) => throw new RuntimeException(
            s"TaskManager contains already a task with executionID ${executionID}.")
        case None =>
      }

      val splitProvider = new TaskInputSplitProvider(currentJobManager, jobID, vertexID,
          executionID, timeout)

      val env = new RuntimeEnvironment(task, tdd, userCodeClassLoader, memoryManager,
          ioManager, splitProvider, currentJobManager, bcVarManager)

      task.setEnvironment(env)

      // register the task with the network stack and profilers
      channelManager match {
          case Some(cm) => cm.register(task)
          case None => throw new RuntimeException("ChannelManager has not been properly " +
            "instantiated.")
        }

        val jobConfig = tdd.getJobConfiguration

        if (jobConfig.getBoolean(ProfilingUtils.PROFILE_JOB_KEY, true)) {
          profiler match {
            case Some(profiler) => profiler ! MonitorTask(task)
            case None => log.info("There is no profiling enabled for the task manager.")
          }
        }

        val cpTasks = new util.HashMap[String, FutureTask[Path]]()

        for (entry <- DistributedCache.readFileInfoFromConfig(tdd.getJobConfiguration).asScala) {
          val cp = fileCache.createTmpFile(entry.getKey, entry.getValue, jobID)
          cpTasks.put(entry.getKey, cp)
        }
        env.addCopyTasksForCacheFile(cpTasks)

        if (!task.startExecution()) {
          throw new RuntimeException("Cannot start task. Task was canceled or failed.")
        }

        sender ! TaskOperationResult(executionID, true)
      } catch {
        case t: Throwable =>
          val message = if(t.isInstanceOf[CancelTaskException]){
            "Task was canceled"
          }else{
            log.error(t, s"Could not instantiate task with execution ID ${executionID}.")
            ExceptionUtils.stringifyException(t)
          }

          try {
            if (task != null) {
              task.failExternally(t)
              removeAllTaskResources(task)
            }

            libraryCacheManager.unregisterTask(jobID, executionID)
          } catch {
            case t: Throwable => log.error("Error during cleanup of task deployment.", t)
          }

          sender ! new TaskOperationResult(executionID, false, message)
      }
  }
  
  private def setupChannelManager(): Unit = {
    //shutdown existing channel manager
    channelManager foreach {
      cm =>
        try {
          cm.shutdown()
        } catch {
          case t: Throwable => log.error(t, "ChannelManager did not shutdown properly.")
        }
    }

    try {
      import networkConnectionConfig._

      val connectionManager: NetworkConnectionManager = networkConnectionConfig match {
        case _: LocalNetworkConfiguration => new LocalConnectionManager
        case ClusterNetworkConfiguration(numBuffers, bufferSize, numInThreads,
        numOutThreads, lowWaterMark, highWaterMark) =>
          new NettyConnectionManager(connectionInfo.address(),
          connectionInfo.dataPort(), bufferSize, numInThreads, numOutThreads, lowWaterMark,
          highWaterMark)
      }

      channelManager = Some(new ChannelManager(currentJobManager, connectionInfo, numBuffers,
        bufferSize, connectionManager, timeout))
    } catch {
      case ioe: IOException =>
        log.error(ioe, "Failed to instantiate ChannelManager.")
        throw new RuntimeException("Failed to instantiate ChannelManager.", ioe)
    }
  }

  private def setupLibraryCacheManager(blobPort: Int): Unit = {
    // shutdown existing library cache manager
    if (libraryCacheManager != null) {
      try {
        libraryCacheManager.shutdown()
      }
      catch {
        case t: Throwable => log.error(t, "Could not properly shut down LibraryCacheManager.")
      }
      libraryCacheManager = null
    }
    
    if (blobPort > 0) {
      val address = new InetSocketAddress(currentJobManager.path.address.host.getOrElse
        ("localhost"), blobPort)
      libraryCacheManager = new BlobLibraryCacheManager(new BlobCache(address), cleanupInterval)
    } else {
      libraryCacheManager = new FallbackLibraryCacheManager
    }
  }

  /**
   * Removes all tasks from this TaskManager.
   */
  private def cancelAndClearEverything(cause: Throwable) {
    if (runningTasks.size > 0) {
      log.info("Cancelling all computations and discarding all cached data.")
      for (t <- runningTasks.values) {
        t.failExternally(cause)
        runningTasks.remove(t.getExecutionId)
      }
    }
  }

  private def unregisterTask(executionID: ExecutionAttemptID): Unit = {
    log.info(s"Unregister task with execution ID ${executionID}.")
    runningTasks.remove(executionID) match {
      case Some(task) =>
        removeAllTaskResources(task)
        libraryCacheManager.unregisterTask(task.getJobID, executionID)
      case None =>
        if(log.isDebugEnabled){
          log.debug(s"Cannot find task with ID ${executionID} to unregister.")
        }
    }
  }

  private def removeAllTaskResources(task: Task): Unit = {
    if(task.getEnvironment != null) {
      try {
        for (entry <- DistributedCache.readFileInfoFromConfig(task.getEnvironment
          .getJobConfiguration).asScala) {
          fileCache.deleteTmpFile(entry.getKey, entry.getValue, task.getJobID)
        }
      }catch{
        case t: Throwable => log.error("Error cleaning up local files from the distributed cache" +
          ".", t)
      }
    }

    channelManager foreach {
      _.unregister(task.getExecutionId, task)
    }

    profiler foreach {
      _ ! UnmonitorTask(task.getExecutionId)
    }

    task.unregisterMemoryManager(memoryManager)
  }
  
  private def logMemoryStats(): Unit = {
    if (log.isDebugEnabled) {
      val memoryMXBean = ManagementFactory.getMemoryMXBean()
      val gcMXBeans = ManagementFactory.getGarbageCollectorMXBeans().asScala
      
      log.debug(TaskManager.getMemoryUsageStatsAsString(memoryMXBean))
      log.debug(TaskManager.getGarbageCollectorStatsAsString(gcMXBeans))
    }
  }
}

/**
 * TaskManager companion object. Contains TaskManager executable entry point, command
 * line parsing, and constants.
 */
object TaskManager {

  val LOG = LoggerFactory.getLogger(classOf[TaskManager])
  val FAILURE_RETURN_CODE = -1

  val TASK_MANAGER_NAME = "taskmanager"
  val PROFILER_NAME = "profiler"
    
  val REGISTRATION_DELAY = 0 seconds
  val REGISTRATION_INTERVAL = 10 seconds
  val MAX_REGISTRATION_ATTEMPTS = 10
  val HEARTBEAT_INTERVAL = 5000 millisecond

  
  def main(args: Array[String]): Unit = {
    val (hostname, port, configuration) = parseArgs(args)

    val (taskManagerSystem, _) = startActorSystemAndActor(hostname, port, configuration)

    taskManagerSystem.awaitTermination()
  }

  def parseArgs(args: Array[String]): (String, Int, Configuration) = {
    val parser = new scopt.OptionParser[TaskManagerCLIConfiguration]("taskmanager") {
      head("flink task manager")
      opt[String]("configDir") action { (x, c) =>
        c.copy(configDir = x)
      } text ("Specify configuration directory.")

      opt[String]("tempDir") optional() action { (x, c) =>
        c.copy(tmpDir = x)
      } text ("Specify temporary directory.")
    }


    parser.parse(args, TaskManagerCLIConfiguration()) map {
      config =>
        GlobalConfiguration.loadConfiguration(config.configDir)

        val configuration = GlobalConfiguration.getConfiguration()

        if (config.tmpDir != null && GlobalConfiguration.getString(ConfigConstants
          .TASK_MANAGER_TMP_DIR_KEY,
          null) == null) {
          configuration.setString(ConfigConstants.TASK_MANAGER_TMP_DIR_KEY, config.tmpDir)
        }

        val jobManagerHostname = configuration.getString(ConfigConstants
          .JOB_MANAGER_IPC_ADDRESS_KEY, null);
        val jobManagerPort = configuration.getInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY,
          ConfigConstants.DEFAULT_JOB_MANAGER_IPC_PORT)

        val jobManagerAddress = new InetSocketAddress(jobManagerHostname, jobManagerPort);

        val port = configuration.getInteger(ConfigConstants.TASK_MANAGER_IPC_PORT_KEY, 0)
        val hostname = NetUtils.resolveAddress(jobManagerAddress).getHostName;

        (hostname, port, configuration)
    } getOrElse {
      LOG.error(s"TaskManager parseArgs called with ${args.mkString(" ")}.")
      LOG.error("CLI parsing failed. Usage: " + parser.usage)
      sys.exit(FAILURE_RETURN_CODE)
    }
  }

  def startActorSystemAndActor(hostname: String, port: Int, configuration: Configuration,
                               localExecution: Boolean = false): (ActorSystem, ActorRef) = {
    implicit val actorSystem = AkkaUtils.createActorSystem(hostname, port, configuration)

    val (connectionInfo, jobManagerURL, taskManagerConfig, networkConnectionConfiguration) =
      parseConfiguration(hostname, configuration, localExecution)

    (actorSystem, startActor(connectionInfo, jobManagerURL, taskManagerConfig,
      networkConnectionConfiguration))
  }

  def parseConfiguration(hostname: String, configuration: Configuration,
                         localExecution: Boolean = false):
  (InstanceConnectionInfo, String, TaskManagerConfiguration, NetworkConnectionConfiguration) = {
    val dataport = configuration.getInteger(ConfigConstants.TASK_MANAGER_DATA_PORT_KEY,
      ConfigConstants.DEFAULT_TASK_MANAGER_DATA_PORT) match {
      case 0 => NetUtils.getAvailablePort
      case x => x
    }

    val connectionInfo = new InstanceConnectionInfo(InetAddress.getByName(hostname), dataport)

    val jobManagerURL = configuration.getString(ConfigConstants.JOB_MANAGER_AKKA_URL, null) match {
      case url: String => url
      case _ =>
        val jobManagerAddress = configuration.getString(ConfigConstants
          .JOB_MANAGER_IPC_ADDRESS_KEY, null);
        val jobManagerRPCPort = configuration.getInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY,
          ConfigConstants.DEFAULT_JOB_MANAGER_IPC_PORT);

        if (jobManagerAddress == null) {
          throw new RuntimeException("JobManager address has not been specified in the " +
            "configuration.")
        }

        JobManager.getRemoteAkkaURL(jobManagerAddress + ":" + jobManagerRPCPort)
    }

    val slots = configuration.getInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, 1)

    val numberOfSlots = if (slots > 0) slots else 1

    val pageSize = configuration.getInteger(ConfigConstants.TASK_MANAGER_NETWORK_BUFFER_SIZE_KEY,
      ConfigConstants.DEFAULT_TASK_MANAGER_NETWORK_BUFFER_SIZE)

    val tmpDirs = configuration.getString(ConfigConstants.TASK_MANAGER_TMP_DIR_KEY,
      ConfigConstants.DEFAULT_TASK_MANAGER_TMP_PATH).split(",|" + File.pathSeparator)

    val numBuffers = configuration.getInteger(ConfigConstants.TASK_MANAGER_NETWORK_NUM_BUFFERS_KEY,
      ConfigConstants.DEFAULT_TASK_MANAGER_NETWORK_NUM_BUFFERS)
    val bufferSize = configuration.getInteger(ConfigConstants.TASK_MANAGER_NETWORK_BUFFER_SIZE_KEY,
      ConfigConstants.DEFAULT_TASK_MANAGER_NETWORK_BUFFER_SIZE)

    val networkConnectionConfiguration = if(localExecution){
      LocalNetworkConfiguration(numBuffers, bufferSize)
    } else {
      val numInThreads = configuration.getInteger(
        ConfigConstants.TASK_MANAGER_NET_NUM_IN_THREADS_KEY,
        ConfigConstants.DEFAULT_TASK_MANAGER_NET_NUM_IN_THREADS)
      val numOutThreads = configuration.getInteger(ConfigConstants
        .TASK_MANAGER_NET_NUM_OUT_THREADS_KEY,
        ConfigConstants.DEFAULT_TASK_MANAGER_NET_NUM_OUT_THREADS)
      val lowWaterMark = configuration.getInteger(ConfigConstants
        .TASK_MANAGER_NET_NETTY_LOW_WATER_MARK,
        ConfigConstants.DEFAULT_TASK_MANAGER_NET_NETTY_LOW_WATER_MARK)
      val highWaterMark = configuration.getInteger(ConfigConstants
        .TASK_MANAGER_NET_NETTY_HIGH_WATER_MARK,
        ConfigConstants.DEFAULT_TASK_MANAGER_NET_NETTY_HIGH_WATER_MARK)

      ClusterNetworkConfiguration(numBuffers, bufferSize, numInThreads, numOutThreads,
        lowWaterMark, highWaterMark)
    }

    val networkBufferMem = if(localExecution) 0 else numBuffers * bufferSize;

    val configuredMemory: Long = configuration.getInteger(ConfigConstants
      .TASK_MANAGER_MEMORY_SIZE_KEY, -1)

    val memorySize = if (configuredMemory > 0) {
      configuredMemory << 20
    } else {
      val fraction = configuration.getFloat(ConfigConstants.TASK_MANAGER_MEMORY_FRACTION_KEY,
        ConfigConstants.DEFAULT_MEMORY_MANAGER_MEMORY_FRACTION)
      ((EnvironmentInformation.getSizeOfFreeHeapMemoryWithDefrag - networkBufferMem ) * fraction)
        .toLong
    }


    val memoryLoggingIntervalMs = configuration.getBoolean(ConfigConstants
      .TASK_MANAGER_DEBUG_MEMORY_USAGE_START_LOG_THREAD,
      ConfigConstants.DEFAULT_TASK_MANAGER_DEBUG_MEMORY_USAGE_START_LOG_THREAD) match {
      case true => Some(
        configuration.getLong(ConfigConstants.TASK_MANAGER_DEBUG_MEMORY_USAGE_LOG_INTERVAL_MS,
          ConfigConstants.DEFAULT_TASK_MANAGER_DEBUG_MEMORY_USAGE_LOG_INTERVAL_MS)
      )
      case false => None
    }

    val profilingInterval = configuration.getBoolean(ProfilingUtils.ENABLE_PROFILING_KEY,
      false) match {
      case true => Some(configuration.getInteger(ProfilingUtils.TASKMANAGER_REPORTINTERVAL_KEY,
        ProfilingUtils.DEFAULT_TASKMANAGER_REPORTINTERVAL).toLong)
      case false => None
    }

    val cleanupInterval = configuration.getLong(ConfigConstants
      .LIBRARY_CACHE_MANAGER_CLEANUP_INTERVAL,
      ConfigConstants.DEFAULT_LIBRARY_CACHE_MANAGER_CLEANUP_INTERVAL) * 1000

    val timeout = FiniteDuration(configuration.getInteger(ConfigConstants.AKKA_ASK_TIMEOUT,
      ConfigConstants.DEFAULT_AKKA_ASK_TIMEOUT), TimeUnit.SECONDS)

    val taskManagerConfig = TaskManagerConfiguration(numberOfSlots, memorySize, pageSize,
      tmpDirs, cleanupInterval, memoryLoggingIntervalMs, profilingInterval, timeout)

    (connectionInfo, jobManagerURL, taskManagerConfig, networkConnectionConfiguration)
  }

  def startActor(connectionInfo: InstanceConnectionInfo, jobManagerURL: String,
                 taskManagerConfig: TaskManagerConfiguration,
                 networkConnectionConfiguration: NetworkConnectionConfiguration)
                (implicit actorSystem: ActorSystem): ActorRef = {
    startActor(Props(new TaskManager(connectionInfo, jobManagerURL, taskManagerConfig,
      networkConnectionConfiguration)))
  }

  def startActor(props: Props)(implicit actorSystem: ActorSystem): ActorRef = {
    actorSystem.actorOf(props, TASK_MANAGER_NAME)
  }

  def startActorWithConfiguration(hostname: String, configuration: Configuration,
                                  localExecution: Boolean = false)
                                 (implicit system: ActorSystem) = {
    val (connectionInfo, jobManagerURL, taskManagerConfig, networkConnectionConfiguration) =
      parseConfiguration(hostname, configuration, localExecution)

    startActor(connectionInfo, jobManagerURL, taskManagerConfig, networkConnectionConfiguration)
  }

  def startProfiler(instancePath: String, reportInterval: Long)(implicit system: ActorSystem):
  ActorRef = {
    system.actorOf(Props(classOf[TaskManagerProfiler], instancePath, reportInterval), PROFILER_NAME)
  }

  def checkTempDirs(tmpDirs: Array[String]): Unit = {
    tmpDirs.zipWithIndex.foreach {
      case (dir: String, _) =>
        val file = new File(dir)

        if (!file.exists) {
          throw new Exception(s"Temporary file directory ${file.getAbsolutePath} does not exist.")
        }
        if (!file.isDirectory) {
          throw new Exception(s"Temporary file directory ${file.getAbsolutePath} is not a " +
            s"directory.")
        }
        if (!file.canWrite) {
          throw new Exception(s"Temporary file directory ${file.getAbsolutePath} is not writable.")
        }
      case (_, id) => throw new Exception(s"Temporary file directory #${id} is null.")

    }
  }

  private def getMemoryUsageStatsAsString(memoryMXBean: MemoryMXBean): String = {
    val heap = memoryMXBean.getHeapMemoryUsage()
    val nonHeap = memoryMXBean.getNonHeapMemoryUsage

    val heapUsed = heap.getUsed >> 20
    val heapCommitted = heap.getCommitted >> 20
    val heapMax = heap.getMax >> 20

    val nonHeapUsed = nonHeap.getUsed >> 20
    val nonHeapCommitted = nonHeap.getCommitted >> 20
    val nonHeapMax = nonHeap.getMax >> 20

    s"Memory usage stats: [HEAP: $heapUsed/$heapCommitted/$heapMax MB, " +
      s"NON HEAP: $nonHeapUsed/$nonHeapCommitted/$nonHeapMax MB (used/committed/max)]"
  }

  private def getGarbageCollectorStatsAsString(gcMXBeans: Iterable[GarbageCollectorMXBean]) 
    : String =
  {
    val beans = gcMXBeans map {
      bean =>
        s"[${bean.getName}, GC TIME (ms): ${bean.getCollectionTime}, " +
          s"GC COUNT: ${bean.getCollectionCount}]"
    } mkString (", ")

    "Garbage collector stats: " + beans
  }
}
