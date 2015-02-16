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

import java.io.{File, IOException}
import java.net.{InetAddress, InetSocketAddress}
import java.util
import java.util.concurrent.{FutureTask, TimeUnit}
import management.{GarbageCollectorMXBean, ManagementFactory, MemoryMXBean}

import akka.actor._
import akka.pattern.ask
import org.apache.flink.api.common.cache.DistributedCache
import org.apache.flink.configuration.{ConfigConstants, Configuration, GlobalConfiguration}
import org.apache.flink.core.fs.Path
import org.apache.flink.runtime.ActorLogMessages
import org.apache.flink.runtime.akka.AkkaUtils
import org.apache.flink.runtime.blob.BlobCache
import org.apache.flink.runtime.broadcast.BroadcastVariableManager
import org.apache.flink.runtime.deployment.{PartitionInfo, TaskDeploymentDescriptor}
import org.apache.flink.runtime.execution.librarycache.{BlobLibraryCacheManager, FallbackLibraryCacheManager, LibraryCacheManager}
import org.apache.flink.runtime.execution.{CancelTaskException, ExecutionState, RuntimeEnvironment}
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID
import org.apache.flink.runtime.filecache.FileCache
import org.apache.flink.runtime.instance.{HardwareDescription, InstanceConnectionInfo, InstanceID}
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync
import org.apache.flink.runtime.io.network.NetworkEnvironment
import org.apache.flink.runtime.io.network.netty.NettyConfig
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID
import org.apache.flink.runtime.jobmanager.JobManager
import org.apache.flink.runtime.memorymanager.DefaultMemoryManager
import org.apache.flink.runtime.messages.JobManagerMessages.UpdateTaskExecutionState
import org.apache.flink.runtime.messages.Messages.Acknowledge
import org.apache.flink.runtime.messages.RegistrationMessages.{AlreadyRegistered,
RefuseRegistration, AcknowledgeRegistration, RegisterTaskManager}
import org.apache.flink.runtime.messages.TaskManagerMessages._
import org.apache.flink.runtime.messages.TaskManagerProfilerMessages
.{UnregisterProfilingListener, UnmonitorTask, MonitorTask, RegisterProfilingListener}
import org.apache.flink.runtime.net.NetUtils
import org.apache.flink.runtime.profiling.ProfilingUtils
import org.apache.flink.runtime.security.SecurityUtils
import org.apache.flink.runtime.security.SecurityUtils.FlinkSecuredRunner
import org.apache.flink.runtime.util.EnvironmentInformation
import org.apache.flink.util.ExceptionUtils
import org.slf4j.LoggerFactory

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Failure, Success}
import scala.language.postfixOps

/**
 * The TaskManager is responsible for executing the individual tasks of a Flink job. It is
 * implemented as an actor. The TaskManager has the following phases:
 *
 * - Waiting to be registered with its JobManager. In that phase, it periodically sends
 * [[RegisterAtJobManager]] messages to itself, which trigger the sending of
 * a [[RegisterTaskManager]] message to the JobManager.
 *
 * - Upon successful registration, the JobManager replies with an [[AcknowledgeRegistration]]
 * message. This stops the registration messages and initializes all fields
 * that require the JobManager's actor reference
 *
 *  - [[SubmitTask]] is sent from the JobManager and contains the next Task to be executed on this
 *  TaskManager
 *
 *  - [[CancelTask]] requests to cancel the corresponding task
 *
 *  - [[FailTask]] requests to fail the corresponding task
 *
 * - ...
 */
class TaskManager(val connectionInfo: InstanceConnectionInfo, val jobManagerAkkaURL: String,
                  val taskManagerConfig: TaskManagerConfiguration,
                  val networkConfig: NetworkEnvironmentConfiguration)
  extends Actor with ActorLogMessages with ActorLogging {

  import context._
  import taskManagerConfig.{timeout => tmTimeout, _}

import scala.collection.JavaConverters._

  implicit val timeout = tmTimeout

  log.info("Starting task manager at {}.", self.path)
  log.info("Creating {} task slot(s).", numberOfSlots)
  log.info("TaskManager connection information {}.", connectionInfo)

  val HEARTBEAT_INTERVAL = 5000 millisecond

  var registrationDelay = 50 milliseconds
  var registrationDuration = 0 seconds

  TaskManager.checkTempDirs(tmpDirPaths)
  val ioManager = new IOManagerAsync(tmpDirPaths)
  val memoryManager = new DefaultMemoryManager(memorySize, numberOfSlots, pageSize)
  val bcVarManager = new BroadcastVariableManager()
  val hardwareDescription = HardwareDescription.extractFromSystem(memoryManager.getMemorySize)
  val fileCache = new FileCache()
  val runningTasks = scala.collection.mutable.HashMap[ExecutionAttemptID, Task]()

  // Actors which want to be notified once this task manager has been registered at the job manager
  val waitForRegistration = scala.collection.mutable.Set[ActorRef]()

  val profiler = profilingInterval match {
    case Some(interval) =>
      log.info("Profiling of jobs is enabled.")
      Some(TaskManager.startProfiler(self.path.toSerializationFormat, interval))
    case None =>
      log.info("Profiling of jobs is disabled.")
      None
  }

  if (log.isInfoEnabled) {
    log.info(TaskManager.getMemoryUsageStatsAsString(ManagementFactory.getMemoryMXBean))
  }

  var libraryCacheManager: LibraryCacheManager = null
  var networkEnvironment: Option[NetworkEnvironment] = None
  var registrationAttempts: Int = 0
  var registered: Boolean = false
  var currentJobManager = ActorRef.noSender
  var profilerListener: Option[ActorRef] = None
  var instanceID: InstanceID = null
  var heartbeatScheduler: Option[Cancellable] = None

  memoryLogggingIntervalMs.foreach {
    interval =>
      val d = FiniteDuration(interval, TimeUnit.MILLISECONDS)
      context.system.scheduler.schedule(d, d, self, LogMemoryUsage)
  }

  override def preStart(): Unit = {
    tryJobManagerRegistration()
  }

  override def postStop(): Unit = {
    log.info("Stopping task manager {}.", self.path)

    cancelAndClearEverything(new Exception("Task Manager is shutting down."))

    heartbeatScheduler foreach {
      _.cancel()
    }

    networkEnvironment foreach {
      ne =>
        try {
          ne.shutdown()
        } catch {
          case t: Throwable => log.error(t, "ChannelManager did not shutdown properly.")
        }
    }

    ioManager.shutdown()
    memoryManager.shutdown()

    try {
      fileCache.shutdown()
    } catch {
      case t: Throwable => log.error(t, "FileCache did not shutdown properly.")
    }

    if (libraryCacheManager != null) {
      try {
        libraryCacheManager.shutdown()
      }
      catch {
        case t: Throwable => log.error(t, "LibraryCacheManager did not shutdown properly.")
      }
    }

    if(log.isDebugEnabled){
      log.debug("Task manager {} is completely stopped.", self.path)
    }
  }

  private def tryJobManagerRegistration(): Unit = {
    registrationDuration = 0 seconds

    registered = false
    currentJobManager = ActorRef.noSender

    context.system.scheduler.scheduleOnce(registrationDelay, self, RegisterAtJobManager)
  }

  override def receiveWithLogMessages: Receive = {
    case RegisterAtJobManager =>
      if(!registered) {
        registrationDuration += registrationDelay
        // double delay for exponential backoff
        registrationDelay *= 2

        if (registrationDuration > maxRegistrationDuration) {
          log.warning("TaskManager could not register at JobManager {} after {}.",
            jobManagerAkkaURL,
            maxRegistrationDuration)

          self ! PoisonPill
        } else {
          log.info("Try to register at master {}. {}. Attempt", jobManagerAkkaURL,
            registrationAttempts)
          val jobManager = context.actorSelection(jobManagerAkkaURL)

          jobManager ! RegisterTaskManager(connectionInfo, hardwareDescription, numberOfSlots)

          context.system.scheduler.scheduleOnce(registrationDelay, self, RegisterAtJobManager)
        }
      }

    case AcknowledgeRegistration(id, blobPort, profilerListener) =>
      if(!registered) {
        finishRegistration(sender, id, blobPort, profilerListener)
      } else {
        log.info("The TaskManager {} is already registered at the JobManager {}, but received " +
          "another AcknowledgeRegistration message.", self.path, currentJobManager.path)
      }

    case AlreadyRegistered(id, blobPort, profilerListener) =>
      if(!registered) {
        log.warning("The TaskManager {} seems to be already registered at the JobManager {} even" +
          "though it has not yet finished the registration process.", self.path, sender.path)

        finishRegistration(sender, id, blobPort, profilerListener)
      } else {
        // ignore AlreadyRegistered messages which arrived after AcknowledgeRegistration
        log.info("The TaskManager {} has already been registered at the JobManager {}.",
          self.path, sender.path)
      }

    case RefuseRegistration(reason) =>
      if(!registered) {
        log.error("The registration of task manager {} was refused by the job manager {} " +
          "because {}.", self.path, jobManagerAkkaURL, reason)

        // Shut task manager down
        self ! PoisonPill
      } else {
        // ignore RefuseRegistration messages which arrived after AcknowledgeRegistration
        log.info("Received RefuseRegistration from the JobManager even though being already " +
          "registered")
      }

    case SubmitTask(tdd) =>
      submitTask(tdd)

    case updateMsg:UpdateTask =>
      updateMsg match {
        case UpdateTaskSinglePartitionInfo(executionID, resultID, partitionInfo) =>
          updateTask(executionID, List((resultID, partitionInfo)))
        case UpdateTaskMultiplePartitionInfos(executionID, partitionInfos) =>
          updateTask(executionID, partitionInfos)
      }

    case CancelTask(executionID) =>
      runningTasks.get(executionID) match {
        case Some(task) =>
          // execute cancel operation concurrently
          Future {
            task.cancelExecution()
          }.onFailure{
            case t: Throwable => log.error(t, "Could not cancel task {}.", task)
          }

          sender ! new TaskOperationResult(executionID, true)
        case None =>
          sender ! new TaskOperationResult(executionID, false,
            "No task with that execution ID was found.")
      }

    case UnregisterTask(executionID) =>
      unregisterTask(executionID)

    case updateMsg:UpdateTaskExecutionState =>
      val futureResponse = (currentJobManager ? updateMsg)(timeout)

      val jobID = updateMsg.taskExecutionState.getJobID
      val executionID = updateMsg.taskExecutionState.getID
      val executionState = updateMsg.taskExecutionState.getExecutionState

      futureResponse.mapTo[Boolean].onComplete{
        case Success(result) =>
          if(!result){
            self ! FailTask(executionID,
              new IllegalStateException("Task has been disposed on JobManager."))
          }

          if (!result || executionState == ExecutionState.FINISHED || executionState ==
            ExecutionState.CANCELED || executionState == ExecutionState.FAILED) {
            self ! UnregisterTask(executionID)
          }
        case Failure(t) =>
          log.warning(s"Execution state change notification failed for task $executionID " +
            s"of job $jobID. Cause ${t.getMessage}.")
          self ! UnregisterTask(executionID)
      }

    case SendHeartbeat =>
      currentJobManager ! Heartbeat(instanceID)

    case LogMemoryUsage =>
      logMemoryStats()

    case SendStackTrace =>
      val traces = Thread.getAllStackTraces.asScala
      val stackTraceStr = traces.map((trace: (Thread, Array[StackTraceElement])) => {
        val (thread, elements) = trace
        "Thread: " + thread.getName + '\n' + elements.mkString("\n")
      }).mkString("\n\n")

      sender ! StackTrace(instanceID, stackTraceStr)

    case NotifyWhenRegisteredAtJobManager =>
      if (registered) {
        sender ! RegisteredAtJobManager
      } else {
        waitForRegistration += sender
      }

    case FailTask(executionID, cause) =>
      runningTasks.get(executionID) match {
        case Some(task) =>
          // execute failing operation concurrently
          Future {
            task.failExternally(cause)
          }.onFailure{
            case t: Throwable => log.error(t, "Could not fail task {} externally.", task)
          }
        case None =>
      }

    case Terminated(jobManager) =>
      log.info("Job manager {} is no longer reachable. Cancelling all tasks and trying to " +
        "reregister.", jobManager.path)

      cancelAndClearEverything(new Throwable("Lost connection to JobManager"))

      cleanupTaskManager()

      tryJobManagerRegistration()

    case FailIntermediateResultPartitions(executionID) =>
      log.info("Fail intermediate result partitions associated with execution {}.", executionID)
      networkEnvironment foreach {
        _.getPartitionManager.failIntermediateResultPartitions(executionID)
      }
  }

  /**
   * Handle unmatched messages with an exception.
   */
  override def unhandled(message: Any): Unit = {
    // let the actor crash
    throw new RuntimeException("Received unknown message " + message)
  }

  /**
   * Receives a [[TaskDeploymentDescriptor]] describing the task to be executed. Sets up a
   * [[RuntimeEnvironment]] for the task and starts its execution in a separate thread.
   *
   * @param tdd TaskDeploymentDescriptor describing the task to be executed on this [[TaskManager]]
   */
  private def submitTask(tdd: TaskDeploymentDescriptor): Unit = {
    val jobID = tdd.getJobID
    val vertexID = tdd.getVertexID
    val executionID = tdd.getExecutionId
    val taskIndex = tdd.getIndexInSubtaskGroup
    val numSubtasks = tdd.getNumberOfSubtasks
    var startRegisteringTask = 0L
    var task: Task = null

    try {
      if (log.isDebugEnabled) {
        startRegisteringTask = System.currentTimeMillis()
      }
      libraryCacheManager.registerTask(jobID, executionID, tdd.getRequiredJarFiles)
      // triggers the download of all missing jar files from the job manager
      libraryCacheManager.registerTask(jobID, executionID, tdd.getRequiredJarFiles)

      if (log.isDebugEnabled) {
        log.debug("Register task {} took {}s", executionID,
          (System.currentTimeMillis() - startRegisteringTask) / 1000.0)
      }

      val userCodeClassLoader = libraryCacheManager.getClassLoader(jobID)

      if (userCodeClassLoader == null) {
        throw new RuntimeException("No user code Classloader available.")
      }

      task = new Task(jobID, vertexID, taskIndex, numSubtasks, executionID,
        tdd.getTaskName, self)

      runningTasks.put(executionID, task) match {
        case Some(_) => throw new RuntimeException(
          s"TaskManager contains already a task with executionID $executionID.")
        case None =>
      }

      val splitProvider = new TaskInputSplitProvider(currentJobManager, jobID, vertexID,
        executionID, userCodeClassLoader, timeout)

      val env = new RuntimeEnvironment(currentJobManager, task, tdd, userCodeClassLoader,
        memoryManager, ioManager, splitProvider, bcVarManager, networkEnvironment.get)

      task.setEnvironment(env)

      // register the task with the network stack and profiles
      networkEnvironment match {
        case Some(ne) => ne.registerTask(task)
        case None => throw new RuntimeException(
          "Network environment has not been properly instantiated.")
      }

      val jobConfig = tdd.getJobConfiguration

      if (jobConfig.getBoolean(ProfilingUtils.PROFILE_JOB_KEY, true)) {
        profiler match {
          case Some(profilerActorRef) => profilerActorRef ! MonitorTask(task)
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

      sender ! TaskOperationResult(executionID, success = true)
    } catch {
      case t: Throwable =>
        val message = if (t.isInstanceOf[CancelTaskException]) {
          "Task was canceled"
        } else {
          log.error(t, "Could not instantiate task with execution ID {}.", executionID)
          ExceptionUtils.stringifyException(t)
        }

        try {
          if (task != null) {
            task.failExternally(t)
            removeAllTaskResources(task)
          }

          libraryCacheManager.unregisterTask(jobID, executionID)
        } catch {
          case t: Throwable => log.error(t, "Error during cleanup of task deployment.")
        }

        sender ! new TaskOperationResult(executionID, false, message)
    }
  }

  private def cleanupTaskManager(): Unit = {
    context.unwatch(currentJobManager)

    networkEnvironment foreach {
      _.shutdown()
    }

    networkEnvironment = None

    if(libraryCacheManager != null){
      try {
        libraryCacheManager.shutdown()
      } catch {
        case t: Throwable => log.error(t, "Could not shut down the library cache manager.")
      }
    }

    libraryCacheManager = null

    heartbeatScheduler foreach {
      _.cancel()
    }

    heartbeatScheduler = None

    profilerListener foreach {
      listener =>
        profiler foreach {
          _.tell(UnregisterProfilingListener, listener)
        }
    }

    profilerListener = None
    currentJobManager = ActorRef.noSender
    instanceID = null
    registered = false
  }

  private def updateTask(executionId: ExecutionAttemptID,
                         partitionInfos: Seq[(IntermediateDataSetID, PartitionInfo)]): Unit = {

    runningTasks.get(executionId) match {
      case Some(task) =>
        val errors = partitionInfos flatMap {
          case (resultID, partitionInfo) =>
            Option(task.getEnvironment.getReaderById(resultID)) match {
              case Some(reader) =>
                Future {
                  try {
                    reader.updateInputChannel(partitionInfo)
                  } catch {
                    case t: Throwable =>
                      log.error(t, "Task update failure. Trying to cancel task.")

                      try {
                        task.cancelExecution()
                      } catch {
                        case t: Throwable =>
                          log.error(t, "Failed canceling task with execution ID {} after task" +
                            "update failure..", executionId)
                      }
                  }
                }
                None
              case None => Some(s"No reader with ID $resultID for task $executionId was found.")
            }
        }

        if(errors.isEmpty) {
          sender ! Acknowledge
        } else {
          sender ! Failure(new IllegalStateException(errors.mkString("\n")))
        }
      case None =>
        log.info("Could not update task with ID {}, because it is no longer running.",
          executionId)
        sender ! Acknowledge
    }
  }

  private def finishRegistration(jobManager: ActorRef, id: InstanceID, blobPort: Int,
                                  profilerListener: Option[ActorRef]): Unit = {
    setupTaskManager(jobManager, id, blobPort, profilerListener)

    for (listener <- waitForRegistration) {
      listener ! RegisteredAtJobManager
    }

    waitForRegistration.clear()
  }

  private def setupTaskManager(jobManager: ActorRef, id: InstanceID, blobPort: Int,
                                profilerListener: Option[ActorRef]): Unit = {
    registered = true
    currentJobManager = jobManager
    this.profilerListener = profilerListener
    instanceID = id

    // watch job manager to detect when it dies
    context.watch(currentJobManager)

    setupNetworkEnvironment()
    setupLibraryCacheManager(blobPort)

    // schedule regular heartbeat message for oneself
    heartbeatScheduler = Some(context.system.scheduler.schedule(
      TaskManager.HEARTBEAT_INTERVAL, TaskManager.HEARTBEAT_INTERVAL, self, SendHeartbeat))

    profilerListener foreach {
      listener =>
        profiler foreach {
          _.tell(RegisterProfilingListener, listener)
        }
    }
  }

  private def setupNetworkEnvironment(): Unit = {
    //shutdown existing network environment
    networkEnvironment foreach {
      ne =>
        try {
          ne.shutdown()
        } catch {
          case t: Throwable => log.error(t, "Network environment did not shutdown properly.")
        }
    }

    try {
      networkEnvironment = Some(new NetworkEnvironment(self, currentJobManager, timeout,
        networkConfig))
    } catch {
      case ioe: IOException =>
        log.error(ioe, "Failed to instantiate network environment.")
        throw new RuntimeException("Failed to instantiate ChannelManager.", ioe)
    }
  }

  private def setupLibraryCacheManager(blobPort: Int): Unit = {
    // shutdown existing library cache manager first
    if (libraryCacheManager != null) {
      try {
        libraryCacheManager.shutdown()
      }
      catch {
        case t: Throwable => log.error(t, "Could not properly shut down LibraryCacheManager.")
      }
      libraryCacheManager = null
    }

    // Check if a blob server is specified
    if (blobPort > 0) {
      val address = new InetSocketAddress(currentJobManager.path.address.host.getOrElse
        ("localhost"), blobPort)

      log.info("Determined BLOB server address to be {}.", address)

      libraryCacheManager = new BlobLibraryCacheManager(
                                     new BlobCache(address, configuration), cleanupInterval)
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
    log.info("Unregister task with execution ID {}.", executionID)

    runningTasks.remove(executionID) match {
      case Some(task) =>
        removeAllTaskResources(task)
        libraryCacheManager.unregisterTask(task.getJobID, executionID)
      case None =>
        if (log.isDebugEnabled) {
          log.debug("Cannot find task with ID {} to unregister.", executionID)
        }
    }
  }

  private def removeAllTaskResources(task: Task): Unit = {
    if (task.getEnvironment != null) {
      try {
        for (entry <- DistributedCache.readFileInfoFromConfig(
          task.getEnvironment.getJobConfiguration).asScala) {
          fileCache.deleteTmpFile(entry.getKey, entry.getValue, task.getJobID)
        }
      } catch {
        case t: Throwable => log.error("Error cleaning up local files from the distributed cache" +
          ".", t)
      }
    }

    networkEnvironment foreach {
      _.unregisterTask(task)
    }

    profiler foreach {
      _ ! UnmonitorTask(task.getExecutionId)
    }

    task.unregisterMemoryManager(memoryManager)
  }

  private def logMemoryStats(): Unit = {
    if (log.isInfoEnabled) {
      val memoryMXBean = ManagementFactory.getMemoryMXBean
      val gcMXBeans = ManagementFactory.getGarbageCollectorMXBeans.asScala

      log.info(TaskManager.getMemoryUsageStatsAsString(memoryMXBean))
      log.info(TaskManager.getGarbageCollectorStatsAsString(gcMXBeans))
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
    EnvironmentInformation.logEnvironmentInfo(LOG, "TaskManager")
    val (hostname, port, configuration) = parseArgs(args)

    if(SecurityUtils.isSecurityEnabled) {
      LOG.info("Security is enabled. Starting secure TaskManager.")
      SecurityUtils.runSecured(new FlinkSecuredRunner[Unit] {
        override def run(): Unit = {
          startActor(hostname, port, configuration, TaskManager.TASK_MANAGER_NAME)
        }
      })
    } else {
      startActor(hostname, port, configuration, TaskManager.TASK_MANAGER_NAME)
    }
  }

  def startActor(hostname: String, port: Int, configuration: Configuration,
                 taskManagerName: String) : Unit = {

    val (taskManagerSystem, _) = startActorSystemAndActor(hostname, port, configuration,
      taskManagerName, localAkkaCommunication = false, localTaskManagerCommunication = false)

    taskManagerSystem.awaitTermination()
  }

  /**
   * Parse the command line arguments of the [[TaskManager]]. The method loads the configuration,
   * extracts the hostname and port on which the actor system shall listen.
   *
   * @param args Command line arguments
   * @return Tuple of (hostname, port, configuration)
   */
  def parseArgs(args: Array[String]): (String, Int, Configuration) = {
    val parser = new scopt.OptionParser[TaskManagerCLIConfiguration]("taskmanager") {
      head("flink task manager")
      opt[String]("configDir") action { (x, c) =>
        c.copy(configDir = x)
      } text "Specify configuration directory."

      opt[String]("tempDir") optional() action { (x, c) =>
        c.copy(tmpDir = x)
      } text "Specify temporary directory."
    }


    parser.parse(args, TaskManagerCLIConfiguration()) map {
      config =>
        GlobalConfiguration.loadConfiguration(config.configDir)

        val configuration = GlobalConfiguration.getConfiguration

        if (config.tmpDir != null && GlobalConfiguration.getString(ConfigConstants
          .TASK_MANAGER_TMP_DIR_KEY,
          null) == null) {
          configuration.setString(ConfigConstants.TASK_MANAGER_TMP_DIR_KEY, config.tmpDir)
        }

        val jobManagerHostname = configuration.getString(
          ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, null)

        val jobManagerPort = configuration.getInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY,
          ConfigConstants.DEFAULT_JOB_MANAGER_IPC_PORT)

        val jobManagerAddress = new InetSocketAddress(jobManagerHostname, jobManagerPort)

        val port = configuration.getInteger(ConfigConstants.TASK_MANAGER_IPC_PORT_KEY, 0)
        // try to find out the TaskManager's own hostname by connecting to jobManagerAddress
        val hostname = NetUtils.resolveAddress(jobManagerAddress).getHostName

        (hostname, port, configuration)
    } getOrElse {
      LOG.error(s"TaskManager parseArgs called with ${args.mkString(" ")}.")
      LOG.error("CLI parsing failed. Usage: " + parser.usage)
      sys.exit(FAILURE_RETURN_CODE)
    }
  }

  def startActorSystemAndActor(hostname: String, port: Int, configuration: Configuration,
                               taskManagerName: String,
                               localAkkaCommunication: Boolean,
                               localTaskManagerCommunication: Boolean): (ActorSystem, ActorRef) = {
    implicit val actorSystem = AkkaUtils.createActorSystem(configuration, Some((hostname, port)))

    val (connectionInfo, jobManagerURL, taskManagerConfig, networkConfig) =
      parseConfiguration(hostname, configuration, localAkkaCommunication,
        localTaskManagerCommunication)

    (actorSystem, startActor(taskManagerName, connectionInfo, jobManagerURL, taskManagerConfig,
      networkConfig))
  }

  /**
   * Extracts from the configuration the TaskManager's settings. Returns the TaskManager's
   * connection information, the JobManager's Akka URL, the task manager configuration and the
   * network connection configuration.
   *
   * @param hostname Hostname of the instance on which the TaskManager runs
   * @param configuration Configuration instance containing the user provided configuration values
   * @param localAkkaCommunication true if the TaskManager runs in the same [[ActorSystem]] as the
   *                               JobManager, otherwise false
   * @param localTaskManagerCommunication true if all TaskManager run in the same JVM, otherwise
   *                                      false
   * @return Tuple of (TaskManager's connection information, JobManager's Akka URL, TaskManager's
   *         configuration, network connection configuration)
   */
  def parseConfiguration(hostname: String, configuration: Configuration,
                         localAkkaCommunication: Boolean,
                         localTaskManagerCommunication: Boolean):
  (InstanceConnectionInfo, String, TaskManagerConfiguration, NetworkEnvironmentConfiguration) = {
    val dataport = configuration.getInteger(ConfigConstants.TASK_MANAGER_DATA_PORT_KEY,
      ConfigConstants.DEFAULT_TASK_MANAGER_DATA_PORT) match {
      case 0 => NetUtils.getAvailablePort
      case x => x
    }

    val connectionInfo = new InstanceConnectionInfo(InetAddress.getByName(hostname), dataport)

    val jobManagerURL = if (localAkkaCommunication) {
      // JobManager and TaskManager are in the same ActorSystem -> Use local Akka URL
      JobManager.getLocalJobManagerAkkaURL
    }
    else {
      val jobManagerAddress = configuration.getString(
        ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, null)

      val jobManagerRPCPort = configuration.getInteger(
        ConfigConstants.JOB_MANAGER_IPC_PORT_KEY,
        ConfigConstants.DEFAULT_JOB_MANAGER_IPC_PORT)

      if (jobManagerAddress == null) {
        throw new RuntimeException(
          "JobManager address has not been specified in the configuration.")
      }

      val hostPort = new InetSocketAddress(InetAddress.getByName(jobManagerAddress),
                                           jobManagerRPCPort)
      JobManager.getRemoteJobManagerAkkaURL(hostPort)
    }

    val slots = configuration.getInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, 1)

    val numberOfSlots = if (slots > 0) slots else 1

    val pageSize = configuration.getInteger(ConfigConstants.TASK_MANAGER_NETWORK_BUFFER_SIZE_KEY,
      ConfigConstants.DEFAULT_TASK_MANAGER_NETWORK_BUFFER_SIZE)

    val tmpDirs = configuration.getString(ConfigConstants.TASK_MANAGER_TMP_DIR_KEY,
      ConfigConstants.DEFAULT_TASK_MANAGER_TMP_PATH).split(",|" + File.pathSeparator)

    val numNetworkBuffers = configuration.getInteger(
      ConfigConstants.TASK_MANAGER_NETWORK_NUM_BUFFERS_KEY,
      ConfigConstants.DEFAULT_TASK_MANAGER_NETWORK_NUM_BUFFERS)

    val nettyConfig = localTaskManagerCommunication match {
      case true => None
      case false => Some(new NettyConfig(
        connectionInfo.address(), connectionInfo.dataPort(), pageSize, configuration))
    }

    val networkConfig = NetworkEnvironmentConfiguration(numNetworkBuffers, pageSize, nettyConfig)

    val networkBufferMem = if (localTaskManagerCommunication) 0 else numNetworkBuffers * pageSize

    val configuredMemory: Long = configuration.getInteger(
      ConfigConstants.TASK_MANAGER_MEMORY_SIZE_KEY, -1
    )

    val memorySize = if (configuredMemory > 0) {
      configuredMemory << 20
    } else {
      val fraction = configuration.getFloat(ConfigConstants.TASK_MANAGER_MEMORY_FRACTION_KEY,
        ConfigConstants.DEFAULT_MEMORY_MANAGER_MEMORY_FRACTION)

      LOG.info("Using {} of the free heap space for managed memory.", fraction)

      ((EnvironmentInformation.getSizeOfFreeHeapMemoryWithDefrag - networkBufferMem) * fraction)
        .toLong
    }

    val memoryLoggingIntervalMs = configuration.getBoolean(
      ConfigConstants.TASK_MANAGER_DEBUG_MEMORY_USAGE_START_LOG_THREAD,
      ConfigConstants.DEFAULT_TASK_MANAGER_DEBUG_MEMORY_USAGE_START_LOG_THREAD
    ) match {
      case true => Some(
        configuration.getLong(ConfigConstants.TASK_MANAGER_DEBUG_MEMORY_USAGE_LOG_INTERVAL_MS,
          ConfigConstants.DEFAULT_TASK_MANAGER_DEBUG_MEMORY_USAGE_LOG_INTERVAL_MS)
      )
      case false => None
    }

    val profilingInterval = configuration.getBoolean(
      ProfilingUtils.ENABLE_PROFILING_KEY, false
    ) match {
      case true => Some(configuration.getInteger(ProfilingUtils.TASKMANAGER_REPORTINTERVAL_KEY,
        ProfilingUtils.DEFAULT_TASKMANAGER_REPORTINTERVAL).toLong)
      case false => None
    }

    val cleanupInterval = configuration.getLong(
      ConfigConstants.LIBRARY_CACHE_MANAGER_CLEANUP_INTERVAL,
      ConfigConstants.DEFAULT_LIBRARY_CACHE_MANAGER_CLEANUP_INTERVAL) * 1000

    val timeout = AkkaUtils.getTimeout(configuration)

    val maxRegistrationDuration = Duration(configuration.getString(
      ConfigConstants.TASK_MANAGER_MAX_REGISTRATION_DURATION,
      ConfigConstants.DEFAULT_TASK_MANAGER_MAX_REGISTRATION_DURATION))

    val taskManagerConfig = TaskManagerConfiguration(numberOfSlots, memorySize, pageSize,
      tmpDirs, cleanupInterval, memoryLoggingIntervalMs, profilingInterval, timeout,
      maxRegistrationDuration, configuration)

    (connectionInfo, jobManagerURL, taskManagerConfig, networkConfig)
  }

  def startActor(taskManagerName: String,
                 connectionInfo: InstanceConnectionInfo,
                 jobManagerURL: String,
                 taskManagerConfig: TaskManagerConfiguration,
                 networkConfig: NetworkEnvironmentConfiguration)
                (implicit actorSystem: ActorSystem): ActorRef = {
    startActor(taskManagerName,
      Props(new TaskManager(connectionInfo, jobManagerURL, taskManagerConfig, networkConfig)))
  }

  def startActor(taskManagerName: String, props: Props)
                (implicit actorSystem: ActorSystem): ActorRef = {
    actorSystem.actorOf(props, taskManagerName)
  }

  def startActorWithConfiguration(hostname: String, taskManagerName: String,
                                  configuration: Configuration,
                                  localAkkaCommunication: Boolean,
                                  localTaskManagerCommunication: Boolean)
                                 (implicit system: ActorSystem) = {
    val (connectionInfo, jobManagerURL, taskManagerConfig, networkConnectionConfiguration) =
      parseConfiguration(hostname, configuration, localAkkaCommunication,
        localTaskManagerCommunication)

    startActor(taskManagerName, connectionInfo, jobManagerURL, taskManagerConfig,
      networkConnectionConfiguration)
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

        if (LOG.isInfoEnabled) {
          val totalSpaceGb = file.getTotalSpace >>  30
          val usableSpaceGb = file.getUsableSpace >> 30
          val usablePercentage = usableSpaceGb.asInstanceOf[Double] / totalSpaceGb * 100

          val path = file.getAbsolutePath

          LOG.info(f"Temporary file directory '$path': total $totalSpaceGb GB, " +
            f"usable $usableSpaceGb GB ($usablePercentage%.2f%% usable)")
        }
      case (_, id) => throw new Exception(s"Temporary file directory #$id is null.")

    }
  }

  private def getMemoryUsageStatsAsString(memoryMXBean: MemoryMXBean): String = {
    val heap = memoryMXBean.getHeapMemoryUsage
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
  : String = {
    val beans = gcMXBeans map {
      bean =>
        s"[${bean.getName}, GC TIME (ms): ${bean.getCollectionTime}, " +
          s"GC COUNT: ${bean.getCollectionCount}]"
    } mkString ", "

    "Garbage collector stats: " + beans
  }
}
