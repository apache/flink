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
import java.util.concurrent.{TimeUnit, FutureTask}
import management.{GarbageCollectorMXBean, ManagementFactory, MemoryMXBean}

import akka.actor._
import akka.pattern.ask
import com.codahale.metrics.{Gauge, MetricFilter, MetricRegistry}
import com.codahale.metrics.json.MetricsModule
import com.codahale.metrics.jvm.{MemoryUsageGaugeSet, GarbageCollectorMetricSet}
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.flink.api.common.cache.DistributedCache
import org.apache.flink.configuration.{ConfigConstants, Configuration, GlobalConfiguration}
import org.apache.flink.core.fs.Path
import org.apache.flink.runtime.ActorLogMessages
import org.apache.flink.runtime.akka.AkkaUtils
import org.apache.flink.runtime.blob.BlobCache
import org.apache.flink.runtime.broadcast.BroadcastVariableManager
import org.apache.flink.runtime.deployment.{InputChannelDeploymentDescriptor, TaskDeploymentDescriptor}
import org.apache.flink.runtime.execution.librarycache.{BlobLibraryCacheManager, FallbackLibraryCacheManager, LibraryCacheManager}
import org.apache.flink.runtime.execution.{CancelTaskException, ExecutionState, RuntimeEnvironment}
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID
import org.apache.flink.runtime.filecache.FileCache
import org.apache.flink.runtime.instance.{HardwareDescription, InstanceConnectionInfo, InstanceID}
import org.apache.flink.runtime.io.disk.iomanager.IOManager.IOMode
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync
import org.apache.flink.runtime.io.network.NetworkEnvironment
import org.apache.flink.runtime.io.network.netty.NettyConfig
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID
import org.apache.flink.runtime.jobgraph.tasks.{OperatorStateCarrier,BarrierTransceiver}
import org.apache.flink.runtime.jobmanager.{BarrierReq,JobManager}
import org.apache.flink.runtime.memorymanager.DefaultMemoryManager
import org.apache.flink.runtime.messages.JobManagerMessages.UpdateTaskExecutionState
import org.apache.flink.runtime.messages.Messages.{Disconnect, Acknowledge}
import org.apache.flink.runtime.messages.RegistrationMessages.{AlreadyRegistered, RefuseRegistration, AcknowledgeRegistration, RegisterTaskManager}
import org.apache.flink.runtime.messages.TaskManagerMessages._
import org.apache.flink.runtime.messages.TaskManagerProfilerMessages.{UnregisterProfilingListener, UnmonitorTask, MonitorTask, RegisterProfilingListener}
import org.apache.flink.runtime.net.NetUtils
import org.apache.flink.runtime.process.ProcessReaper
import org.apache.flink.runtime.profiling.ProfilingUtils
import org.apache.flink.runtime.security.SecurityUtils
import org.apache.flink.runtime.security.SecurityUtils.FlinkSecuredRunner
import org.apache.flink.runtime.util.{MathUtils, EnvironmentInformation}
import org.apache.flink.util.ExceptionUtils
import org.slf4j.LoggerFactory

import scala.concurrent._
import scala.concurrent.duration._
import scala.util.{Failure, Success}
import scala.collection.JavaConverters._

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
class TaskManager(val connectionInfo: InstanceConnectionInfo,
                  val jobManagerAkkaURL: String,
                  val taskManagerConfig: TaskManagerConfiguration,
                  val networkConfig: NetworkEnvironmentConfiguration)
  extends Actor with ActorLogMessages with ActorLogging {

  import context._
  import taskManagerConfig.{timeout => tmTimeout, _}



  implicit val timeout = tmTimeout

  log.info("Starting task manager at {}.", self.path)
  log.info("Creating {} task slot(s).", numberOfSlots)
  log.info("TaskManager connection information: {}", connectionInfo)

  val HEARTBEAT_INTERVAL = 5000 millisecond

  var registrationDelay = 50 milliseconds
  var registrationDuration = 0 seconds
  var registrationAttempts: Int = 0

  val ioManager = new IOManagerAsync(tmpDirPaths)
  val memoryManager = new DefaultMemoryManager(memorySize, numberOfSlots, pageSize)
  val bcVarManager = new BroadcastVariableManager()
  val hardwareDescription = HardwareDescription.extractFromSystem(memoryManager.getMemorySize)
  val fileCache = new FileCache()
  val runningTasks = scala.collection.mutable.HashMap[ExecutionAttemptID, Task]()
  val metricRegistry = new MetricRegistry
  // register metrics
  metricRegistry.register("gc", new GarbageCollectorMetricSet)
  metricRegistry.register("memory", new MemoryUsageGaugeSet)
  metricRegistry.register("load", new Gauge[Double] {
    override def getValue: Double =
      ManagementFactory.getOperatingSystemMXBean().getSystemLoadAverage()
  })
  // register metric serialization
  val metricRegistryMapper: ObjectMapper =
    new ObjectMapper().registerModule(new MetricsModule(TimeUnit.SECONDS, TimeUnit.MILLISECONDS,
      false, MetricFilter.ALL))

  // Actors which want to be notified once this task manager has been registered at the job manager
  val waitForRegistration = scala.collection.mutable.Set[ActorRef]()

  val profiler = profilingInterval match {
    case Some(interval) =>
      log.info("Profiling of jobs is enabled.")
      Some(TaskManager.startProfiler(self.path.toSerializationFormat, interval, context.system))
    case None =>
      log.info("Profiling of jobs is disabled.")
      None
  }

  if (log.isInfoEnabled) {
    log.info(TaskManager.getMemoryUsageStatsAsString(ManagementFactory.getMemoryMXBean))
  }

  var libraryCacheManager: Option[LibraryCacheManager] = None
  var networkEnvironment: Option[NetworkEnvironment] = None
  var currentJobManager: Option[ActorRef] = None
  var profilerListener: Option[ActorRef] = None
  var instanceID: InstanceID = null
  var heartbeatScheduler: Option[Cancellable] = None


  override def preStart(): Unit = {
    tryJobManagerRegistration()
  }

  override def postStop(): Unit = {
    log.info("Stopping task manager {}.", self.path)

    currentJobManager foreach {
      _ ! Disconnect(s"TaskManager ${self.path} is shutting down.")
    }

    cancelAndClearEverything(new Exception("Task Manager is shutting down."))

    cleanupTaskManager()

    ioManager.shutdown()
    memoryManager.shutdown()

    try {
      fileCache.shutdown()
    } catch {
      case t: Throwable => log.error(t, "FileCache did not shutdown properly.")
    }

    if(log.isDebugEnabled){
      log.debug("Task manager {} is completely stopped.", self.path)
    }
  }
  
  private def tryJobManagerRegistration(): Unit = {
    context.system.scheduler.scheduleOnce(registrationDelay, self, RegisterAtJobManager)
  }

  override def receiveWithLogMessages: Receive = {
    case RegisterAtJobManager =>
      if(currentJobManager.isEmpty) {
        registrationDuration += registrationDelay
        // double delay for exponential backoff
        registrationDelay *= 2
        registrationAttempts += 1

        if (registrationDuration > maxRegistrationDuration) {
          log.error("TaskManager could not register at JobManager {} after {}.",
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
      if(currentJobManager.isEmpty) {
        finishRegistration(sender, id, blobPort, profilerListener)
      } else {
        log.info("The TaskManager {} is already registered at the JobManager {}, but received " +
          "another AcknowledgeRegistration message.", self.path, sender.path)
      }

    case AlreadyRegistered(id, blobPort, profilerListener) =>
      if(currentJobManager.isEmpty) {
        log.warning("The TaskManager {} seems to be already registered at the JobManager {} even" +
          "though it has not yet finished the registration process.", self.path, sender.path)

        finishRegistration(sender, id, blobPort, profilerListener)
      } else {
        // ignore AlreadyRegistered messages which arrived after AcknowledgeRegistration
        log.info("The TaskManager {} has already been registered at the JobManager {}.",
          self.path, sender.path)
      }

    case RefuseRegistration(reason) =>
      if(currentJobManager.isEmpty) {
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
      unregisterTaskAndNotifyFinalState(executionID)

    case updateMsg:UpdateTaskExecutionState =>
      currentJobManager foreach {
        jobManager => {
          val futureResponse = (jobManager ? updateMsg)(timeout)

          val jobID = updateMsg.taskExecutionState.getJobID
          val executionID = updateMsg.taskExecutionState.getID
          val executionState = updateMsg.taskExecutionState.getExecutionState

          futureResponse.mapTo[Boolean].onComplete {
            case Success(result) =>
              if (!result) {
                self ! FailTask(executionID,
                  new IllegalStateException("Task has been disposed on JobManager."))
              }

              if (!result || executionState == ExecutionState.FINISHED || executionState ==
                ExecutionState.CANCELED || executionState == ExecutionState.FAILED) {
                self ! UnregisterTask(executionID)
              }
            case Failure(t) =>
              log.error(t, "Execution state change notification failed for task {}" +
                s"of job {}.", executionID, jobID)
              self ! UnregisterTask(executionID)
          }
        }
      }

    case SendHeartbeat =>
      var report: Array[Byte] = null
      try {
        report = metricRegistryMapper.writeValueAsBytes(metricRegistry)
      } catch {
        case all: Throwable => log.warning("Error turning the report into JSON", all)
      }

      currentJobManager foreach {
        _ ! Heartbeat(instanceID, report)
      }


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
      if (currentJobManager.isDefined) {
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
        "reregister.", jobManagerAkkaURL)

      cancelAndClearEverything(new Throwable("Lost connection to JobManager"))

      cleanupTaskManager()

      tryJobManagerRegistration()

    case Disconnect(msg) =>
      log.info("Job manager {} wants {} to disconnect. Reason {}.", jobManagerAkkaURL,
        self.path, msg)

      cancelAndClearEverything(new Throwable("Job manager wants me to disconnect."))

      cleanupTaskManager()

      tryJobManagerRegistration()

    case FailIntermediateResultPartitions(executionID) =>
      log.info("Fail intermediate result partitions associated with execution {}.", executionID)
      networkEnvironment foreach {
        _.getPartitionManager.releasePartitionsProducedBy(executionID)
      }

    case BarrierReq(attemptID, checkpointID) =>
      log.debug("[FT-TaskManager] Barrier {} request received for attempt {}", 
          checkpointID, attemptID)
      runningTasks.get(attemptID) match {
        case Some(i) =>
          if (i.getExecutionState == ExecutionState.RUNNING) {
            i.getEnvironment.getInvokable match {
              case barrierTransceiver: BarrierTransceiver =>
                new Thread(new Runnable {
                  override def run(): Unit =  
                    barrierTransceiver.broadcastBarrierFromSource(checkpointID);
                }).start()
              case _ => log.error("[FT-TaskManager] Received a barrier for the wrong vertex")
            }
          }
        case None => log.error("[FT-TaskManager] Received a barrier for an unknown vertex")
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
      val userCodeClassLoader = libraryCacheManager match {
        case Some(manager) =>
          if (log.isDebugEnabled) {
            startRegisteringTask = System.currentTimeMillis()
          }

          // triggers the download of all missing jar files from the job manager
          manager.registerTask(jobID, executionID, tdd.getRequiredJarFiles)

          if (log.isDebugEnabled) {
            log.debug("Register task {} at library cache manager took {}s", executionID,
              (System.currentTimeMillis() - startRegisteringTask) / 1000.0)
          }

          manager.getClassLoader(jobID)
        case None => throw new IllegalStateException("There is no valid library cache manager.")
      }

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

      val env = currentJobManager match {
        case Some(jobManager) =>
          val splitProvider = new TaskInputSplitProvider(jobManager, jobID, vertexID,
            executionID, userCodeClassLoader, timeout)

          new RuntimeEnvironment(jobManager, task, tdd, userCodeClassLoader,
            memoryManager, ioManager, splitProvider, bcVarManager, networkEnvironment.get)

        case None => throw new IllegalStateException("TaskManager has not yet been registered at " +
          "a JobManager.")
      }

      task.setEnvironment(env)

      //inject operator state
      if(tdd.getOperatorStates != null)
      {
        val vertex = task.getEnvironment.getInvokable match {
          case opStateCarrier: OperatorStateCarrier =>
            opStateCarrier.injectState(tdd.getOperatorStates)
        }
      }
      
      // register the task with the network stack and profiles
      networkEnvironment match {
        case Some(ne) =>
          log.info("Register task {} on {}.", task, connectionInfo)
          ne.registerTask(task)
        case None => throw new RuntimeException(
          "Network environment has not been properly instantiated.")
      }

      val jobConfig = tdd.getJobConfiguration

      if (jobConfig.getBoolean(ProfilingUtils.PROFILE_JOB_KEY, true)) {
        profiler match {
          case Some(profilerActorRef) => profilerActorRef ! MonitorTask(task)
          case None => // no log message here - floods the log
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

          libraryCacheManager foreach { _.unregisterTask(jobID, executionID) }
        } catch {
          case t: Throwable => log.error(t, "Error during cleanup of task deployment.")
        }

        sender ! new TaskOperationResult(executionID, false, message)
    }
  }

  private def cleanupTaskManager(): Unit = {
    currentJobManager foreach {
      context.unwatch(_)
    }

    networkEnvironment foreach {
      ne =>
        try {
          ne.shutdown()
        } catch {
          case t: Throwable => log.error(t, "ChannelManager did not shutdown properly.")
        }
    }

    networkEnvironment = None

    libraryCacheManager foreach {
      manager =>
        try {
          manager.shutdown()
        } catch {
          case t: Throwable => log.error(t, "Could not shut down the library cache manager.")
        }
    }

    libraryCacheManager = None

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
    currentJobManager = None
    instanceID = null
    registrationAttempts = 0
    registrationDuration = 0 seconds
  }

  private def updateTask(
    executionId: ExecutionAttemptID,
    partitionInfos: Seq[(IntermediateDataSetID, InputChannelDeploymentDescriptor)]): Unit = {

    runningTasks.get(executionId) match {
      case Some(task) =>
        val errors = partitionInfos flatMap {
          case (resultID, partitionInfo) =>
            Option(task.getEnvironment.getInputGateById(resultID)) match {
              case Some(reader) =>
                Future {
                  try {
                    reader.updateInputChannel(partitionInfo)
                  } catch {
                    case t: Throwable =>
                      log.error(t, "Could not update task {}. Trying to cancel task.",
                       task.getTaskName)

                      try {
                        task.markFailed(t)
                      } catch {
                        case t: Throwable =>
                          log.error(t, "Failed canceling task with execution ID {} after task" +
                            "update failure.", executionId)
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

    currentJobManager = Some(jobManager)
    this.profilerListener = profilerListener
    instanceID = id

    // watch job manager to detect when it dies
    context.watch(jobManager)

    setupNetworkEnvironment(jobManager)
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

  private def setupNetworkEnvironment(jobManager: ActorRef): Unit = {
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
      networkEnvironment = Some(new NetworkEnvironment(self, jobManager, timeout, networkConfig))
    } catch {
      case ioe: IOException =>
        log.error(ioe, "Failed to instantiate network environment.")
        throw new RuntimeException("Failed to instantiate ChannelManager.", ioe)
    }
  }

  private def setupLibraryCacheManager(blobPort: Int): Unit = {
    // shutdown existing library cache manager first
    libraryCacheManager foreach {
      manager => {
        try{
          manager.shutdown()
        } catch {
          case t: Throwable => log.error(t, "Could not properly shut down LibraryCacheManager.")
        }
      }
    }

    // Check if a blob server is specified
    if (blobPort > 0) {

      val address = new InetSocketAddress(
        currentJobManager.flatMap(_.path.address.host).getOrElse("localhost"),
        blobPort)

      log.info("Determined BLOB server address to be {}.", address)

      libraryCacheManager = Some(new BlobLibraryCacheManager(
                                     new BlobCache(address, configuration), cleanupInterval))
    } else {
      libraryCacheManager = Some(new FallbackLibraryCacheManager)
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
        unregisterTaskAndNotifyFinalState(t.getExecutionId)
      }
    }
  }

  private def unregisterTaskAndNotifyFinalState(executionID: ExecutionAttemptID): Unit = {
    runningTasks.remove(executionID) match {
      case Some(task) =>
        log.info("Unregister task with execution ID {}.", executionID)
        removeAllTaskResources(task)
        libraryCacheManager foreach { _.unregisterTask(task.getJobID, executionID) }

        log.info("Updating FINAL execution state of {} ({}) to {}.", task.getTaskName,
          task.getExecutionId, task.getExecutionState);

        self ! UpdateTaskExecutionState(new TaskExecutionState(
          task.getJobID, task.getExecutionId, task.getExecutionState, task.getFailureCause))

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
 * line parsing, constants, and setup methods for the TaskManager.
 */
object TaskManager {

  val LOG = LoggerFactory.getLogger(classOf[TaskManager])

  val STARTUP_FAILURE_RETURN_CODE = 1
  val RUNTIME_FAILURE_RETURN_CODE = 2

  val TASK_MANAGER_NAME = "taskmanager"
  val PROFILER_NAME = "profiler"

  val REGISTRATION_DELAY = 0 seconds
  val REGISTRATION_INTERVAL = 10 seconds
  val MAX_REGISTRATION_ATTEMPTS = 10
  val HEARTBEAT_INTERVAL = 5000 millisecond


  // --------------------------------------------------------------------------
  //  TaskManager standalone entry point
  // --------------------------------------------------------------------------

  /**
   * Entry point (main method) to run the TaskManager in a standalone fashion.
   *
   * @param args The command line arguments.
   */
  def main(args: Array[String]): Unit = {
    // startup checks and logging
    EnvironmentInformation.logEnvironmentInfo(LOG, "TaskManager", args)
    EnvironmentInformation.checkJavaVersion()

    // try to parse the command line arguments
    val configuration = try {
      parseArgsAndLoadConfig(args)
    }
    catch {
      case t: Throwable => {
        LOG.error(t.getMessage(), t)
        System.exit(STARTUP_FAILURE_RETURN_CODE)
        null
      }
    }

    // run the TaskManager (is requested in an authentication enabled context)
    try {
      if (SecurityUtils.isSecurityEnabled) {
        LOG.info("Security is enabled. Starting secure TaskManager.")
        SecurityUtils.runSecured(new FlinkSecuredRunner[Unit] {
          override def run(): Unit = {
            runTaskManager(configuration, classOf[TaskManager])
          }
        })
      }
      else {
        LOG.info("Security is not enabled. Starting non-authenticated TaskManager.")
        runTaskManager(configuration, classOf[TaskManager])
      }
    }
    catch {
      case t: Throwable => {
        LOG.error("Failed to run TaskManager.", t)
        System.exit(STARTUP_FAILURE_RETURN_CODE)
      }
    }
  }

  /**
   * Parse the command line arguments of the [[TaskManager]] and loads the configuration.
   *
   * @param args Command line arguments
   * @return The parsed configuration.
   */
  @throws(classOf[Exception])
  def parseArgsAndLoadConfig(args: Array[String]): Configuration = {

    // set up the command line parser
    val parser = new scopt.OptionParser[TaskManagerCLIConfiguration]("taskmanager") {
      head("flink task manager")
      opt[String]("configDir") action { (x, c) =>
        c.copy(configDir = x)
      } text "Specify configuration directory."
    }

    // parse the CLI arguments
    val cliConfig = parser.parse(args, TaskManagerCLIConfiguration()).getOrElse {
      throw new Exception(
        s"Invalid command line agruments: ${args.mkString(" ")}. Usage: ${parser.usage}")
    }

    // load the configuration
    try {
      LOG.info("Loading configuration from " + cliConfig.configDir)
      GlobalConfiguration.loadConfiguration(cliConfig.configDir)
      GlobalConfiguration.getConfiguration()
    }
    catch {
      case e: Exception => throw new Exception("Could not load configuration", e)
    }
  }

  // --------------------------------------------------------------------------
  //  Starting and running the TaskManager
  // --------------------------------------------------------------------------

  /**
   * Starts and runs the TaskManager. Brings up an actor system for the TaskManager and its
   * actors, starts the TaskManager's services (library cache, shuffle network stack, ...),
   * and starts the TaskManager itself.

   * @param configuration The configuration for the TaskManager.
   * @param taskManagerClass The actor class to instantiate. Allows to use TaskManager subclasses
   *                         for example for YARN.
   */
  @throws(classOf[Exception])
  def runTaskManager(configuration: Configuration,
                     taskManagerClass: Class[_ <: TaskManager]) : Unit = {

    val (jobManagerHostname, jobManagerPort) = getAndCheckJobManagerAddress(configuration)

    var taskManagerHostname = configuration.getString(
                                       ConfigConstants.TASK_MANAGER_HOSTNAME_KEY, null)

    if (taskManagerHostname != null) {
      LOG.info("Using configured hostname/address for TaskManager: " + taskManagerHostname)
    }
    else {
      // try to find out the hostname of the interface from which the TaskManager
      // can connect to the JobManager. This involves a reverse name lookup
      LOG.info("Trying to select the network interface and address to use " +
        "by connecting to the configured JobManager")

      val jobManagerAddress = new InetSocketAddress(jobManagerHostname, jobManagerPort)
      taskManagerHostname = try {
        // try to get the address for up to two minutes and start
        // logging only after ten seconds
        NetUtils.findConnectingAddress(jobManagerAddress, 120000, 10000).getHostName()
      }
      catch {
        case t: Throwable => throw new Exception("TaskManager cannot find a network interface " +
          "that can communicate with the JobManager (" + jobManagerAddress + ")", t)
      }

      LOG.info("TaskManager will use hostname/address '{}' for communication.", taskManagerHostname)
    }

    // if no task manager port has been configured, use 0 (system will pick any free port)
    val actorSystemPort = configuration.getInteger(ConfigConstants.TASK_MANAGER_IPC_PORT_KEY, 0)
    if (actorSystemPort < 0) {
      throw new Exception("Invalid value for '" + ConfigConstants.TASK_MANAGER_IPC_PORT_KEY  +
        "' (port for the TaskManager actor system) : " + actorSystemPort +
        " - Leave config parameter empty or use 0 to let the system choose a port automatically.")
    }

    runTaskManager(taskManagerHostname, actorSystemPort, configuration, taskManagerClass)
  }

  /**
   * Starts and runs the TaskManager. Brings up an actor system for the TaskManager and its
   * actors, starts the TaskManager's services (library cache, shuffle network stack, ...),
   * and starts the TaskManager itself.
   *
   * This method will also spawn a process reaper for the TaskManager (kill the process if
   * the actor fails) and optionally start the JVM memory logging thread.
   *
   * @param taskManagerHostname The hostname/address of the interface where the actor system
   *                         will communicate.
   * @param actorSystemPort The port at which the actor system will communicate.
   * @param configuration The configuration for the TaskManager.
   */
  @throws(classOf[Exception])
  def runTaskManager(taskManagerHostname: String,
                     actorSystemPort: Int,
                     configuration: Configuration) : Unit = {

    runTaskManager(taskManagerHostname, actorSystemPort, configuration, classOf[TaskManager])
  }

  /**
   * Starts and runs the TaskManager. Brings up an actor system for the TaskManager and its
   * actors, starts the TaskManager's services (library cache, shuffle network stack, ...),
   * and starts the TaskManager itself.
   *
   * This method will also spawn a process reaper for the TaskManager (kill the process if
   * the actor fails) and optionally start the JVM memory logging thread.
   *
   * @param taskManagerHostname The hostname/address of the interface where the actor system
   *                         will communicate.
   * @param actorSystemPort The port at which the actor system will communicate.
   * @param configuration The configuration for the TaskManager.
   * @param taskManagerClass The actor class to instantiate. Allows the use of TaskManager
   *                         subclasses for example for YARN.
   */
  @throws(classOf[Exception])
  def runTaskManager(taskManagerHostname: String,
                     actorSystemPort: Int,
                     configuration: Configuration,
                     taskManagerClass: Class[_ <: TaskManager]) : Unit = {

    LOG.info("Starting TaskManager")

    // Bring up the TaskManager actor system first, bind it to the given address.
    LOG.info("Starting TaskManager actor system")

    val taskManagerSystem = try {
      val akkaConfig = AkkaUtils.getAkkaConfig(configuration,
                                               Some((taskManagerHostname, actorSystemPort)))
      if (LOG.isDebugEnabled) {
        LOG.debug("Using akka configuration\n " + akkaConfig)
      }
      AkkaUtils.createActorSystem(akkaConfig)
    }
    catch {
      case t: Throwable => {
        if (t.isInstanceOf[org.jboss.netty.channel.ChannelException]) {
          val cause = t.getCause()
          if (cause != null && t.getCause().isInstanceOf[java.net.BindException]) {
            val address = taskManagerHostname + ":" + actorSystemPort
            throw new Exception("Unable to bind TaskManager actor system to address " +
              address + " - " + cause.getMessage(), t)
          }
        }
        throw new Exception("Could not create TaskManager actor system", t)
      }
    }

    // start all the TaskManager services (network stack,  library cache, ...)
    // and the TaskManager actor
    try {
      LOG.info("Starting TaskManager actor")
      val taskManager = startTaskManagerActor(configuration, taskManagerSystem, taskManagerHostname,
        TASK_MANAGER_NAME, false, false, taskManagerClass)

      // start a process reaper that watches the JobManager. If the JobManager actor dies,
      // the process reaper will kill the JVM process (to ensure easy failure detection)
      LOG.debug("Starting TaskManager process reaper")
      taskManagerSystem.actorOf(
        Props(classOf[ProcessReaper], taskManager, LOG, RUNTIME_FAILURE_RETURN_CODE),
        "TaskManager_Process_Reaper")

      // if desired, start the logging daemon that periodically logs the
      // memory usage information
      if (LOG.isInfoEnabled && configuration.getBoolean(
        ConfigConstants.TASK_MANAGER_DEBUG_MEMORY_USAGE_START_LOG_THREAD,
        ConfigConstants.DEFAULT_TASK_MANAGER_DEBUG_MEMORY_USAGE_START_LOG_THREAD)) {
        LOG.info("Starting periodic memory usage logger")

        val interval = configuration.getLong(
          ConfigConstants.TASK_MANAGER_DEBUG_MEMORY_USAGE_LOG_INTERVAL_MS,
          ConfigConstants.DEFAULT_TASK_MANAGER_DEBUG_MEMORY_USAGE_LOG_INTERVAL_MS)

        val logger = new Thread("Memory Usage Logger") {
          override def run(): Unit = {
            try {
              val memoryMXBean = ManagementFactory.getMemoryMXBean
              val gcMXBeans = ManagementFactory.getGarbageCollectorMXBeans.asScala

              while (!taskManagerSystem.isTerminated) {
                Thread.sleep(interval)
                LOG.info(getMemoryUsageStatsAsString(memoryMXBean))
                LOG.info(TaskManager.getGarbageCollectorStatsAsString(gcMXBeans))
              }
            }
            catch {
              case t: Throwable => LOG.error("Memory usage logging thread died", t)
            }
          }
        }
        logger.setDaemon(true)
        logger.start()
      }

      // block until everything is done
      taskManagerSystem.awaitTermination()
    }
    catch {
      case t: Throwable => {
        LOG.error("Error while starting up taskManager", t)
        try {
          taskManagerSystem.shutdown()
        } catch {
          case tt: Throwable => LOG.warn("Could not cleanly shut down actor system", tt)
        }
        throw t
      }
    }
  }

  @throws(classOf[Exception])
  def startTaskManagerActor(configuration: Configuration,
                            actorSystem: ActorSystem,
                            taskManagerHostname: String,
                            taskManagerActorName: String,
                            localAkkaCommunication: Boolean,
                            localTaskManagerCommunication: Boolean,
                            taskManagerClass: Class[_ <: TaskManager]): ActorRef = {

    val (tmConfig, netConfig, connectionInfo, jmAkkaURL) =  parseTaskManagerConfiguration(
      configuration, taskManagerHostname, localAkkaCommunication, localTaskManagerCommunication)

    val tmProps = Props(taskManagerClass, connectionInfo, jmAkkaURL, tmConfig, netConfig)
    actorSystem.actorOf(tmProps, taskManagerActorName)
  }

  /**
   * Starts the profiler actor.
   *
   * @param instanceActorPath The actor path of the taskManager that is profiled.
   * @param reportInterval The interval in which the profiler runs.
   * @param actorSystem The actor system for the profiler actor
   * @return The profiler actor ref.
   */
  private def startProfiler(instanceActorPath: String,
                            reportInterval: Long,
                            actorSystem: ActorSystem): ActorRef = {

    val profilerProps = Props(classOf[TaskManagerProfiler], instanceActorPath, reportInterval)
    actorSystem.actorOf(profilerProps, PROFILER_NAME)
  }

  // --------------------------------------------------------------------------
  //  Resolving the TaskManager actor
  // --------------------------------------------------------------------------

  /**
   * Resolves the TaskManager actor reference in a blocking fashion.
   *
   * @param taskManagerUrl The akka URL of the JobManager.
   * @param system The local actor system that should perform the lookup.
   * @param timeout The maximum time to wait until the lookup fails.
   * @throws java.io.IOException Thrown, if the lookup fails.
   * @return The ActorRef to the TaskManager
   */
  @throws(classOf[IOException])
  def getTaskManagerRemoteReference(taskManagerUrl: String,
                                   system: ActorSystem,
                                   timeout: FiniteDuration): ActorRef = {
    try {
      val future = AkkaUtils.getReference(taskManagerUrl, system, timeout)
      Await.result(future, timeout)
    }
    catch {
      case e @ (_ : ActorNotFound | _ : TimeoutException) =>
        throw new IOException(
          s"TaskManager at $taskManagerUrl not reachable. " +
            s"Please make sure that the TaskManager is running and its port is reachable.", e)

      case e: IOException =>
        throw new IOException("Could not connect to TaskManager at " + taskManagerUrl, e)
    }
  }

  // --------------------------------------------------------------------------
  //  Miscellaneous Utilities
  // --------------------------------------------------------------------------

  /**
   * Utility method to extract TaskManager config parameters from the configuration and to
   * sanity check them.
   *
   * @param configuration The configuration.
   * @param taskManagerHostname The host name under which the TaskManager communicates.
   * @param localAkkaCommunication True, if the TaskManager runs in the same actor
   *                               system as its JobManager.
   * @param localTaskManagerCommunication True, to skip initializing the network stack.
   *                                      Use only when only one task manager is used.
   * @return A tuple (TaskManagerConfiguration, network configuration,
   *                  InstanceConnectionInfo, JobManager actor Akka URL).
   */
  @throws(classOf[Exception])
  def parseTaskManagerConfiguration(configuration: Configuration,
                                    taskManagerHostname: String,
                                    localAkkaCommunication: Boolean,
                                    localTaskManagerCommunication: Boolean):
  (TaskManagerConfiguration, NetworkEnvironmentConfiguration, InstanceConnectionInfo, String) = {

    // ------- read values from the config and check them ---------
    //                      (a lot of them)

    // ----> hosts / ports for communication and data exchange

    val dataport = configuration.getInteger(ConfigConstants.TASK_MANAGER_DATA_PORT_KEY,
      ConfigConstants.DEFAULT_TASK_MANAGER_DATA_PORT) match {
      case 0 => NetUtils.getAvailablePort()
      case x => x
    }

    checkConfigParameter(dataport > 0, dataport, ConfigConstants.TASK_MANAGER_DATA_PORT_KEY,
      "Leave config parameter empty or use 0 to let the system choose a port automatically.")

    val taskManagerAddress = InetAddress.getByName(taskManagerHostname)
    val connectionInfo = new InstanceConnectionInfo(taskManagerAddress, dataport)

    val jobManagerActorURL = if (localAkkaCommunication) {
      // JobManager and TaskManager are in the same ActorSystem -> Use local Akka URL
      JobManager.getLocalJobManagerAkkaURL
    }
    else {
      // both run in different actor system
      val (jobManagerHostname, jobManagerPort) = getAndCheckJobManagerAddress(configuration)
      val hostPort = new InetSocketAddress(jobManagerHostname, jobManagerPort)
      JobManager.getRemoteJobManagerAkkaURL(hostPort)
    }

    // ----> memory / network stack (shuffles/broadcasts), task slots, temp directories

    // we need this because many configs have been written with a "-1" entry
    val slots = configuration.getInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, 1) match {
      case -1 => 1
      case x => x
    }

    val pageSize = configuration.getInteger(ConfigConstants.TASK_MANAGER_NETWORK_BUFFER_SIZE_KEY,
      ConfigConstants.DEFAULT_TASK_MANAGER_NETWORK_BUFFER_SIZE)
    val numNetworkBuffers = configuration.getInteger(
      ConfigConstants.TASK_MANAGER_NETWORK_NUM_BUFFERS_KEY,
      ConfigConstants.DEFAULT_TASK_MANAGER_NETWORK_NUM_BUFFERS)

    val configuredMemory = configuration.getLong(ConfigConstants.TASK_MANAGER_MEMORY_SIZE_KEY, -1L)

    checkConfigParameter(slots >= 1, slots, ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS,
      "Number of task slots must be at least one.")

    checkConfigParameter(numNetworkBuffers > 0, numNetworkBuffers,
      ConfigConstants.TASK_MANAGER_NETWORK_NUM_BUFFERS_KEY)

    checkConfigParameter(pageSize >= DefaultMemoryManager.MIN_PAGE_SIZE, pageSize,
      ConfigConstants.TASK_MANAGER_NETWORK_BUFFER_SIZE_KEY,
      "Minimum buffer size is " + DefaultMemoryManager.MIN_PAGE_SIZE)

    checkConfigParameter(MathUtils.isPowerOf2(pageSize), pageSize,
      ConfigConstants.TASK_MANAGER_NETWORK_BUFFER_SIZE_KEY,
      "Buffer size must be a power of 2.")

    checkConfigParameter(configuredMemory == -1 || configuredMemory > 0, configuredMemory,
      ConfigConstants.TASK_MANAGER_MEMORY_SIZE_KEY,
      "MemoryManager needs at least one MB of memory. " +
        "Leave this config parameter empty to let the system automatically " +
        "pick a fraction of the available memory.")

    val tmpDirs = configuration.getString(
      ConfigConstants.TASK_MANAGER_TMP_DIR_KEY,
      ConfigConstants.DEFAULT_TASK_MANAGER_TMP_PATH)
      .split(",|" + File.pathSeparator)

    checkTempDirs(tmpDirs)

    val nettyConfig = if (localTaskManagerCommunication) {
      None
    } else {
      Some(new NettyConfig(
        connectionInfo.address(), connectionInfo.dataPort(), pageSize, configuration))
    }

    // Default spill I/O mode for intermediate results
    val syncOrAsync = configuration.getString(ConfigConstants.TASK_MANAGER_NETWORK_DEFAULT_IO_MODE,
      ConfigConstants.DEFAULT_TASK_MANAGER_NETWORK_DEFAULT_IO_MODE)

    val ioMode : IOMode = if (syncOrAsync == "async") {
      IOMode.ASYNC
    }
    else {
      IOMode.SYNC
    }

    val networkConfig = NetworkEnvironmentConfiguration(numNetworkBuffers, pageSize, ioMode,
      nettyConfig)

    val networkBufferMem = numNetworkBuffers * pageSize

    val memorySize = if (configuredMemory > 0) {
      LOG.info("Using {} MB for Flink managed memory.", configuredMemory)
      configuredMemory << 20 // megabytes to bytes
    }
    else {
      val fraction = configuration.getFloat(ConfigConstants.TASK_MANAGER_MEMORY_FRACTION_KEY,
        ConfigConstants.DEFAULT_MEMORY_MANAGER_MEMORY_FRACTION)
      checkConfigParameter(fraction > 0.0f, fraction,
        ConfigConstants.TASK_MANAGER_MEMORY_FRACTION_KEY,
        "MemoryManager fraction of the free memory must be positive.")

      val relativeMemSize = ((EnvironmentInformation.getSizeOfFreeHeapMemoryWithDefrag() -
        networkBufferMem) * fraction).toLong

      LOG.info("Using {} of the currently free heap space for Flink managed memory ({} MB).",
        fraction, relativeMemSize >> 20)

      relativeMemSize
    }

    // ----> timeouts, library caching, profiling

    val timeout = try {
      AkkaUtils.getTimeout(configuration)
    }
    catch {
      case e: Exception => throw new Exception(
        s"Invalid format for '${ConfigConstants.AKKA_ASK_TIMEOUT}'. " +
          s"Use formats like '50 s' or '1 min' to specify the timeout.")
    }
    LOG.info("Messages between TaskManager and JobManager have a max timeout of " + timeout)

    val profilingInterval =
      if (configuration.getBoolean(ProfilingUtils.ENABLE_PROFILING_KEY, false)) {
        Some(configuration.getLong(ProfilingUtils.TASKMANAGER_REPORTINTERVAL_KEY,
          ProfilingUtils.DEFAULT_TASKMANAGER_REPORTINTERVAL))
      } else {
        None
      }

    val cleanupInterval = configuration.getLong(
      ConfigConstants.LIBRARY_CACHE_MANAGER_CLEANUP_INTERVAL,
      ConfigConstants.DEFAULT_LIBRARY_CACHE_MANAGER_CLEANUP_INTERVAL) * 1000



    val maxRegistrationDuration = Duration(configuration.getString(
      ConfigConstants.TASK_MANAGER_MAX_REGISTRATION_DURATION,
      ConfigConstants.DEFAULT_TASK_MANAGER_MAX_REGISTRATION_DURATION))

    val taskManagerConfig = TaskManagerConfiguration(slots, memorySize, pageSize,
      tmpDirs, cleanupInterval, profilingInterval, timeout, maxRegistrationDuration,
      configuration)

    (taskManagerConfig, networkConfig, connectionInfo, jobManagerActorURL)
  }

  /**
   * Gets the hostname and port of the JobManager from the configuration. Also checks that
   * the hostname is not null and the port non-negative.
   *
   * @param configuration The configuration to read the config values from.
   * @return A 2-tuple (hostname, port).
   */
  private def getAndCheckJobManagerAddress(configuration: Configuration) : (String, Int) = {

    val hostname = configuration.getString(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, null)

    val port = configuration.getInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY,
      ConfigConstants.DEFAULT_JOB_MANAGER_IPC_PORT)

    if (hostname == null) {
      throw new Exception("Config parameter '" + ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY +
        "' is missing (hostname/address of JobManager to connect to).")
    }

    if (port <= 0 || port >= 65536) {
      throw new Exception("Invalid value for '" + ConfigConstants.JOB_MANAGER_IPC_PORT_KEY +
        "' (port of the JobManager actor system) : " + port +
        ".  it must be great than 0 and less than 65536.")
    }

    (hostname, port)
  }

  private def checkConfigParameter(condition: Boolean,
                                   parameter: Any,
                                   name: String,
                                   errorMessage: String = ""): Unit = {
    if (!condition) {
      throw new Exception(
        s"Invalid configuration value for '${name}' : ${parameter} - ${errorMessage}")
    }
  }

  private def checkTempDirs(tmpDirs: Array[String]): Unit = {
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

  /**
   * Gets the memory footprint of the JVM in a string representation.
   *
   * @param memoryMXBean The memory management bean used to access the memory statistics.
   * @return A string describing how much heap memory and direct memory are allocated and used.
   */
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

  /**
   * Gets the garbage collection statistics from the JVM.
   *
   * @param gcMXBeans The collection of garbage collector beans.
   * @return A string denoting the number of times and total elapsed time in garbage collection.
   */
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
