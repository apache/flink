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
import java.net.InetSocketAddress
import java.util
import java.util.concurrent.{FutureTask, TimeUnit}

import akka.actor._
import akka.pattern.ask
import org.apache.flink.api.common.cache.DistributedCache
import org.apache.flink.configuration.{GlobalConfiguration, ConfigConstants, Configuration}
import org.apache.flink.core.fs.Path
import org.apache.flink.runtime.ActorLogMessages
import org.apache.flink.runtime.akka.AkkaUtils
import org.apache.flink.runtime.execution.{ExecutionState, RuntimeEnvironment, ExecutionState2}
import org.apache.flink.runtime.execution.librarycache.{LibraryCacheProfileResponse, LibraryCacheManager, LibraryCacheUpdate}
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID
import org.apache.flink.runtime.filecache.FileCache
import org.apache.flink.runtime.instance.{InstanceConnectionInfo, HardwareDescription, InstanceID}
import org.apache.flink.runtime.io.disk.iomanager.IOManager
import org.apache.flink.runtime.io.network.ChannelManager
import org.apache.flink.runtime.io.network.netty.NettyConnectionManager
import org.apache.flink.runtime.jobgraph.JobID
import org.apache.flink.runtime.jobmanager.JobManager
import org.apache.flink.runtime.memorymanager.DefaultMemoryManager
import org.apache.flink.runtime.messages.JobManagerMessages.UpdateTaskExecutionState
import org.apache.flink.runtime.messages.RegistrationMessages.{RegisterTaskManager, AcknowledgeRegistration}
import org.apache.flink.runtime.messages.TaskManagerMessages._
import org.apache.flink.runtime.net.NetUtils
import org.apache.flink.runtime.util.EnvironmentInformation
import org.apache.flink.util.ExceptionUtils
import org.slf4j.LoggerFactory

import scala.collection.convert.{WrapAsScala, DecorateAsScala}
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.Failure

class TaskManager(val instanceConnection: InstanceConnectionInfo, val jobManagerURL: String, val numberOfSlots: Int,
val memorySize: Long, val pageSize: Int, val tmpDirPaths: Array[String], val networkConnectionConfig:
NetworkConnectionConfiguration, memoryUsageLogging: MemoryUsageLogging) extends Actor with
ActorLogMessages with ActorLogging with DecorateAsScala with WrapAsScala{
  import context.dispatcher
  import AkkaUtils.FUTURE_TIMEOUT

  val REGISTRATION_DELAY = 0 seconds
  val REGISTRATION_INTERVAL = 10 seconds
  val MAX_REGISTRATION_ATTEMPTS = 1
  val HEARTBEAT_INTERVAL = 200 millisecond

  TaskManager.checkTempDirs(tmpDirPaths)
  val ioManager = new IOManager(tmpDirPaths)
  val memoryManager = new DefaultMemoryManager(memorySize, numberOfSlots, pageSize)
  val hardwareDescription = HardwareDescription.extractFromSystem(memoryManager.getMemorySize)
  val fileCache = new FileCache()
  val runningTasks = scala.collection.concurrent.TrieMap[ExecutionAttemptID, Task]()


  var channelManager: Option[ChannelManager] = None
  var registrationScheduler: Option[Cancellable] = None
  var registrationAttempts: Int = 0
  var registered: Boolean = false
  var currentJobManager = ActorRef.noSender
  var instanceID: InstanceID = null;
  var memoryMXBean:Option[MemoryMXBean] = None
  var gcMXBeans:Option[Iterable[GarbageCollectorMXBean]] = None
  var heartbeatScheduler: Option[Cancellable] = None

  if(log.isDebugEnabled){
    memoryUsageLogging.logIntervalMs.foreach{
      interval =>
        val d = FiniteDuration(interval, TimeUnit.MILLISECONDS)
        memoryMXBean = Some(ManagementFactory.getMemoryMXBean)
        gcMXBeans = Some(ManagementFactory.getGarbageCollectorMXBeans.asScala)

        context.system.scheduler.schedule(d, d, self, LogMemoryUsage)
    }
  }

  override def preStart(): Unit = {
    tryJobManagerRegistration()
  }

  override def postStop(): Unit = {
    channelManager foreach {
      channelManager =>
      try {
        channelManager.shutdown()
      }catch{
        case t: Throwable =>
          log.error(t, "ChannelManager did not shutdown properly.")
      }
    }

    ioManager.shutdown()
    memoryManager.shutdown()
    fileCache.shutdown()
  }

  def tryJobManagerRegistration(): Unit = {
    registrationAttempts = 0
    import context.dispatcher
    registrationScheduler = Some(context.system.scheduler.schedule(REGISTRATION_DELAY, REGISTRATION_INTERVAL,
      self, RegisterAtMaster))
  }


  override def receiveWithLogMessages: Receive = {
    case RegisterAtMaster => {
      registrationAttempts += 1

      if (registered) {
        registrationScheduler.foreach(_.cancel())
      } else if (registrationAttempts <= MAX_REGISTRATION_ATTEMPTS) {
        val jobManagerURL = getJobManagerURL

        log.info(s"Try to register at master ${jobManagerURL}. ${registrationAttempts}. Attempt")
        val jobManager = context.actorSelection(jobManagerURL)

        jobManager ! RegisterTaskManager(hardwareDescription, numberOfSlots)
      } else {
        log.error("TaskManager could not register at JobManager.");
        throw new RuntimeException("TaskManager could not register at JobManager");
      }
    }

    case AcknowledgeRegistration(id) => {
      if (!registered) {
        registered = true
        currentJobManager = sender()
        instanceID = id

        log.info(s"TaskManager successfully registered at JobManager ${currentJobManager.path.toString}.")

        heartbeatScheduler = Some(context.system.scheduler.schedule(HEARTBEAT_INTERVAL, HEARTBEAT_INTERVAL, self,
          SendHeartbeat))
        context.watch(currentJobManager)

        setupChannelManager()
      }
    }

    case CancelTask( executionID) => {
      runningTasks.get(executionID) match {
        case Some(task) =>
          Future{ task.cancelExecution() }
          new TaskOperationResult(executionID, true)
        case None =>
          new TaskOperationResult(executionID, false, "No task with that execution ID was " +
            "found.")
      }
    }

    case SubmitTask(tdd) => {
      val jobID = tdd.getJobID
      val vertexID = tdd.getVertexID
      val executionID = tdd.getExecutionId
      val taskIndex = tdd.getIndexInSubtaskGroup
      val numSubtasks = tdd.getCurrentNumberOfSubtasks

      try{
        val userCodeClassLoader = LibraryCacheManager.getClassLoader(jobID)

        if(userCodeClassLoader == null){
          throw new RuntimeException("No user code Classloader available.")
        }

        val task = new Task(jobID, vertexID, taskIndex, numSubtasks, executionID, tdd.getTaskName, this)

        if(runningTasks.putIfAbsent(executionID, task) != null){
          throw new RuntimeException(s"TaskManager contains already a task with executionID ${executionID}.")
        }

        var success = false
        try{
          val splitProvider = new TaskInputSplitProvider(currentJobManager, jobID, vertexID)
          val env = new RuntimeEnvironment(task, tdd, userCodeClassLoader, memoryManager, ioManager, splitProvider,
            currentJobManager)
          task.setEnvironment(env)

          // register the task with the network stack and profilers
          channelManager match {
            case Some(cm) => cm.register(task)
            case None => throw new RuntimeException("ChannelManager has not been properly instantiated.")
          }

          val jobConfig = tdd.getJobConfiguration

          // TODO: Implement profiler
          val enableProfiling = false

//          if(enableProfiling){
//            task.registerProfiler(profiler, jobConfig)
//          }

          val cpTasks = new util.HashMap[String, FutureTask[Path]]()

          for(entry <- DistributedCache.readFileInfoFromConfig(tdd.getJobConfiguration)){
            val cp = fileCache.createTmpFile(entry.getKey, entry.getValue, jobID)
            cpTasks.put(entry.getKey, cp)
          }
          env.addCopyTasksForCacheFile(cpTasks)

          if(!task.startExecution()){
            throw new RuntimeException("Cannot start task. Task was canceled or failed.")
          }

          success = true
          new TaskOperationResult(executionID, true)
        }finally{
          if(!success){
            runningTasks.remove(executionID)
            for(entry <- DistributedCache.readFileInfoFromConfig(tdd.getJobConfiguration)){
              fileCache.deleteTmpFile(entry.getKey, entry.getValue, jobID)
            }
          }
        }
      }catch{
        case t: Throwable =>
          log.error(t, "Could not instantiate task.")

          try{
            LibraryCacheManager.unregister(jobID)
          }catch{
            case ioe: IOException =>
              log.debug(s"Unregistering the execution ${executionID} caused an IOException.")
          }

          new TaskOperationResult(executionID, false, ExceptionUtils.stringifyException(t))
      }
    }

    case RequestLibraryCacheProfile(request) => {
      val response = new LibraryCacheProfileResponse(request)
      val requiredLibraries = request.getRequiredLibraries

      for((library, id) <- requiredLibraries.zipWithIndex){
        response.setCached(id, LibraryCacheManager.contains(library) != null)
      }

      sender() ! response
    }

    case SendHeartbeat => {
      currentJobManager ! Heartbeat(instanceID)
    }

    case x:LibraryCacheUpdate => {
      log.info(s"Registered library ${x.getLibraryFileName}.")
      sender() ! AcknowledgeLibraryCacheUpdate
    }

    case LogMemoryUsage => {
      memoryMXBean foreach {
        mxbean => log.debug(TaskManager.getMemoryUsageStatsAsString(mxbean))
      }

      gcMXBeans foreach {
        mxbeans => log.debug(TaskManager.getGarbageCollectorStatsAsString(mxbeans))
      }
    }
  }

  def notifyExecutionStateChange(jobID: JobID, executionID: ExecutionAttemptID, executionState : ExecutionState,
                                 optionalError: Throwable): Unit = {
    val futureResponse = currentJobManager ? UpdateTaskExecutionState(new TaskExecutionState(jobID, executionID,
      executionState, optionalError))

    futureResponse.onComplete{
      x =>
        x match {
          case Failure(ex) =>
            log.error(ex, "Error sending task state update to JobManager.")
          case _ =>
        }
        if(executionState == ExecutionState.FINISHED || executionState == ExecutionState.CANCELED || executionState ==
          ExecutionState.FAILED){
          unregisterTask(executionID)
        }
    }
  }

  def unregisterTask(executionID: ExecutionAttemptID): Unit = {
    runningTasks.remove(executionID) match {
      case task: Task =>
        for(entry <- DistributedCache.readFileInfoFromConfig(task.getEnvironment.getJobConfiguration)){
          fileCache.deleteTmpFile(entry.getKey, entry.getValue, task.getJobID)
        }

        channelManager foreach { _.unregister(executionID, task) }

//        task.unregisterProfiler(profiler)

        task.unregisterMemoryManager(memoryManager)

        try{
          LibraryCacheManager.unregister(task.getJobID)
        }catch{
          case ioe: IOException =>
            log.error(ioe, s"Unregistering the execution ${executionID} caused an IOException.")
        }
      case _ =>
        log.error(s"Cannot find task with ID ${executionID} to unregister.")
    }
  }

  private def getJobManagerURL: String = {
    JobManager.getAkkaURL(jobManagerURL)
  }

  def setupChannelManager(): Unit = {
    //shutdown existing channel manager
    channelManager map {
      cm =>
        try{
          cm.shutdown()
        }catch{
          case t: Throwable => log.error(t, "ChannelManager did not shutdown properly.")
        }
    }

    try {
      import networkConnectionConfig._

      val networkConnectionManager = new NettyConnectionManager(instanceConnection.address(), instanceConnection.dataPort(),
        bufferSize, numInThreads, numOutThreads, closeAfterIdleForMs)
      channelManager = Some(new ChannelManager(currentJobManager, instanceConnection, numBuffers, bufferSize,
        networkConnectionManager))
    }catch{
      case ioe: IOException =>
        log.error(ioe, "Failed to instantiate ChannelManager.")
        throw new RuntimeException("Failed to instantiate ChannelManager.", ioe)
    }
  }
}

object TaskManager{

  val LOG = LoggerFactory.getLogger(classOf[TaskManager])
  val FAILURE_RETURN_CODE = -1

  def main(args: Array[String]): Unit = {
    val (hostname, port, configuration) = initialize(args)

    val taskManagerSystem = startActorSystemAndActor(hostname, port, configuration)
    taskManagerSystem.awaitTermination()
  }

  private def initialize(args: Array[String]):(String, Int, Configuration) = {
    val parser = new scopt.OptionParser[TaskManagerCLIConfiguration]("taskmanager"){
      head("flink task manager")
      opt[String]("configDir") action { (x, c) =>
        c.copy(configDir = x)
      } text("Specify configuration directory.")
      opt[String]("tempDir") action { (x, c) =>
        c.copy(tmpDir = x)
      } text("Specify temporary directory.")
    }


    parser.parse(args, TaskManagerCLIConfiguration()) map {
      config =>
        GlobalConfiguration.loadConfiguration(config.configDir)

        val configuration = GlobalConfiguration.getConfiguration()

        if(config.tmpDir != null && GlobalConfiguration.getString(ConfigConstants.TASK_MANAGER_TMP_DIR_KEY,
          null) == null){
          configuration.setString(ConfigConstants.TASK_MANAGER_TMP_DIR_KEY, config.tmpDir)
        }

        val jobManagerHostname = configuration.getString(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, null);
        val jobManagerPort = configuration.getInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY,
          ConfigConstants.DEFAULT_JOB_MANAGER_IPC_PORT)

        val jobManagerAddress = new InetSocketAddress(jobManagerHostname, jobManagerPort);

        val port = configuration.getInteger(ConfigConstants.TASK_MANAGER_IPC_PORT_KEY, 0)
        val hostname = NetUtils.resolveAddress(jobManagerAddress).getHostName;

        (hostname, port, configuration)
    } getOrElse {
      LOG.error("CLI parsing failed. Usage: " + parser.usage)
      sys.exit(FAILURE_RETURN_CODE)
    }
  }

  def startActorSystemAndActor(hostname: String, port: Int, configuration: Configuration) = {
    val actorSystem = AkkaUtils.createActorSystem(hostname, port, configuration)
    startActor(actorSystem, configuration)
    actorSystem
  }

  def startActor(actorSystem: ActorSystem, configuration: Configuration): ActorRef = {
    val jobManagerAddress = configuration.getString(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, null);
    val jobManagerRPCPort = configuration.getInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY,
      ConfigConstants.DEFAULT_JOB_MANAGER_IPC_PORT);

    if(jobManagerAddress == null){
      throw new RuntimeException("JobManager address has not been specified in the configuration.")
    }

    val jobManagerURL = jobManagerAddress + ":" + jobManagerRPCPort
    val slots = configuration.getInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, 1)

    val numberOfSlots = if(slots > 0) slots else 1

    val configuredMemory:Long = configuration.getInteger(ConfigConstants.TASK_MANAGER_MEMORY_SIZE_KEY, -1)

    val memorySize = if(configuredMemory > 0){
      configuredMemory << 20
    } else{
      val fraction = configuration.getFloat(ConfigConstants.TASK_MANAGER_MEMORY_FRACTION_KEY,
        ConfigConstants.DEFAULT_MEMORY_MANAGER_MEMORY_FRACTION)
      (EnvironmentInformation.getSizeOfFreeHeapMemoryWithDefrag * fraction).toLong
    }

    val pageSize = configuration.getInteger(ConfigConstants.TASK_MANAGER_NETWORK_BUFFER_SIZE_KEY,
      ConfigConstants.DEFAULT_TASK_MANAGER_NETWORK_BUFFER_SIZE)

    actorSystem.actorOf(Props(classOf[TaskManager], jobManagerURL, numberOfSlots, memorySize, pageSize), "taskmanager");
  }

  def getAkkaURL(address: String): String = {
    s"akka.tcp://flink@${address}/user/taskmanager"
  }

  def checkTempDirs(tmpDirs: Array[String]): Unit ={
    tmpDirs.zipWithIndex.foreach {
      case (dir: String, _) =>
        val file = new File(dir)

        if(!file.exists) {
          throw new Exception(s"Temporary file directory ${file.getAbsolutePath} does not exist.")
        }
        if(!file.isDirectory){
          throw new Exception(s"Temporary file directory ${file.getAbsolutePath} is not a directory.")
        }
        if(!file.canWrite){
          throw new Exception(s"Temporary file directory ${file.getAbsolutePath} is not writable.")
        }
      case (_, id) => throw new Exception(s"Temporary file directory #${id} is null.")

    }
  }

  def getMemoryUsageStatsAsString(memoryMXBean: MemoryMXBean): String = {
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

  def getGarbageCollectorStatsAsString(gcMXBeans: Iterable[GarbageCollectorMXBean]): String = {
    val beans = gcMXBeans map {
      bean =>
        s"[${bean.getName}, GC TIME (ms): ${bean.getCollectionTime}, GC COUNT: ${bean.getCollectionCount}]"
    } mkString(", ")

    "Garbage collector stats: " + beans
  }
}
