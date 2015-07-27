package org.apache.flink.mesos.scheduler

import java.util.{List => JList, Random}

import org.apache.flink.configuration.ConfigConstants._
import org.apache.flink.configuration.{Configuration, GlobalConfiguration}
import org.apache.flink.runtime.StreamingMode
import org.apache.flink.runtime.jobmanager.{JobManager, JobManagerMode}
import org.apache.mesos.Protos.TaskState._
import org.apache.mesos.Protos._
import org.apache.mesos.{Scheduler, SchedulerDriver}
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.util.Try

class FlinkScheduler extends Scheduler with SchedulerUtils {

  val LOG = LoggerFactory.getLogger(classOf[FlinkScheduler])
  val jobManager: Option[Thread] = None
  var currentConfiguration: Option[Configuration] = None
  var taskManagers: Set[TaskID] = Set()
  var taskmanagerCount = 0

  override def offerRescinded(driver: SchedulerDriver, offerId: OfferID): Unit = {
    LOG.warn(s"Rescinded offer: ${offerId.getValue}")
  }

  override def disconnected(driver: SchedulerDriver): Unit = {
    LOG.warn("Disconnected from Mesos master.")
  }

  override def reregistered(driver: SchedulerDriver, masterInfo: MasterInfo): Unit = {
    LOG.info(s"Re-registered with master $masterInfo")
  }

  override def slaveLost(driver: SchedulerDriver, slaveId: SlaveID): Unit = {
    LOG.warn(s"Slave lost: ${slaveId.getValue}")
  }

  override def error(driver: SchedulerDriver, message: String): Unit = {
    LOG.error(s"Error from scheduler driver: $message")
  }

  override def frameworkMessage(driver: SchedulerDriver, executorId: ExecutorID, slaveId: SlaveID, data: Array[Byte]): Unit = {
    val taskID = TaskID.parseFrom(data)
    if (taskID != null) {
      LOG.debug(s"Received heartbeat from taskID: ${taskID.getValue} running at executor: ${executorId.getValue} on slave: ${slaveId.getValue}")

      // add to list if not already known else just log it (at debug level)
      taskManagers.find(_.getValue == taskID.getValue) match {
        case Some(t) =>
          LOG.debug(s"Heartbeat from known taskManager: ${taskID.getValue}")
        case None =>
          LOG.info(s"Heartbeat from unknown taskManager: ${taskID.getValue}, adding them to tracked list")
          taskManagers += taskID
      }
    } else {
      LOG.warn(s"Unrecognized framework message received from executor: ${executorId.getValue} on slave: ${slaveId.getValue}")
    }
  }

  override def registered(driver: SchedulerDriver, frameworkId: FrameworkID, masterInfo: MasterInfo): Unit = {
    LOG.info(s"Registered as ${frameworkId.getValue} with master $masterInfo")
  }

  override def executorLost(driver: SchedulerDriver, executorId: ExecutorID, slaveId: SlaveID, status: Int): Unit = {
    LOG.warn(s"Executor ${executorId.getValue} lost with status $status on slave $slaveId")
  }

  override def statusUpdate(driver: SchedulerDriver, status: TaskStatus): Unit = {
    LOG.info(s"Status update of ${status.getTaskId.getValue} to ${status.getState.name()} with message ${status.getMessage}")
    status.getState match {
      case TASK_FAILED | TASK_FINISHED | TASK_KILLED | TASK_LOST =>
        LOG.info(s"We lost the taskManager with TaskId: ${status.getTaskId.getValue} on slave: ${status.getSlaveId.getValue}")
        // remove from taskManagers map
        taskManagers -= status.getTaskId
      case _ =>
        LOG.debug(s"Ignorable TaskStatus received: ${status.getState.name()}")
    }
  }

  override def resourceOffers(driver: SchedulerDriver, offers: JList[Offer]): Unit = {
    val maxTaskManagers = GlobalConfiguration.getInteger(TASK_MANAGER_COUNT_KEY, DEFAULT_TASK_MANAGER_COUNT)
    val requiredMem = GlobalConfiguration.getInteger(TASK_MANAGER_MEM_KEY, DEFAULT_TASK_MANAGER_MEM)
    val requiredCPU = GlobalConfiguration.getFloat(TASK_MANAGER_CPU_KEY, DEFAULT_TASK_MANAGER_CPU.toFloat)
    val requiredDisk = GlobalConfiguration.getInteger(TASK_MANAGER_DISK_KEY, DEFAULT_TASK_MANGER_DISK)
    val attributeConstraints = parseConstraintString(GlobalConfiguration.getString(TASK_MANAGER_OFFER_ATTRIBUTES_KEY, null))
    val role = GlobalConfiguration.getString(TASK_MANAGER_OFFER_ROLE_KEY, DEFAULT_TASK_MANAGER_OFFER_ROLE)
    val uberJarLocation = GlobalConfiguration.getString(UBERJAR_LOCATION, null)

    // we will combine all resources from te same slave and then launch a single task rather than one per offer
    // this way we have better utilization and less memory wasted on overhead.
    val offersBySlaveId: Map[SlaveID, mutable.Buffer[Offer]] = offers.groupBy(_.getSlaveId)
    for ((slaveId, offers) <- offersBySlaveId) {
      val totalMemory = offers.flatMap(_.getResourcesList.filter(x => x.getName == "mem" && x.getRole == role).map(_.getScalar.getValue)).sum
      val totalCPU = offers.flatMap(_.getResourcesList.filter(x => x.getName == "cpus" && x.getRole == role).map(_.getScalar.getValue)).sum
      val totalDisk = offers.flatMap(_.getResourcesList.filter(x => x.getName == "disk" && x.getRole == role).map(_.getScalar.getValue)).sum
      val portRanges = offers.flatMap(_.getResourcesList.filter(x => x.getName == "ports" && x.getRole == role).flatMap(_.getRanges.getRangeList))
      val ports = getNPortsFromPortRanges(2, portRanges)
      val offerAttributes = toAttributeMap(offers.flatMap(_.getAttributesList))

      // check if all constraints are satisfield
      //  1. Attribute constraints
      //  2. Memory requirements
      //  3. CPU requirements
      //  4. Port requirements
      val meetsRequirements = totalCPU >= requiredCPU &&
        totalMemory >= requiredMem &&
        totalDisk >= requiredDisk &&
        ports.size == 2 &&
        matchesAttributeRequirements(attributeConstraints, offerAttributes)

      val tasksToLaunch: Seq[TaskInfo] = if (!meetsRequirements) {
        LOG.info(
          s"""Declining offer because host/port combination is in use:
              |  cpus: offered $totalCPU needed $requiredCPU
              |  mem : offered $totalMemory needed $requiredMem
              |  disk: offered $totalDisk needed $requiredDisk
              |  ports: $ports""".stripMargin)

        Seq()
      } else {
        // create task Id
        taskmanagerCount += 1
        val taskId = TaskID.newBuilder().setValue(s"TaskManager_$taskmanagerCount").build()

        val artifactURIs: Set[String] = Set(
          uberJarLocation
          // http://path/to/log4j-console.properties
        )
        val command = createTaskManagerCommand(totalMemory.toInt)
        val taskInfo = createTaskInfo("taskManager", taskId, slaveId,
          role, requiredMem, requiredCPU, requiredDisk, ports,
          command, artifactURIs, currentConfiguration.get)

        Seq(taskInfo)
      }

      // launch tasks for these offers (if any)
      driver.launchTasks(offers.map(_.getId), tasksToLaunch)
    }
  }

  def startJobManager(hostName: String): Unit = {
    val conf = GlobalConfiguration.getConfiguration
    val executionMode = JobManagerMode.CLUSTER
    val listeningHost = conf.getString(JOB_MANAGER_IPC_ADDRESS_KEY, hostName)
    val listeningPort = conf.getInteger(JOB_MANAGER_IPC_PORT_KEY, DEFAULT_JOB_MANAGER_IPC_PORT)
    val streamingMode = conf.getString(JOB_MANAGER_STREAMING_MODE_KEY, DEFAULT_JOB_MANAGER_STREAMING_MODE) match {
      case "streaming" => StreamingMode.STREAMING
      case _ => StreamingMode.BATCH_ONLY
    }

    currentConfiguration = Some(conf)

    // start jobManager
    Try(JobManager.runJobManager(conf, executionMode, streamingMode, listeningHost, listeningPort))
  }

  private def createTaskManagerCommand(mem: Int): String = {
    val tmJVMHeap = math.round(mem / (1 + JVM_MEM_OVERHEAD_PERCENT_DEFAULT))
    val tmJVMArgs = GlobalConfiguration.getString(TASK_MANAGER_JVM_ARGS_KEY, DEFAULT_TASK_MANAGER_JVM_ARGS)

    createJavaExecCommand(
      jvmArgs = s"$tmJVMArgs -Xmx${tmJVMHeap}M",
      classToExecute = "org.apache.flink.mesos.executors.FlinkTMExecutor")
  }

  private def getNPortsFromPortRanges(n: Int, portRanges: Seq[Value.Range]): Set[Int] = {
    var ports: Set[Int] = Set()

    // find some ports
    for (x <- portRanges) {
      var begin = x.getBegin
      val end = x.getEnd
      while (begin < end && ports.size < n) {
        ports += (begin + (end - begin) * new Random().nextLong()).toInt
        begin += 1
      }
      if (ports.size == n) {
        return ports
      }
    }

    ports
  }
}
