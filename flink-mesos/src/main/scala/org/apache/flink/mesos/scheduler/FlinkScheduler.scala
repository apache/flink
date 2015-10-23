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

package org.apache.flink.mesos.scheduler

import java.io.File
import java.util.{List => JList}

import org.apache.flink.configuration.{Configuration, GlobalConfiguration}
import org.apache.mesos.Protos.TaskState._
import org.apache.mesos.Protos._
import org.apache.mesos.{Scheduler, SchedulerDriver}
import org.slf4j.LoggerFactory
import scopt.OptionParser

import scala.collection.JavaConversions._

object FlinkScheduler extends Scheduler with SchedulerUtils {

  val LOG = LoggerFactory.getLogger(FlinkScheduler.getClass)
  var jobManager: Option[Thread] = None
  var currentConfiguration: Option[Configuration] = None
  var taskManagers: Set[RunningTaskManager] = Set()
  var taskManagerCount = 0

  override def offerRescinded(driver: SchedulerDriver, offerId: OfferID): Unit = { }

  override def disconnected(driver: SchedulerDriver): Unit = { }

  override def reregistered(driver: SchedulerDriver, masterInfo: MasterInfo): Unit = { }

  override def slaveLost(driver: SchedulerDriver, slaveId: SlaveID): Unit = {
    LOG.warn(s"Slave lost: ${slaveId.getValue}, removing all task managers matching slaveId")
    taskManagers = taskManagers.filter(_.slaveId != slaveId)
  }

  override def error(driver: SchedulerDriver, message: String): Unit = { }

  override def frameworkMessage(driver: SchedulerDriver, executorId: ExecutorID,
                                slaveId: SlaveID, data: Array[Byte]): Unit = { }

  override def registered(driver: SchedulerDriver, frameworkId: FrameworkID,
                          masterInfo: MasterInfo): Unit = { }

  override def executorLost(driver: SchedulerDriver, executorId: ExecutorID,
                            slaveId: SlaveID, status: Int): Unit = {
    LOG.warn(s"Executor ${executorId.getValue} lost with status $status on slave $slaveId")
  }

  override def statusUpdate(driver: SchedulerDriver, status: TaskStatus): Unit = {
    val taskId = status.getTaskId.getValue
    val slaveId = status.getSlaveId.getValue
    LOG.info(
      s"statusUpdate received from taskId: $taskId slaveId: $slaveId [${status.getState.name()}]")

    status.getState match {
      case TASK_FAILED | TASK_FINISHED | TASK_KILLED | TASK_LOST | TASK_ERROR =>
        LOG.info(s"Lost taskManager with TaskId: $taskId on slave: $slaveId")
        taskManagers = taskManagers.filter(_.taskId != status.getTaskId)
      case _ =>
        LOG.debug(s"No action to take for statusUpdate ${status.getState.name()}")
    }
  }

  override def resourceOffers(driver: SchedulerDriver, offers: JList[Offer]): Unit = {
    // we will combine all resources from te same slave and then launch a single task rather
    // than one per offer this way we have better utilization and less memory wasted on overhead.
    for((slaveId, offers) <- offers.groupBy(_.getSlaveId)) {
      val tasks = constructTaskInfoFromOffers(slaveId, offers.toList)
      driver.launchTasks(offers.map(_.getId), tasks)
    }
  }

  def constructTaskInfoFromOffers(slaveId: SlaveID, offers: List[Offer]): Seq[TaskInfo] = {
    val maxTaskManagers = GlobalConfiguration.getInteger(
      TASK_MANAGER_COUNT_KEY, DEFAULT_TASK_MANAGER_COUNT)
    val requiredMem = GlobalConfiguration.getFloat(
      TASK_MANAGER_MEM_KEY, DEFAULT_TASK_MANAGER_MEM)
    val requiredCPU = GlobalConfiguration.getFloat(
      TASK_MANAGER_CPU_KEY, DEFAULT_TASK_MANAGER_CPU.toFloat)
    val requiredDisk = GlobalConfiguration.getFloat(
      TASK_MANAGER_DISK_KEY, DEFAULT_TASK_MANGER_DISK)
    val attributeConstraints = parseConstraintString(GlobalConfiguration.getString(
      TASK_MANAGER_OFFER_ATTRIBUTES_KEY, DEFAULT_TASK_MANAGER_OFFER_ATTRIBUTES))
    val role = GlobalConfiguration.getString(
      MESOS_FRAMEWORK_ROLE_KEY, DEFAULT_MESOS_FRAMEWORK_ROLE)
    val uri = GlobalConfiguration.getString(
      FLINK_UBERJAR_LOCATION_KEY, null)
    val nativeLibPath = GlobalConfiguration.getString(
      MESOS_NATIVE_JAVA_LIBRARY_KEY, DEFAULT_MESOS_NATIVE_JAVA_LIBRARY)

    // combine offers into a single chunk
    val totalMemory = offers.flatMap(_.getResourcesList
      .filter(x => x.getName == "mem" && x.getRole == role)
      .map(_.getScalar.getValue)).sum

    val totalCPU = offers.flatMap(_.getResourcesList
      .filter(x => x.getName == "cpus" && x.getRole == role)
      .map(_.getScalar.getValue)).sum

    val totalDisk = offers.flatMap(_.getResourcesList
      .filter(x => x.getName == "disk" && x.getRole == role)
      .map(_.getScalar.getValue)).sum

    val portRanges = offers.flatMap(_.getResourcesList
      .filter(x => x.getName == "ports" && x.getRole == role)
      .flatMap(_.getRanges.getRangeList))

    val ports = getNPortsFromPortRanges(2, portRanges)

    val offerAttributes = toAttributeMap(offers.flatMap(_.getAttributesList))

    // check if all constraints are satisfield
    //  0. We need more task managers
    //  1. Attribute constraints
    //  2. Memory requirements
    //  3. CPU requirements
    //  4. Port requirements
    val meetsRequirements =
      taskManagers.size < maxTaskManagers &&
      totalCPU >= requiredCPU &&
      totalMemory >= requiredMem &&
      totalDisk >= requiredDisk &&
      ports.size == 2 &&
      matchesAttributeRequirements(attributeConstraints, offerAttributes)

    LOG.info( if(meetsRequirements) "Accepting" else "Declining " +
      s"offer(s) from slave ${slaveId.getValue} " +
      s"offered [cpus: $totalCPU | mem : $totalMemory | disk: $totalDisk] " +
      s"required [cpus: $requiredCPU | mem: $requiredMem | disk: $requiredDisk]")

    if (meetsRequirements) {
      // create task Id
      taskManagerCount += 1

      // get flink base dir
      val basename = uri.split('/').last.split('.').head

      // create executor
      val command = s"cd ${basename}*;" + createTaskManagerCommand(requiredMem.toInt)
      val executorInfo = createExecutorInfo(s"$taskManagerCount", role,
        uri, command, nativeLibPath)

      // create task
      val taskId = TaskID.newBuilder().setValue(s"TaskManager_$taskManagerCount").build()
      val taskInfo = createTaskInfo(
        "taskManager", taskId, slaveId, role, requiredMem,
        requiredCPU, requiredDisk, ports, executorInfo, currentConfiguration.get)

      Seq(taskInfo)
    } else {
      Seq()
    }
  }

  def main(args: Array[String]) {

    // create parser for the commandline args
    val parser = new OptionParser[Conf]("flink-mesos-scheduler") {
      head("Flink Mesos Framework")
      opt[File]('c', "confDir") required() valueName "<directory path>" action { (path, conf) =>
        conf.copy(confDir = path.toPath.toAbsolutePath.toString) } text "confDir is required"
    }

    // parse the config
    val cliConf = parser.parse(args, Conf()) match {
      case Some(c) => c
      case None => sys.exit(-1)
    }

    // startup checks
    checkEnvironment(args)

    LOG.info(s"Loading configuration from ${cliConf.confDir}")
    GlobalConfiguration.loadConfiguration(cliConf.confDir)

    // start job manager thread
    val jobManagerThread = createJobManagerThread(cliConf.host)
    jobManager = Some(jobManagerThread)
    jobManagerThread.start()

    // start scheduler
    val scheduler = this
    val (fwInfo, creds) = createFrameworkInfoAndCredentials(cliConf)
    val driver = createDriver(scheduler, fwInfo, creds)

    try {
      sys.exit(if (driver.run eq Status.DRIVER_STOPPED) 0 else 1)
    } catch {
      case _: Throwable => sys.exit(-1)
    }
  }
}
