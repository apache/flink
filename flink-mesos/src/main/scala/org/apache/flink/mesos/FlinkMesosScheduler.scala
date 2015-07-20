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
package org.apache.flink.mesos

import java.util.{List => JList}

import com.google.protobuf.ByteString
import org.apache.flink.configuration.ConfigConstants._
import org.apache.flink.configuration.{ConfigConstants, GlobalConfiguration}
import org.apache.mesos.Protos.{TaskInfo => MesosTaskInfo, _}
import org.apache.mesos.{Scheduler, SchedulerDriver}
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

class FlinkMesosScheduler(conf: Conf) extends Scheduler {
  val LOG = LoggerFactory.getLogger(classOf[TaskManagerExecutor])
  var taskManagers: Map[SlaveID, TaskInfo] = Map()
  var jobManager: Option[TaskInfo] = None
  val slaveOfferConstraints: Map[String, Set[String]] = parseConstraintString(conf.slaveOfferConstraints())

  // offer went away, maybe we should update our internal data structures to reflect that
  override def offerRescinded(driver: SchedulerDriver, offerId: OfferID): Unit = {}

  override def disconnected(driver: SchedulerDriver): Unit = {}

  // maybe we want to do some sort of state reconciliation
  override def reregistered(driver: SchedulerDriver, masterInfo: MasterInfo): Unit = {}

  // unused
  override def slaveLost(driver: SchedulerDriver, slaveId: SlaveID): Unit = {}

  // unused
  override def error(driver: SchedulerDriver, message: String): Unit = {}

  // unused
  override def frameworkMessage(driver: SchedulerDriver, executorId: ExecutorID, slaveId: SlaveID, data: Array[Byte]): Unit = {}

  override def statusUpdate(driver: SchedulerDriver, status: TaskStatus): Unit = {}

  override def registered(driver: SchedulerDriver, frameworkId: FrameworkID, masterInfo: MasterInfo): Unit = {
    val config = GlobalConfiguration.getConfiguration
    val port = offsetPort(config.getInteger(JOB_MANAGER_IPC_PORT_KEY, DEFAULT_JOB_MANAGER_IPC_PORT), frameworkId.hashCode())
    config.setInteger(JOB_MANAGER_IPC_PORT_KEY, port)

    LOG.info(s"Flink was registered: ${frameworkId.getValue} ${masterInfo.getHostname}")
  }

  override def executorLost(driver: SchedulerDriver, executorId: ExecutorID, slaveId: SlaveID, status: Int): Unit = {
  }

  def createCommand(offer: Offer, jobManager: Boolean, taskId: Int): CommandInfo = ???

  override def resourceOffers(driver: SchedulerDriver, offers: JList[Offer]): Unit = {
    val filters = Filters.newBuilder().setRefuseSeconds(5).build()
    for (offer <- offers) {
      val offerAttributes = toAttributeMap(offer.getAttributesList)
      val meetsConstraints = matchesAttributeRequirements(slaveOfferConstraints, offerAttributes)
      val slaveId = offer.getSlaveId.getValue
      val mem = getResource(offer.getResourcesList, "mem")
      val cpus = getResource(offer.getResourcesList, "cpus").toInt
      val id = offer.getId.getValue
      if (jobManager.isEmpty && meetsJobManagerRequirements(offer)) {
        // accept offer and start container
        createJobManagerTask(offer, )
      }
    }
  }

  /**
   * Partition the existing set of resources into two groups, those remaining to be
   * scheduled and those requested to be used for a new task.
   * @param resources The full list of available resources
   * @param resourceName The name of the resource to take from the available resources
   * @param amountToUse The amount of resources to take from the available resources
   * @return The remaining resources list and the used resources list.
   */
  def partitionResources(resources: JList[Resource], resourceName: String, amountToUse: Double): (List[Resource], List[Resource]) = {
    var remain = amountToUse
    var requestedResources = new ArrayBuffer[Resource]
    val remainingResources = resources.map {
      case r => {
        if (remain > 0 &&
          r.getType == Value.Type.SCALAR &&
          r.getScalar.getValue > 0.0 &&
          r.getName == resourceName) {
          val usage = Math.min(remain, r.getScalar.getValue)
          requestedResources += createResource(resourceName, usage, Some(r.getRole))
          remain -= usage
          createResource(resourceName, r.getScalar.getValue - usage, Some(r.getRole))
        } else {
          r
        }
      }
    }

    // Filter any resource that has depleted.
    val filteredResources =
      remainingResources.filter(r => r.getType != Value.Type.SCALAR || r.getScalar.getValue > 0.0)

    (filteredResources.toList, requestedResources.toList)
  }

  /** Build a Mesos resource protobuf object */
  def createResource(name: String, amount: Double, role: Option[String] = None): Resource = {
    val builder = Resource.newBuilder()
      .setName(name)
      .setType(Value.Type.SCALAR)
      .setScalar(Value.Scalar.newBuilder().setValue(amount).build())

    role.foreach { r => builder.setRole(r) }

    builder.build()
  }

  private def createJobManagerTask(offer: Offer, resources: JList[Resource]): TaskInfo = {
    val memory = calculateTotalMemory(getResource(offer.getResourcesList, "mem").toInt, conf)
    LOG.info("JobManager address: " + offer.getHostname)
    LOG.info("JobManager port: " + conf.flinkConfiguration.getInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY, -1))
    conf.flinkConfiguration.setString(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, offer.getHostname)
    val flinkJMCommand = s"java -Xmx${memory}M -cp . org.apache.flink.mesos.executors.FlinkJMExecutor"

    val jobManagerExecutor = createExecutorInfo("jobManager", "JobManager Executor", flinkJMCommand)
    val taskId = TaskID.newBuilder().setValue("jm_task-" + jobManagerExecutor.hashCode).build()
    val result = createTaskInfo("jobManager", resources, jobManagerExecutor, offer.getSlaveId, taskId)
    logLaunchInfo("Jobmanager", offer, resources, result)
    result
  }

  private def logLaunchInfo(name: String, offer: Offer, resources: JList[Resource], task: TaskInfo) {
    LOG.info("---- Launching Flink " + name + "----")
    LOG.info("Hostname: " + offer.getHostname)
    LOG.info("Offer: " + offer.getId.getValue)
    LOG.info("SlaveID: " + offer.getSlaveId.getValue)
    LOG.info("TaskID: " + task.getTaskId.getValue)
    for (resource <- resources) {
      LOG.info(resource.getName + ": " + resource.getScalar.getValue)
    }
  }


  def createExecutorInfo(id: String, name: String, command: String): ExecutorInfo = {
    ExecutorInfo.newBuilder().setExecutorId(ExecutorID.newBuilder().setValue(id))
      .setCommand(CommandInfo.newBuilder().setValue(command))
      .setName(name)
      .setData(ByteString.copyFrom(serialize(conf.flinkConfiguration)))
      .build()
  }

  def createTaskInfo(name: String, resources: JList[Resource], executorInfo: ExecutorInfo, slaveID: SlaveID, taskID: TaskID): TaskInfo = {
    val taskInfo = TaskInfo.newBuilder().setName(name).setTaskId(taskID).setSlaveId(slaveID).setExecutor(executorInfo)
    for (resource <- resources) {
      taskInfo.addResources(resource)
    }
    taskInfo.build()
  }

  def createJobManagerCommand()

  def meetsTaskManagerRequirements(offer: Offer): Boolean = meetsConstraints(conf.taskManagerMem(), conf.taskManagerCores())(offer)

  def meetsJobManagerRequirements(offer: Offer): Boolean = meetsConstraints(conf.jobManagerMem(), conf.jobManagerCores())(offer)

  def meetsConstraints(requiredMem: Int, requiredCPUs: Int)(o: Offer): Boolean = {
    val mem = getResource(o.getResourcesList, "mem")
    val cpus = getResource(o.getResourcesList, "cpus")
    val slaveId = o.getSlaveId.getValue
    val offerAttributes = toAttributeMap(o.getAttributesList)

    // check if all constraints are satisfield
    //  1. Attribute constraints
    //  2. Memory requirements
    //  3. CPU requirements - need at least 1 for executor, 1 for task
    val meetsConstraints = matchesAttributeRequirements(slaveOfferConstraints, offerAttributes)
    val meetsMemoryRequirements = mem >= calculateTotalMemory(requiredMem, conf)
    val meetsCPURequirements = cpus >= requiredCPUs

    val meetsRequirements = meetsConstraints && meetsMemoryRequirements && meetsCPURequirements

    // add some debug messaging
    val debugstr = if (meetsRequirements) "Accepting" else "Declining"
    val id = o.getId.getValue
    LOG.debug(s"$debugstr offer: $id with attributes: $offerAttributes mem: $mem cpu: $cpus")

    meetsConstraints
  }

  def partitionOffersByRequirements(requiredMem: Int, requiredCPUs: Int)(offers: JList[Offer]) = {
    val slaveOfferConstraints: Map[String, Set[String]] = parseConstraintString(conf.slaveOfferConstraints())

    offers.partition { o => meetsConstraints(requiredMem, requiredCPUs)(o) }
  }

  object FlinkMesosScheduler {
    def main(args: Array[String]) {
      val conf = new Conf(args)
    }
  }
