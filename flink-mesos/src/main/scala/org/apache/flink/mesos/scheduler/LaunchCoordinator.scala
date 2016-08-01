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

import akka.actor.{Actor, ActorRef, FSM, Props}
import com.netflix.fenzo._
import com.netflix.fenzo.functions.Action1
import com.netflix.fenzo.plugins.VMLeaseObject
import grizzled.slf4j.Logger
import org.apache.flink.api.java.tuple.{Tuple2=>FlinkTuple2}
import org.apache.flink.configuration.Configuration
import org.apache.flink.mesos.scheduler.LaunchCoordinator._
import org.apache.flink.mesos.scheduler.messages._
import org.apache.mesos.Protos.TaskInfo
import org.apache.mesos.{SchedulerDriver, Protos}

import scala.collection.JavaConverters._
import scala.collection.mutable.{Map => MutableMap}
import scala.concurrent.duration._

/**
  * The launch coordinator handles offer processing, including
  * matching offers to tasks and making reservations.
  *
  * The coordinator uses Netflix Fenzo to optimize task placement.   During the GatheringOffers phase,
  * offers are evaluated by Fenzo for suitability to the planned tasks.   Reservations are then placed
  * against the best offers, leading to revised offers containing reserved resources with which to launch task(s).
  */
class LaunchCoordinator(
    manager: ActorRef,
    config: Configuration,
    schedulerDriver: SchedulerDriver,
    optimizerBuilder: TaskSchedulerBuilder
  ) extends Actor with FSM[TaskState, GatherData] {

  val LOG = Logger(getClass)

  /**
    * The task placement optimizer.
    *
    * The optimizer contains the following state:
    *  - unused offers
    *  - existing task placement (for fitness calculation involving task colocation)
    */
  private[mesos] val optimizer: TaskScheduler = {
    optimizerBuilder
      .withLeaseRejectAction(new Action1[VirtualMachineLease]() {
        def call(lease: VirtualMachineLease) {
          LOG.info(s"Declined offer ${lease.getId} from ${lease.hostname()} of ${lease.memoryMB()} MB, ${lease.cpuCores()} cpus.")
          schedulerDriver.declineOffer(lease.getOffer.getId)
        }
      }).build
  }

  override def postStop(): Unit = {
    optimizer.shutdown()
    super.postStop()
  }

  /**
    * Initial state
    */
  startWith(Suspended, GatherData(tasks = Nil, newLeases = Nil))

  /**
    * State: Suspended
    *
    * Wait for (re-)connection to Mesos.   No offers exist in this state, but outstanding tasks might.
    */
  when(Suspended) {
    case Event(msg: Connected, data: GatherData) =>
      if(data.tasks.nonEmpty) goto(GatheringOffers)
      else goto(Idle)
  }

  /**
    * State: Idle
    *
    * Wait for a task request to arrive, then transition into gathering offers.
    */
  onTransition {
    case _ -> Idle => assert(nextStateData.tasks.isEmpty)
  }

  when(Idle) {
    case Event(msg: Disconnected, data: GatherData) =>
      goto(Suspended)

    case Event(offers: ResourceOffers, data: GatherData) =>
      // decline any offers that come in
      schedulerDriver.suppressOffers()
      for(offer <- offers.offers().asScala) { schedulerDriver.declineOffer(offer.getId) }
      stay()

    case Event(msg: Launch, data: GatherData) =>
      goto(GatheringOffers) using data.copy(tasks = data.tasks ++ msg.tasks.asScala)
  }

  /**
    * Transition logic to control the flow of offers.
    */
  onTransition {
    case _ -> GatheringOffers =>
      LOG.info(s"Now gathering offers for at least ${nextStateData.tasks.length} task(s).")
      schedulerDriver.reviveOffers()

    case GatheringOffers -> _ =>
      // decline any outstanding offers and suppress future offers
      LOG.info(s"No longer gathering offers; all requests fulfilled.")

      assert(nextStateData.newLeases.isEmpty)
      schedulerDriver.suppressOffers()
      optimizer.expireAllLeases()
  }

  /**
    * State: GatheringOffers
    *
    * Wait for offers to accumulate for a fixed length of time or from specific slaves.
    *
    * While gathering offers, other task requests may safely arrive.
    */
  when(GatheringOffers, stateTimeout = GATHER_DURATION) {

    case Event(msg: Disconnected, data: GatherData) =>
      // reconciliation spec: offers are implicitly declined upon disconnect
      goto(Suspended) using data.copy(newLeases = Nil)

    case Event(offers: ResourceOffers, data: GatherData) =>
      val leases = offers.offers().asScala.map(new VMLeaseObject(_).asInstanceOf[VirtualMachineLease])
      if(LOG.isInfoEnabled) {
        val (cpus, mem) = leases.foldLeft((0.0,0.0)) { (z,o) => (z._1 + o.cpuCores(), z._2 + o.memoryMB()) }
        LOG.info(s"Received offer(s) of $mem MB, $cpus cpus:")
        for(lease <- leases) {
          LOG.info(s"  ${lease.getId} from ${lease.hostname()} of ${lease.memoryMB()} MB, ${lease.cpuCores()} cpus")
        }
      }
      stay using data.copy(newLeases = data.newLeases ++ leases)

    case Event(StateTimeout, data: GatherData) =>
      val remaining = MutableMap(data.tasks.map(t => t.taskRequest.getId -> t):_*)

      LOG.info(s"Processing ${remaining.size} task(s) against ${data.newLeases.length} new offer(s) plus outstanding offers.")

      // attempt to assign the outstanding tasks using the optimizer
      val result = optimizer.scheduleOnce(
        data.tasks.map(_.taskRequest).asJava, data.newLeases.asJava)

      if(LOG.isInfoEnabled) {
        // note that vmCurrentStates are computed before any actions taken (incl. expiration)
        LOG.info("Resources considered: (note: expired offers not deducted from below)")
        for(vm <- optimizer.getVmCurrentStates.asScala) {
          val lease = vm.getCurrAvailableResources
          LOG.info(s"  ${vm.getHostname} has ${lease.memoryMB()} MB, ${lease.cpuCores()} cpus")
        }
      }
      log.debug(result.toString)

      for((hostname, assignments) <- result.getResultMap.asScala) {

        // process the assignments into a set of operations (reserve and/or launch)
        val slaveId = assignments.getLeasesUsed.get(0).getOffer.getSlaveId
        val offerIds = assignments.getLeasesUsed.asScala.map(_.getOffer.getId)
        val operations = processAssignments(slaveId, assignments, remaining.toMap)

        // update the state to reflect the launched tasks
        val launchedTasks = operations
          .filter(_.getType==Protos.Offer.Operation.Type.LAUNCH)
          .flatMap(_.getLaunch.getTaskInfosList.asScala.map(_.getTaskId))
        for(taskId <- launchedTasks) {
          val task = remaining.remove(taskId.getValue).get
          LOG.debug(s"Assigned task ${task.taskRequest().getId} to host ${hostname}.")
          optimizer.getTaskAssigner.call(task.taskRequest, hostname)
        }

        // send the operations to Mesos via manager
        manager ! new AcceptOffers(hostname, offerIds.asJava, operations.asJava)

        if(LOG.isInfoEnabled) {
          LOG.info(s"Launched ${launchedTasks.length} task(s) on ${hostname} using ${offerIds.length} offer(s):")
          for(offerId <- offerIds) {
            LOG.info(s"  ${offerId.getValue}")
          }
        }
      }

      // stay in GatheringOffers state if any tasks remain, otherwise transition to idle
      if(remaining.isEmpty) {
        goto(Idle) using data.copy(newLeases = Nil, tasks = Nil)
      } else {
        LOG.info(s"Waiting for more offers; ${remaining.size} task(s) are not yet launched.")

        stay using data.copy(newLeases = Nil, tasks = remaining.values.toList) forMax SUBSEQUENT_GATHER_DURATION
      }
  }

  /**
    * Default handling of events.
    */
  whenUnhandled {
    case Event(msg: Launch, data: GatherData) =>
      // accumulate any tasks that come in
      stay using data.copy(tasks = data.tasks ++ msg.tasks.asScala)

    case Event(offer: OfferRescinded, data: GatherData) =>
      // forget rescinded offers
      LOG.info(s"Offer ${offer.offerId()} was rescinded.")
      optimizer.expireLease(offer.offerId().getValue)
      stay using data.copy(newLeases = data.newLeases.filterNot(_.getOffer.getId == offer.offerId()))

    case Event(msg: Assign, _) =>
      // recovering an earlier task assignment
      for(task <- msg.tasks.asScala) {
        LOG.debug(s"Assigned task ${task.f0.getId} to host ${task.f1}.")
        optimizer.getTaskAssigner.call(task.f0, task.f1)
      }
      stay()

    case Event(msg: Unassign, _) =>
      // planning to terminate a task - unassign it from its host in the optimizer's state
      LOG.debug(s"Unassigned task ${msg.taskID} from host ${msg.hostname}.")
      optimizer.getTaskUnAssigner.call(msg.taskID.getValue, msg.hostname)
      stay()
  }

  onTransition {
    case previousState -> nextState =>
      LOG.debug(s"State change ($previousState -> $nextState) with data $nextStateData")
  }

  initialize()
}

object LaunchCoordinator {

  val GATHER_DURATION = 5.seconds
  val SUBSEQUENT_GATHER_DURATION = 5.seconds

  // ------------------------------------------------------------------------
  //  FSM State
  // ------------------------------------------------------------------------

  /**
    * An FSM state of the launch coordinator.
    */
  sealed trait TaskState
  case object GatheringOffers extends TaskState
  case object Idle extends TaskState
  case object Suspended extends TaskState

  /**
    * FSM state data.
    *
    * @param tasks the tasks to launch.
    * @param newLeases new leases not yet handed to the optimizer.
    */
  case class GatherData(tasks: Seq[LaunchableTask] = Nil, newLeases: Seq[VirtualMachineLease] = Nil)

  // ------------------------------------------------------------------------
  //  Messages
  // ------------------------------------------------------------------------

  /**
    * Instructs the launch coordinator to launch some new task.
    */
  case class Launch(tasks: java.util.List[LaunchableTask]) {
    require(tasks.size() >= 1, "Launch message must contain at least one task")
  }

  /**
    * Informs the launch coordinator that some task(s) are assigned to a host (for planning purposes).
    *
    * This is sent by the RM in recovery procedures to recover the optimizer state.  In normal operation,
    * the launch coordinator itself updates the optimizer state.
    */
  case class Assign(tasks: java.util.List[FlinkTuple2[TaskRequest, String]])

  /**
    * Informs the launch coordinator that some task is no longer assigned to a host (for planning purposes).
    */
  case class Unassign(taskID: Protos.TaskID, hostname: String)

  // ------------------------------------------------------------------------
  //  Utils
  // ------------------------------------------------------------------------

  /**
    * Process the given task assignments into a set of Mesos operations.
    *
    * The operations may include reservations and task launches.
    *
    * @param slaveId the slave associated with the given assignments.
    * @param assignments the task assignments as provided by the optimizer.
    * @param allTasks all known tasks, keyed by taskId.
    * @return the operations to perform.
    */
  private def processAssignments(
      slaveId: Protos.SlaveID,
      assignments: VMAssignmentResult,
      allTasks: Map[String, LaunchableTask]): Seq[Protos.Offer.Operation] = {

    def taskInfo(assignment: TaskAssignmentResult): Protos.TaskInfo = {
      allTasks(assignment.getTaskId).launch(slaveId, assignment)
    }

    val launches = Protos.Offer.Operation.newBuilder().setType(Protos.Offer.Operation.Type.LAUNCH).setLaunch(
      Protos.Offer.Operation.Launch.newBuilder().addAllTaskInfos(
        assignments.getTasksAssigned.asScala.map(taskInfo).asJava
      )
    ).build()

    Seq(launches)
  }

  /**
    * Get the configuration properties for the launch coordinator.
    *
    * @param actorClass the launch coordinator actor class.
    * @param flinkConfig the Flink configuration.
    * @param schedulerDriver the Mesos scheduler driver.
    * @tparam T the launch coordinator actor class.
    * @return the Akka props to create the launch coordinator actor.
    */
  def createActorProps[T <: LaunchCoordinator](
    actorClass: Class[T],
    manager: ActorRef,
    flinkConfig: Configuration,
    schedulerDriver: SchedulerDriver,
    optimizerBuilder: TaskSchedulerBuilder): Props = {

    Props.create(actorClass, manager, flinkConfig, schedulerDriver, optimizerBuilder)
  }
}
