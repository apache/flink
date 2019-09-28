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

import grizzled.slf4j.Logger

import akka.actor.{Actor, FSM, Props}
import org.apache.flink.configuration.Configuration
import org.apache.flink.mesos.scheduler.ReconciliationCoordinator.Reconcile
import org.apache.flink.mesos.scheduler.TaskMonitor._
import org.apache.flink.mesos.scheduler.messages.{Connected, Disconnected, StatusUpdate}
import org.apache.mesos.Protos.TaskState._
import org.apache.mesos.{SchedulerDriver, Protos}

import scala.PartialFunction.empty
import scala.concurrent.duration._

/**
  * Monitors a Mesos task throughout its lifecycle.
  *
  * Models a task with a state machine reflecting the perceived state of the task in Mesos.
  * The state is primarily updated when task status information arrives from Mesos.
  *
  * The associated state data primarily tracks the task's goal (intended) state, as
  * persisted by the scheduler. Keep in mind that goal state is persisted before actions are taken.
  * The goal state strictly transitions thru New->Launched->Released.
  *
  * Unlike most exchanges with Mesos, task status is delivered at-least-once,
  * so status handling should be idempotent.
  */
class TaskMonitor(
    flinkConfig: Configuration,
    schedulerDriver: SchedulerDriver,
    goalState: TaskGoalState) extends Actor with FSM[TaskMonitorState,StateData] {

  val LOG = Logger(getClass)

  startWith(Suspended, StateData(goalState))

  // ------------------------------------------------------------------------
  //  Suspended State
  // ------------------------------------------------------------------------

  when(Suspended) {
    case Event(update: TaskGoalStateUpdated, _) =>
      stay() using StateData(update.state)
    case Event(msg: StatusUpdate, _) =>
      stay()
    case Event(msg: Connected, StateData(goal: New)) =>
      goto(New)
    case Event(msg: Connected, StateData(goal: Launched)) =>
      goto(Reconciling)
    case Event(msg: Connected, StateData(goal: Released)) =>
      goto(Killing)
  }

  // ------------------------------------------------------------------------
  //  New State
  // ------------------------------------------------------------------------

  when(New) {
    case Event(TaskGoalStateUpdated(goal: Launched), _) =>
      goto(Staging) using StateData(goal)
  }

  // ------------------------------------------------------------------------
  //  Reconciliation State
  // ------------------------------------------------------------------------

  onTransition {
    case _ -> Reconciling =>
      nextStateData.goal match {
        case goal: Launched =>
          val taskStatus = Protos.TaskStatus.newBuilder()
            .setTaskId(goal.taskID).setSlaveId(goal.slaveID).setState(TASK_STAGING).build()
          context.parent ! Reconcile(Seq(taskStatus))
        case _ =>
      }
  }

  when(Reconciling) {
    empty
  }

  // ------------------------------------------------------------------------
  //  Staging State
  // ------------------------------------------------------------------------

  when(Staging, stateTimeout = LAUNCH_TIMEOUT) {
    case Event(StateTimeout, _) =>
      LOG.warn(s"Mesos task ${stateData.goal.taskID.getValue} didn't launch as expected;"
        + s" reconciling.")

      // likely cause: the task launch message was dropped - docs suggest reconciliation
      goto(Reconciling)
  }

  // ------------------------------------------------------------------------
  //  Running State
  // ------------------------------------------------------------------------

  when(Running) {
    empty
  }

  // ------------------------------------------------------------------------
  //  Killing State
  // ------------------------------------------------------------------------

  onTransition {
    case _ -> Killing =>
      schedulerDriver.killTask(nextStateData.goal.taskID)
  }

  when(Killing, stateTimeout = RETRY_INTERVAL) {

    case Event(TaskGoalStateUpdated(goal: Released), _) =>
      stay() using StateData(goal)

    case Event(StateTimeout, _) =>
      // retry kill command
      LOG.info(s"Re-attempting to kill Mesos task ${stateData.goal.taskID.getValue}.")
      schedulerDriver.killTask(stateData.goal.taskID)
      stay()
  }

  // ------------------------------------------------------------------------
  //  Common Handling
  // ------------------------------------------------------------------------

  whenUnhandled {

    case Event(msg: Disconnected, _) =>
      goto(Suspended)

    case Event(TaskGoalStateUpdated(goal: Released), _) =>
      goto(Killing) using StateData(goal)

    case Event(msg: StatusUpdate, StateData(goal: Launched)) =>
      LOG.debug(s"Status update received for Mesos task ${goal.taskID.getValue}: $msg")
      msg.status().getState match {
        case TASK_STAGING | TASK_STARTING => goto(Staging)
        case TASK_RUNNING =>
          LOG.info(s"Mesos task ${goal.taskID.getValue} is running.")
          goto(Running)
        case TASK_FINISHED | TASK_LOST | TASK_FAILED | TASK_KILLED | TASK_ERROR =>
          LOG.warn(s"Mesos task ${goal.taskID.getValue} failed unexpectedly.")
          context.parent ! TaskTerminated(goal.taskID, msg.status())
          stop()
        case TASK_KILLING => stay()
      }

    case Event(msg: StatusUpdate, StateData(goal: Released)) =>
      LOG.debug(s"Status update received for Mesos task ${goal.taskID.getValue}: $msg")
      msg.status().getState match {
        case TASK_STAGING | TASK_STARTING | TASK_RUNNING =>
          LOG.info(s"Mesos task ${goal.taskID.getValue} is running unexpectedly; killing.")
          goto(Killing)
        case TASK_FINISHED | TASK_LOST | TASK_FAILED | TASK_KILLED | TASK_ERROR =>
          LOG.info(s"Mesos task ${goal.taskID.getValue} exited as planned.")
          context.parent ! TaskTerminated(goal.taskID, msg.status())
          stop()
        case TASK_KILLING => stay()
      }
  }

  onTransition {
    case previousState -> nextState =>
      LOG.debug(s"State change ($previousState -> $nextState) with data ${nextStateData}")
  }

  initialize()
}

object TaskMonitor {

  val RETRY_INTERVAL = (5 seconds)
  val LAUNCH_TIMEOUT = (30 seconds)

  // ------------------------------------------------------------------------
  // State
  // ------------------------------------------------------------------------

  /**
    * An FSM state of the task monitor, roughly corresponding to the task status.
    */
  sealed trait TaskMonitorState
  case object Suspended extends TaskMonitorState
  case object New extends TaskMonitorState
  case object Reconciling extends TaskMonitorState
  case object Staging extends TaskMonitorState
  case object Running extends TaskMonitorState
  case object Killing extends TaskMonitorState

  /**
    * The task monitor state data.
    * @param goal the goal (intentional) state of the task.
    */
  case class StateData(goal:TaskGoalState)

  /**
    * Indicates the goal (intentional) state of a Mesos task; behavior varies accordingly.
    */
  sealed trait TaskGoalState {
    val taskID: Protos.TaskID
  }
  case class New(taskID: Protos.TaskID) extends TaskGoalState
  case class Launched(taskID: Protos.TaskID, slaveID: Protos.SlaveID) extends TaskGoalState
  case class Released(taskID: Protos.TaskID, slaveID: Protos.SlaveID) extends TaskGoalState


  // ------------------------------------------------------------------------
  //  Messages
  // ------------------------------------------------------------------------

  /**
    * Conveys an update to the goal (intentional) state of a given task.
    */
  case class TaskGoalStateUpdated(state: TaskGoalState)

  /**
    * Indicates that the Mesos task has terminated for whatever reason.
    */
  case class TaskTerminated(taskID: Protos.TaskID, status: Protos.TaskStatus)

  // ------------------------------------------------------------------------
  //  Utils
  // ------------------------------------------------------------------------

  /**
    * Creates the properties for the TaskMonitor actor.
    *
    * @param actorClass the task monitor actor class
    * @param flinkConfig the Flink configuration
    * @param schedulerDriver the Mesos scheduler driver
    * @param goalState the task's goal state
    * @tparam T the type of the task monitor actor class
    * @return the Props to create the task monitor
    */
  def createActorProps[T <: TaskMonitor](actorClass: Class[T],
                                         flinkConfig: Configuration,
                                         schedulerDriver: SchedulerDriver,
                                         goalState: TaskGoalState): Props = {
    Props.create(actorClass, flinkConfig, schedulerDriver, goalState)
  }
}
