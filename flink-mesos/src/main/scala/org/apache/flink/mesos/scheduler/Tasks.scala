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

import akka.actor.SupervisorStrategy.Escalate
import akka.actor._
import org.apache.flink.configuration.Configuration
import org.apache.flink.mesos.scheduler.ReconciliationCoordinator.Reconcile
import org.apache.flink.mesos.scheduler.TaskMonitor._
import org.apache.flink.mesos.scheduler.messages._
import org.apache.mesos.{SchedulerDriver, Protos}

import scala.collection.mutable.{Map => MutableMap}

/**
  * Aggregate of monitored tasks.
  *
  * Routes messages between the scheduler and individual task monitor actors.
  */
class Tasks(
     manager: ActorRef,
     flinkConfig: Configuration,
     schedulerDriver: SchedulerDriver,
     taskMonitorCreator: (ActorRefFactory,TaskGoalState) => ActorRef) extends Actor {

  /**
    * A map of task monitors by task ID.
    */
  private[scheduler] val taskMap: MutableMap[Protos.TaskID,ActorRef] = MutableMap()

  /**
    * Cache of current connection state.
    */
  private[scheduler] var registered: Option[Any] = None

  override def supervisorStrategy: SupervisorStrategy = AllForOneStrategy() {
    case _ => Escalate
  }

  override def receive: Receive = {

    case msg: Disconnected =>
      registered = None
      context.actorSelection("*").tell(msg, self)

    case msg : Connected =>
      registered = Some(msg)
      context.actorSelection("*").tell(msg, self)

    case msg: TaskGoalStateUpdated =>
      val taskID = msg.state.taskID

      taskMap.get(taskID) match {
        case None =>
          // create a new actor to monitor the task, with the appropriate initial goal state
          val actorRef = createTask(msg.state)
          registered.foreach(actorRef ! _)

        case Some(actor) =>
          // tell the actor to update its goal state
          actor ! msg
      }

    case msg: StatusUpdate =>
      taskMap.get(msg.status().getTaskId) match {
        case Some(ref) =>
          // tell the actor about the status change
          ref ! msg
        case None =>
          // a status update was received for an unrecognized task, which may occur
          // when a task is resurrected (i.e. the Mesos master isn't using a strict registry).
          // see the Mesos reconciliation guide for more information.

          // create a monitor to reliably terminate the resurrected task
          val actorRef = createTask(Released(msg.status().getTaskId, msg.status().getSlaveId))
          registered.foreach(actorRef ! _)
      }

    case msg: Reconcile =>
      manager.forward(msg)

    case msg: TaskTerminated =>
      taskMap.remove(msg.taskID)
      manager.forward(msg)
  }

  private def createTask(task: TaskGoalState): ActorRef = {
    val actorRef = taskMonitorCreator(context, task)
    taskMap.put(task.taskID, actorRef)
    actorRef
  }
}

object Tasks {

  /**
    * Create a tasks actor.
    */
  def createActorProps[T <: Tasks, M <: TaskMonitor](
      actorClass: Class[T],
      manager: ActorRef,
      flinkConfig: Configuration,
      schedulerDriver: SchedulerDriver,
      taskMonitorClass: Class[M]): Props = {

    val taskMonitorCreator = (factory: ActorRefFactory, task: TaskGoalState) => {
      val props = TaskMonitor.createActorProps(taskMonitorClass, flinkConfig, schedulerDriver, task)
      factory.actorOf(props)
    }

    Props.create(actorClass, manager, flinkConfig, schedulerDriver, taskMonitorCreator)
  }
}
