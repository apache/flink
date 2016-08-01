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

import akka.actor.{Actor, ActorRef, Props}
import org.apache.flink.configuration.Configuration
import org.apache.flink.mesos.scheduler.ReconciliationCoordinator.Reconcile
import org.apache.flink.mesos.scheduler.TaskMonitor.{TaskGoalState, TaskGoalStateUpdated, TaskTerminated}
import org.apache.flink.mesos.scheduler.Tasks._
import org.apache.flink.mesos.scheduler.messages._
import org.apache.mesos.{SchedulerDriver, Protos}

import scala.collection.mutable.{Map => MutableMap}

/**
  * Aggregate of monitored tasks.
  *
  * Routes messages between the scheduler and individual task monitor actors.
  */
class Tasks[M <: TaskMonitor](
     flinkConfig: Configuration,
     schedulerDriver: SchedulerDriver,
     taskMonitorClass: Class[M]) extends Actor {

  /**
    * A map of task monitors by task ID.
    */
  private val taskMap: MutableMap[Protos.TaskID,ActorRef] = MutableMap()

  /**
    * Cache of current connection state.
    */
  private var registered: Option[Any] = None

  override def preStart(): Unit = {
    // TODO subscribe to context.system.deadLetters for messages to nonexistent tasks
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

      // ensure task monitor exists
      if(!taskMap.contains(taskID)) {
        val actorRef = createTask(msg.state)
        registered.foreach(actorRef ! _)
      }

      taskMap(taskID) ! msg

    case msg: StatusUpdate =>
      taskMap(msg.status().getTaskId) ! msg

    case msg: Reconcile =>
      context.parent.forward(msg)

    case msg: TaskTerminated =>
      context.parent.forward(msg)
  }

  private def createTask(task: TaskGoalState): ActorRef = {
    val actorProps = TaskMonitor.createActorProps(taskMonitorClass, flinkConfig, schedulerDriver, task)
    val actorRef = context.actorOf(actorProps, name = actorName(task.taskID))
    taskMap.put(task.taskID, actorRef)
    actorRef
  }
}

object Tasks {

  /**
    * Extract the actor name for the given task ID.
    */
  def actorName(taskID: Protos.TaskID): String = {
    taskID.getValue
  }

  /**
    * Create a tasks actor.
    */
  def createActorProps[T <: Tasks[M], M <: TaskMonitor](
      actorClass: Class[T],
      flinkConfig: Configuration,
      schedulerDriver: SchedulerDriver,
      taskMonitorClass: Class[M]): Props = {

    Props.create(actorClass, flinkConfig, schedulerDriver, taskMonitorClass)
  }
}
