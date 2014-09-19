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

import akka.actor.ActorRef
import org.apache.flink.runtime.ActorLogMessages
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID
import org.apache.flink.runtime.jobmanager.TestingTaskManagerMessages.{ResponseRunningTasks, RequestRunningTasks}
import org.apache.flink.runtime.messages.TaskManagerMessages.UnregisterTask
import org.apache.flink.runtime.taskmanager.TestingTaskManagerMessages.NotifyWhenTaskRemoved

trait TestingTaskManager extends ActorLogMessages {
  self: TaskManager =>

  val waitForRemoval = scala.collection.mutable.HashMap[ExecutionAttemptID, Set[ActorRef]]()

  abstract override def receiveWithLogMessages = {
    receiveTestMessages orElse super.receiveWithLogMessages
  }

  def receiveTestMessages: Receive = {
    case RequestRunningTasks =>
      sender() ! ResponseRunningTasks(runningTasks.toMap)
    case NotifyWhenTaskRemoved(executionID) =>
      runningTasks.get(executionID) match {
        case Some(_) =>
          val set = waitForRemoval.getOrElse(executionID, Set())
          waitForRemoval += (executionID -> (set + sender()))
        case None => sender() ! true
      }
    case UnregisterTask(executionID) =>
      super.receiveWithLogMessages(UnregisterTask(executionID))
      waitForRemoval.get(executionID) match {
        case Some(actors) => for(actor <- actors) actor ! true
        case None =>
      }
  }
}