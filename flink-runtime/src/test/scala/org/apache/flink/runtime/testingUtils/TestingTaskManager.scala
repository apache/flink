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

package org.apache.flink.runtime.testingUtils

import akka.actor.ActorRef
import org.apache.flink.runtime.jobgraph.JobID
import org.apache.flink.runtime.taskmanager.TaskManager
import org.apache.flink.runtime.testingUtils.TestingJobManagerMessages.NotifyWhenJobRemoved
import org.apache.flink.runtime.{ActorLogMessages}
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID
import org.apache.flink.runtime.testingUtils.TestingTaskManagerMessages._
import org.apache.flink.runtime.messages.TaskManagerMessages.UnregisterTask
import scala.concurrent.duration._

trait TestingTaskManager extends ActorLogMessages {
  that: TaskManager =>

  val waitForRemoval = scala.collection.mutable.HashMap[ExecutionAttemptID, Set[ActorRef]]()
  val waitForJobRemoval = scala.collection.mutable.HashMap[JobID, Set[ActorRef]]()

  abstract override def receiveWithLogMessages = {
    receiveTestMessages orElse super.receiveWithLogMessages
  }

  def receiveTestMessages: Receive = {
    
    case RequestRunningTasks =>
      sender ! ResponseRunningTasks(runningTasks.toMap)
      
    case NotifyWhenTaskRemoved(executionID) =>
      runningTasks.get(executionID) match {
        case Some(_) =>
          val set = waitForRemoval.getOrElse(executionID, Set())
          waitForRemoval += (executionID -> (set + sender))
        case None => sender ! true
      }
      
    case UnregisterTask(executionID) =>
      super.receiveWithLogMessages(UnregisterTask(executionID))
      waitForRemoval.get(executionID) match {
        case Some(actors) => for(actor <- actors) actor ! true
        case None =>
      }
      
    case RequestBroadcastVariablesWithReferences => {
      sender ! ResponseBroadcastVariablesWithReferences(
        bcVarManager.getNumberOfVariablesWithReferences)
    }
    
    case NotifyWhenJobRemoved(jobID) => {
      if(runningTasks.values.exists(_.getJobID == jobID)){
        val set = waitForJobRemoval.getOrElse(jobID, Set())
        waitForJobRemoval += (jobID -> (set + sender))
        import context.dispatcher
        context.system.scheduler.scheduleOnce(200 milliseconds, this.self, CheckIfJobRemoved(jobID))
      }else{
        waitForJobRemoval.get(jobID) match {
          case Some(listeners) => (listeners + sender) foreach (_ ! true)
          case None => sender ! true
        }
      }
    }
    
    case CheckIfJobRemoved(jobID) => {
      if(runningTasks.values.forall(_.getJobID != jobID)){
        waitForJobRemoval.get(jobID) match {
          case Some(listeners) => listeners foreach (_ ! true)
          case None =>
        }
      } else {
        import context.dispatcher
        context.system.scheduler.scheduleOnce(200 milliseconds, this.self, CheckIfJobRemoved(jobID))
      }
    }
  }
}
