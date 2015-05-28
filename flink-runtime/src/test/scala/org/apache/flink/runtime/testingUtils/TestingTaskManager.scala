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

import akka.actor.{Terminated, ActorRef}
import org.apache.flink.api.common.JobID
import org.apache.flink.runtime.execution.ExecutionState
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID
import org.apache.flink.runtime.instance.InstanceConnectionInfo
import org.apache.flink.runtime.io.disk.iomanager.IOManager
import org.apache.flink.runtime.io.network.NetworkEnvironment
import org.apache.flink.runtime.memorymanager.DefaultMemoryManager
import org.apache.flink.runtime.messages.Messages.Disconnect
import org.apache.flink.runtime.messages.TaskMessages.{TaskInFinalState, UpdateTaskExecutionState}
import org.apache.flink.runtime.taskmanager.{TaskManagerConfiguration, TaskManager}
import org.apache.flink.runtime.testingUtils.TestingJobManagerMessages.NotifyWhenJobRemoved
import org.apache.flink.runtime.testingUtils.TestingMessages.DisableDisconnect
import org.apache.flink.runtime.testingUtils.TestingTaskManagerMessages._

import scala.concurrent.duration._
import scala.language.postfixOps

/**
 * Subclass of the [[TaskManager]] to support testing messages
 */
class TestingTaskManager(config: TaskManagerConfiguration,
                         connectionInfo: InstanceConnectionInfo,
                         jobManagerAkkaURL: String,
                         memoryManager: DefaultMemoryManager,
                         ioManager: IOManager,
                         network: NetworkEnvironment,
                         numberOfSlots: Int)
  extends TaskManager(config, connectionInfo, jobManagerAkkaURL,
                      memoryManager, ioManager, network, numberOfSlots) {

  import scala.collection.JavaConverters._


  val waitForRemoval = scala.collection.mutable.HashMap[ExecutionAttemptID, Set[ActorRef]]()
  val waitForJobRemoval = scala.collection.mutable.HashMap[JobID, Set[ActorRef]]()
  val waitForJobManagerToBeTerminated = scala.collection.mutable.HashMap[String, Set[ActorRef]]()
  val waitForRunning = scala.collection.mutable.HashMap[ExecutionAttemptID, Set[ActorRef]]()
  val unregisteredTasks = scala.collection.mutable.HashSet[ExecutionAttemptID]()

  var disconnectDisabled = false


  override def receiveWithLogMessages = {
    receiveTestMessages orElse super.receiveWithLogMessages
  }

  /**
   * Handler for testing related messages
   */
  def receiveTestMessages: Receive = {
    case NotifyWhenTaskIsRunning(executionID) => {
      Option(runningTasks.get(executionID)) match {
        case Some(task) if task.getExecutionState == ExecutionState.RUNNING => sender ! true
        case _ =>
          val listeners = waitForRunning.getOrElse(executionID, Set())
          waitForRunning += (executionID -> (listeners + sender))
      }
    }

    case RequestRunningTasks =>
      sender ! ResponseRunningTasks(runningTasks.asScala.toMap)
      
    case NotifyWhenTaskRemoved(executionID) =>
      Option(runningTasks.get(executionID)) match {
        case Some(_) =>
          val set = waitForRemoval.getOrElse(executionID, Set())
          waitForRemoval += (executionID -> (set + sender))
        case None =>
          if(unregisteredTasks.contains(executionID)) {
            sender ! true
          } else {
              val set = waitForRemoval.getOrElse(executionID, Set())
              waitForRemoval += (executionID -> (set + sender))
          }
      }
      
    case TaskInFinalState(executionID) =>
      super.receiveWithLogMessages(TaskInFinalState(executionID))
      waitForRemoval.remove(executionID) match {
        case Some(actors) => for(actor <- actors) actor ! true
        case None =>
      }

      unregisteredTasks += executionID
      
    case RequestBroadcastVariablesWithReferences =>
      sender ! ResponseBroadcastVariablesWithReferences(
        bcVarManager.getNumberOfVariablesWithReferences)

    case RequestNumActiveConnections =>
      val numActive = if (network.isAssociated) {
                        network.getConnectionManager.getNumberOfActiveConnections
                      } else {
                        0
                      }
      sender ! ResponseNumActiveConnections(numActive)

    case NotifyWhenJobRemoved(jobID) =>
      if(runningTasks.values.asScala.exists(_.getJobID == jobID)){
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

    case CheckIfJobRemoved(jobID) =>
      if(runningTasks.values.asScala.forall(_.getJobID != jobID)){
        waitForJobRemoval.remove(jobID) match {
          case Some(listeners) => listeners foreach (_ ! true)
          case None =>
        }
      } else {
        import context.dispatcher
        context.system.scheduler.scheduleOnce(200 milliseconds, this.self, CheckIfJobRemoved(jobID))
      }

    case NotifyWhenJobManagerTerminated(jobManager) =>
      val waiting = waitForJobManagerToBeTerminated.getOrElse(jobManager.path.name, Set())
      waitForJobManagerToBeTerminated += jobManager.path.name -> (waiting + sender)

    case msg@Terminated(jobManager) =>
      super.receiveWithLogMessages(msg)

      waitForJobManagerToBeTerminated.remove(jobManager.path.name) foreach {
        _ foreach {
          _ ! JobManagerTerminated(jobManager)
        }
      }

    case msg:Disconnect =>
      if (!disconnectDisabled) {
        super.receiveWithLogMessages(msg)

        val jobManager = sender()

        waitForJobManagerToBeTerminated.remove(jobManager.path.name) foreach {
          _ foreach {
            _ ! JobManagerTerminated(jobManager)
          }
        }
      }

    case DisableDisconnect =>
      disconnectDisabled = true

    case msg @ UpdateTaskExecutionState(taskExecutionState) =>
      super.receiveWithLogMessages(msg)

      if(taskExecutionState.getExecutionState == ExecutionState.RUNNING) {
        waitForRunning.get(taskExecutionState.getID) foreach {
          _ foreach (_ ! true)
        }
      }
  }
}
