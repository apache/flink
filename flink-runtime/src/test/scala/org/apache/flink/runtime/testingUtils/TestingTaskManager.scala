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
import org.apache.flink.runtime.execution.ExecutionState
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID
import org.apache.flink.runtime.instance.InstanceConnectionInfo
import org.apache.flink.runtime.io.disk.iomanager.IOManager
import org.apache.flink.runtime.io.network.NetworkEnvironment
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService
import org.apache.flink.runtime.memorymanager.DefaultMemoryManager
import org.apache.flink.runtime.messages.JobManagerMessages.{ResponseLeaderSessionID,
RequestLeaderSessionID}
import org.apache.flink.runtime.messages.Messages.{Acknowledge, Disconnect}
import org.apache.flink.runtime.messages.RegistrationMessages.{AlreadyRegistered,
AcknowledgeRegistration}
import org.apache.flink.runtime.messages.TaskMessages.{UpdateTaskExecutionState, TaskInFinalState}
import org.apache.flink.runtime.taskmanager.{TaskManagerConfiguration, TaskManager}
import org.apache.flink.runtime.testingUtils.TestingJobManagerMessages.NotifyWhenJobRemoved
import org.apache.flink.runtime.testingUtils.TestingMessages.{CheckIfJobRemoved, Alive,
DisableDisconnect}
import org.apache.flink.runtime.testingUtils.TestingTaskManagerMessages._

import scala.concurrent.duration._
import scala.language.postfixOps

/** Subclass of the [[TaskManager]] to support testing messages
  *
  * @param config
  * @param connectionInfo
  * @param memoryManager
  * @param ioManager
  * @param network
  * @param numberOfSlots
  */
class TestingTaskManager(
    config: TaskManagerConfiguration,
    connectionInfo: InstanceConnectionInfo,
    memoryManager: DefaultMemoryManager,
    ioManager: IOManager,
    network: NetworkEnvironment,
    numberOfSlots: Int,
    leaderRetrievalService: LeaderRetrievalService)
  extends TaskManager(
    config,
    connectionInfo,
    memoryManager,
    ioManager,
    network,
    numberOfSlots,
    leaderRetrievalService) {

  import scala.collection.JavaConverters._

  val waitForRemoval = scala.collection.mutable.HashMap[ExecutionAttemptID, Set[ActorRef]]()
  val waitForJobManagerToBeTerminated = scala.collection.mutable.HashMap[String, Set[ActorRef]]()
  val waitForRegisteredAtJobManager = scala.collection.mutable.HashMap[ActorRef, Set[ActorRef]]()
  val waitForRunning = scala.collection.mutable.HashMap[ExecutionAttemptID, Set[ActorRef]]()
  val unregisteredTasks = scala.collection.mutable.HashSet[ExecutionAttemptID]()

  var disconnectDisabled = false

  /**
   * Handler for testing related messages
   */
  override def handleMessage: Receive = {
    handleTestingMessage orElse super.handleMessage
  }

  def handleTestingMessage: Receive = {
    case Alive => sender() ! Acknowledge

    case NotifyWhenTaskIsRunning(executionID) => {
      Option(runningTasks.get(executionID)) match {
        case Some(task) if task.getExecutionState == ExecutionState.RUNNING =>
          sender ! decorateMessage(true)

        case _ =>
          val listeners = waitForRunning.getOrElse(executionID, Set())
          waitForRunning += (executionID -> (listeners + sender))
      }
    }

    case RequestRunningTasks =>
      sender ! decorateMessage(ResponseRunningTasks(runningTasks.asScala.toMap))
      
    case NotifyWhenTaskRemoved(executionID) =>
      Option(runningTasks.get(executionID)) match {
        case Some(_) =>
          val set = waitForRemoval.getOrElse(executionID, Set())
          waitForRemoval += (executionID -> (set + sender))
        case None =>
          if(unregisteredTasks.contains(executionID)) {
            sender ! decorateMessage(true)
          } else {
              val set = waitForRemoval.getOrElse(executionID, Set())
              waitForRemoval += (executionID -> (set + sender))
          }
      }

    case TaskInFinalState(executionID) =>
      super.handleMessage(TaskInFinalState(executionID))
      waitForRemoval.remove(executionID) match {
        case Some(actors) => for(actor <- actors) actor ! decorateMessage(true)
        case None =>
      }

      unregisteredTasks += executionID
      
    case RequestBroadcastVariablesWithReferences =>
      sender ! decorateMessage(
        ResponseBroadcastVariablesWithReferences(
          bcVarManager.getNumberOfVariablesWithReferences)
      )

    case RequestNumActiveConnections =>
      val numActive = if (network.isAssociated) {
                        network.getConnectionManager.getNumberOfActiveConnections
                      } else {
                        0
                      }
      sender ! decorateMessage(ResponseNumActiveConnections(numActive))

    case NotifyWhenJobRemoved(jobID) =>
      if(runningTasks.values.asScala.exists(_.getJobID == jobID)){
        context.system.scheduler.scheduleOnce(
          200 milliseconds,
          self,
          decorateMessage(CheckIfJobRemoved(jobID)))(
          context.dispatcher,
          sender
          )
      }else{
        sender ! decorateMessage(true)
      }

    case CheckIfJobRemoved(jobID) =>
      if(runningTasks.values.asScala.forall(_.getJobID != jobID)){
        sender ! decorateMessage(true)
      } else {
        context.system.scheduler.scheduleOnce(
          200 milliseconds,
          self,
          decorateMessage(CheckIfJobRemoved(jobID)))(
          context.dispatcher,
          sender
          )
      }

    case NotifyWhenJobManagerTerminated(jobManager) =>
      val waiting = waitForJobManagerToBeTerminated.getOrElse(jobManager.path.name, Set())
      waitForJobManagerToBeTerminated += jobManager.path.name -> (waiting + sender)

    /**
     * Message from task manager that accumulator values changed and need to be reported immediately
     * instead of lazily through the
     * [[org.apache.flink.runtime.messages.TaskManagerMessages.Heartbeat]] message. We forward this
     * message to the job manager that it knows it should report to the listeners.
     */
    case msg: AccumulatorsChanged =>
      currentJobManager match {
        case Some(jobManager) =>
          jobManager.forward(msg)
          sendHeartbeatToJobManager()
          sender ! true
        case None =>
      }

    case msg@Terminated(jobManager) =>
      super.handleMessage(msg)

      waitForJobManagerToBeTerminated.remove(jobManager.path.name) foreach {
        _ foreach {
          _ ! decorateMessage(JobManagerTerminated(jobManager))
        }
      }

    case msg:Disconnect =>
      if (!disconnectDisabled) {
        super.handleMessage(msg)

        val jobManager = sender()

        waitForJobManagerToBeTerminated.remove(jobManager.path.name) foreach {
          _ foreach {
            _ ! decorateMessage(JobManagerTerminated(jobManager))
          }
        }
      }

    case DisableDisconnect =>
      disconnectDisabled = true

    case msg @ UpdateTaskExecutionState(taskExecutionState) =>
      super.handleMessage(msg)

      if(taskExecutionState.getExecutionState == ExecutionState.RUNNING) {
        waitForRunning.get(taskExecutionState.getID) foreach {
          _ foreach (_ ! decorateMessage(true))
        }
      }

    case RequestLeaderSessionID =>
      sender() ! ResponseLeaderSessionID(leaderSessionID.orNull)

    case NotifyWhenRegisteredAtJobManager(jobManager: ActorRef) =>
      if(isConnected && jobManager == currentJobManager.get) {
        sender() ! true
      } else {
        val list = waitForRegisteredAtJobManager.getOrElse(
          jobManager,
          Set[ActorRef]())

        waitForRegisteredAtJobManager += jobManager -> (list + sender())
      }

    case msg @ (_: AcknowledgeRegistration | _: AlreadyRegistered) =>
      super.handleMessage(msg)

      val jm = sender()

      waitForRegisteredAtJobManager.remove(jm).foreach {
        listeners => listeners.foreach{
          listener =>
            listener ! true
        }
      }
  }
}
