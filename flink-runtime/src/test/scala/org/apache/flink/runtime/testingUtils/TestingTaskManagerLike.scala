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

import java.util.UUID

import akka.actor.{ActorRef, Terminated}
import org.apache.flink.api.common.JobID
import org.apache.flink.runtime.FlinkActor
import org.apache.flink.runtime.execution.ExecutionState
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID
import org.apache.flink.runtime.messages.Acknowledge
import org.apache.flink.runtime.messages.JobManagerMessages.{RequestLeaderSessionID, ResponseLeaderSessionID}
import org.apache.flink.runtime.messages.Messages.Disconnect
import org.apache.flink.runtime.messages.RegistrationMessages.{AcknowledgeRegistration, AlreadyRegistered}
import org.apache.flink.runtime.messages.TaskMessages.{SubmitTask, TaskInFinalState, UpdateTaskExecutionState}
import org.apache.flink.runtime.taskmanager.TaskManager
import org.apache.flink.runtime.testingUtils.TestingJobManagerMessages.NotifyWhenJobRemoved
import org.apache.flink.runtime.testingUtils.TestingMessages._
import org.apache.flink.runtime.testingUtils.TestingTaskManagerMessages._

import scala.concurrent.duration._
import scala.language.postfixOps

/** This mixin can be used to decorate a TaskManager with messages for testing purposes. */
trait TestingTaskManagerLike extends FlinkActor {
  that: TaskManager =>

  import scala.collection.JavaConverters._

  val waitForRemoval = scala.collection.mutable.HashMap[ExecutionAttemptID, Set[ActorRef]]()
  val waitForJobManagerToBeTerminated = scala.collection.mutable.HashMap[UUID, Set[ActorRef]]()
  val waitForRegisteredAtResourceManager =
    scala.collection.mutable.HashMap[ActorRef, Set[ActorRef]]()
  val waitForRunning = scala.collection.mutable.HashMap[ExecutionAttemptID, Set[ActorRef]]()
  val unregisteredTasks = scala.collection.mutable.HashSet[ExecutionAttemptID]()

  /** Map of registered task submit listeners */
  val registeredSubmitTaskListeners = scala.collection.mutable.HashMap[JobID, ActorRef]()

  val waitForShutdown = scala.collection.mutable.HashSet[ActorRef]()

  var disconnectDisabled = false

  /**
   * Handler for testing related messages
   */
  abstract override def handleMessage: Receive = {
    handleTestingMessage orElse super.handleMessage
  }

  def handleTestingMessage: Receive = {
    case Alive => sender() ! Acknowledge.get()

    case NotifyWhenTaskIsRunning(executionID) =>
      Option(runningTasks.get(executionID)) match {
        case Some(task) if task.getExecutionState == ExecutionState.RUNNING =>
          sender ! decorateMessage(true)

        case _ =>
          val listeners = waitForRunning.getOrElse(executionID, Set())
          waitForRunning += (executionID -> (listeners + sender))
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

    case NotifyWhenJobRemoved(jobID) =>
      if(runningTasks.values.asScala.exists(_.getJobID == jobID)){
        context.system.scheduler.scheduleOnce(
          200 milliseconds,
          self,
          decorateMessage(CheckIfJobRemoved(jobID)))(
            context.dispatcher,
            sender()
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
            sender()
          )
      }

    case NotifyWhenJobManagerTerminated(leaderId) =>
      val waiting = waitForJobManagerToBeTerminated.getOrElse(leaderId, Set())
      waitForJobManagerToBeTerminated += leaderId -> (waiting + sender)

    case RegisterSubmitTaskListener(jobId) =>
      registeredSubmitTaskListeners.put(jobId, sender())

    case msg@SubmitTask(tdd) =>
      // this loads offloaded data back into the tdd and needs to be run first!
      super.handleMessage(msg)

      val jobId = tdd.getJobId

      registeredSubmitTaskListeners.get(jobId) match {
        case Some(listenerRef) =>
          listenerRef ! ResponseSubmitTaskListener(tdd)
        case None =>
        // Nothing to do
      }


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

      val currentJM = currentJobManager.getOrElse(ActorRef.noSender)

      val leaderId = if (jobManager.equals(currentJM)) {
        leaderSessionID
      } else {
        None
      }

      super.handleMessage(msg)

      waitForJobManagerToBeTerminated.remove(leaderId.orNull) foreach {
        _ foreach {
          _ ! decorateMessage(JobManagerTerminated(leaderId.orNull))
        }
      }

    case msg:Disconnect =>
      if (!disconnectDisabled) {
        val leaderId = leaderSessionID
        super.handleMessage(msg)

        waitForJobManagerToBeTerminated.remove(leaderId.orNull) foreach {
          _ foreach {
            _ ! decorateMessage(JobManagerTerminated(leaderId.orNull))
          }
        }
      }

    case DisableDisconnect =>
      disconnectDisabled = true

    case NotifyOfComponentShutdown =>
      waitForShutdown += sender()

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
        val list = waitForRegisteredAtResourceManager.getOrElse(
          jobManager,
          Set[ActorRef]())

        waitForRegisteredAtResourceManager += jobManager -> (list + sender())
      }

    case msg @ (_: AcknowledgeRegistration | _: AlreadyRegistered) =>
      super.handleMessage(msg)

      val jm = sender()

      waitForRegisteredAtResourceManager.remove(jm).foreach {
        listeners => listeners.foreach{
          listener =>
            listener ! true
        }
      }
  }

  /**
    * No killing of the VM for testing.
    */
  override protected def shutdown(): Unit = {
    log.info("Shutting down TestingJobManager.")
    waitForShutdown.foreach(_ ! ComponentShutdown(self))
    waitForShutdown.clear()
  }
}
