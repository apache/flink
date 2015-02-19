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

import akka.actor.{Cancellable, Terminated, ActorRef, Props}
import akka.pattern.{ask, pipe}
import org.apache.flink.runtime.ActorLogMessages
import org.apache.flink.runtime.execution.ExecutionState
import org.apache.flink.runtime.jobgraph.{JobStatus, JobID}
import org.apache.flink.runtime.jobmanager.{JobManager, MemoryArchivist}
import org.apache.flink.runtime.messages.ExecutionGraphMessages.JobStatusChanged
import org.apache.flink.runtime.testingUtils.TestingJobManagerMessages._

import scala.collection.convert.WrapAsScala
import scala.concurrent.Future
import scala.concurrent.duration._

import scala.language.postfixOps

/**
 * Mixin for [[TestingJobManager]] to support testing messages
 */
trait TestingJobManager extends ActorLogMessages with WrapAsScala {
  that: JobManager =>

  import context._

  val waitForAllVerticesToBeRunning = scala.collection.mutable.HashMap[JobID, Set[ActorRef]]()
  val waitForTaskManagerToBeTerminated = scala.collection.mutable.HashMap[String, Set[ActorRef]]()

  val waitForAllVerticesToBeRunningOrFinished =
    scala.collection.mutable.HashMap[JobID, Set[ActorRef]]()

  var periodicCheck: Option[Cancellable] = None

  val waitForJobStatus = scala.collection.mutable.HashMap[JobID,
    collection.mutable.HashMap[JobStatus, Set[ActorRef]]]()

  abstract override def receiveWithLogMessages: Receive = {
    receiveTestingMessages orElse super.receiveWithLogMessages
  }

  def receiveTestingMessages: Receive = {
    case RequestExecutionGraph(jobID) =>
      currentJobs.get(jobID) match {
        case Some((executionGraph, jobInfo)) => sender ! ExecutionGraphFound(jobID,
          executionGraph)
        case None => archive.tell(RequestExecutionGraph(jobID), sender)
      }

    case WaitForAllVerticesToBeRunning(jobID) =>
      if(checkIfAllVerticesRunning(jobID)){
        sender ! AllVerticesRunning(jobID)
      }else{
        val waiting = waitForAllVerticesToBeRunning.getOrElse(jobID, Set[ActorRef]())
        waitForAllVerticesToBeRunning += jobID -> (waiting + sender)

        if(periodicCheck.isEmpty){
          periodicCheck =
            Some(context.system.scheduler.schedule(0 seconds, 200 millis, self, NotifyListeners))
        }
      }
    case WaitForAllVerticesToBeRunningOrFinished(jobID) =>
      if(checkIfAllVerticesRunningOrFinished(jobID)){
        sender ! AllVerticesRunning(jobID)
      }else{
        val waiting = waitForAllVerticesToBeRunningOrFinished.getOrElse(jobID, Set[ActorRef]())
        waitForAllVerticesToBeRunningOrFinished += jobID -> (waiting + sender)

        if(periodicCheck.isEmpty){
          periodicCheck =
            Some(context.system.scheduler.schedule(0 seconds, 200 millis, self, NotifyListeners))
        }
      }

    case NotifyListeners =>
      for(jobID <- currentJobs.keySet){
        notifyListeners(jobID)
      }

      if(waitForAllVerticesToBeRunning.isEmpty && waitForAllVerticesToBeRunningOrFinished.isEmpty) {
        periodicCheck foreach { _.cancel() }
        periodicCheck = None
      }


    case NotifyWhenJobRemoved(jobID) =>
      val tms = instanceManager.getAllRegisteredInstances.map(_.getTaskManager)

      val responses = tms.map{
        tm =>
          (tm ? NotifyWhenJobRemoved(jobID))(timeout).mapTo[Boolean]
      }

      import context.dispatcher

      Future.fold(responses)(true)(_ & _) pipeTo sender

    case NotifyWhenTaskManagerTerminated(taskManager) =>
      val waiting = waitForTaskManagerToBeTerminated.getOrElse(taskManager.path.name, Set())
      waitForTaskManagerToBeTerminated += taskManager.path.name -> (waiting + sender)

    case msg@Terminated(taskManager) =>
      super.receiveWithLogMessages(msg)

      waitForTaskManagerToBeTerminated.remove(taskManager.path.name) foreach {
        _ foreach {
          listener =>
            listener ! TaskManagerTerminated(taskManager)
        }
      }
    case RequestWorkingTaskManager(jobID) =>
      currentJobs.get(jobID) match {
        case Some((eg, _)) =>
          if(eg.getAllExecutionVertices.isEmpty){
            sender ! WorkingTaskManager(ActorRef.noSender)
          } else {
            val resource = eg.getAllExecutionVertices.head.getCurrentAssignedResource

            if(resource == null){
              sender ! WorkingTaskManager(ActorRef.noSender)
            } else {
              sender ! WorkingTaskManager(resource.getInstance().getTaskManager)
            }
          }
        case None => sender ! WorkingTaskManager(ActorRef.noSender)
      }

    case NotifyWhenJobStatus(jobID, state) =>
      val jobStatusListener = waitForJobStatus.getOrElseUpdate(jobID,
        scala.collection.mutable.HashMap[JobStatus, Set[ActorRef]]())

      val listener = jobStatusListener.getOrElse(state, Set[ActorRef]())

      jobStatusListener += state -> (listener + sender)

    case msg@JobStatusChanged(jobID, newJobStatus, _, _) =>
      super.receiveWithLogMessages(msg)

      val cleanup = waitForJobStatus.get(jobID) match {
        case Some(stateListener) =>
          stateListener.remove(newJobStatus) match {
            case Some(listeners) =>
              listeners foreach {
                _ ! JobStatusIs(jobID, newJobStatus)
              }
            case _ =>
          }
          stateListener.isEmpty

        case _ => false
      }

      if (cleanup) {
        waitForJobStatus.remove(jobID)
      }
  }

  def checkIfAllVerticesRunning(jobID: JobID): Boolean = {
    currentJobs.get(jobID) match {
      case Some((eg, _)) =>
        eg.getAllExecutionVertices.forall( _.getExecutionState == ExecutionState.RUNNING)
      case None => false
    }
  }

  def checkIfAllVerticesRunningOrFinished(jobID: JobID): Boolean = {
    currentJobs.get(jobID) match {
      case Some((eg, _)) =>
        eg.getAllExecutionVertices.forall {
          case vertex =>
            (vertex.getExecutionState == ExecutionState.RUNNING
              || vertex.getExecutionState == ExecutionState.FINISHED)
        }
      case None => false
    }
  }

  def notifyListeners(jobID: JobID): Unit = {
    if(checkIfAllVerticesRunning((jobID))) {
      waitForAllVerticesToBeRunning.remove(jobID) match {
        case Some(listeners) =>
          for (listener <- listeners) {
            listener ! AllVerticesRunning(jobID)
          }
        case _ =>
      }
    }

    if(checkIfAllVerticesRunningOrFinished(jobID)) {
      waitForAllVerticesToBeRunningOrFinished.remove(jobID) match {
        case Some(listeners) =>
          for (listener <- listeners) {
            listener ! AllVerticesRunning(jobID)
          }
        case _ =>
      }
    }
  }
}
