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

import akka.actor.{Terminated, Cancellable, ActorRef}
import akka.pattern.{ask, pipe}
import org.apache.flink.api.common.JobID
import org.apache.flink.runtime.FlinkActor
import org.apache.flink.runtime.execution.ExecutionState
import org.apache.flink.runtime.jobgraph.JobStatus
import org.apache.flink.runtime.jobmanager.JobManager
import org.apache.flink.runtime.messages.ExecutionGraphMessages.JobStatusChanged
import org.apache.flink.runtime.messages.JobManagerMessages.GrantLeadership
import org.apache.flink.runtime.messages.Messages.{Acknowledge, Disconnect}
import org.apache.flink.runtime.messages.RegistrationMessages.RegisterTaskManager
import org.apache.flink.runtime.messages.TaskManagerMessages.Heartbeat
import org.apache.flink.runtime.testingUtils.TestingJobManagerMessages._
import org.apache.flink.runtime.testingUtils.TestingMessages.{DisableDisconnect,
CheckIfJobRemoved, Alive}
import org.apache.flink.runtime.testingUtils.TestingTaskManagerMessages.AccumulatorsChanged

import scala.collection.mutable
import scala.concurrent.Future
import scala.concurrent.duration._

import language.postfixOps

/** This mixin can be used to decorate a JobManager with messages for testing purpose.  */
trait TestingJobManagerLike extends FlinkActor {
  that: JobManager =>

  import scala.collection.JavaConverters._
  import context._

  val waitForAllVerticesToBeRunning = scala.collection.mutable.HashMap[JobID, Set[ActorRef]]()
  val waitForTaskManagerToBeTerminated = scala.collection.mutable.HashMap[String, Set[ActorRef]]()

  val waitForAllVerticesToBeRunningOrFinished =
    scala.collection.mutable.HashMap[JobID, Set[ActorRef]]()

  var periodicCheck: Option[Cancellable] = None

  val waitForJobStatus = scala.collection.mutable.HashMap[JobID,
    collection.mutable.HashMap[JobStatus, Set[ActorRef]]]()

  val waitForAccumulatorUpdate = scala.collection.mutable.HashMap[JobID, (Boolean, Set[ActorRef])]()

  val waitForLeader = scala.collection.mutable.HashSet[ActorRef]()

  val waitForNumRegisteredTaskManagers = mutable.PriorityQueue.newBuilder(
    new Ordering[(Int, ActorRef)] {
      override def compare(x: (Int, ActorRef), y: (Int, ActorRef)): Int = y._1 - x._1
    })

  var disconnectDisabled = false

  var postStopEnabled = true

  abstract override def postStop(): Unit = {
    if (postStopEnabled) {
      super.postStop()
    } else {
      // only stop leader election service to revoke the leadership of this JM so that a new JM
      // can be elected leader
      leaderElectionService.stop()
    }
  }

  abstract override def handleMessage: Receive = {
    handleTestingMessage orElse super.handleMessage
  }

  def handleTestingMessage: Receive = {
    case Alive => sender() ! Acknowledge

    case RequestExecutionGraph(jobID) =>
      currentJobs.get(jobID) match {
        case Some((executionGraph, jobInfo)) => sender() ! decorateMessage(
          ExecutionGraphFound(
            jobID,
            executionGraph)
        )

        case None => archive.tell(decorateMessage(RequestExecutionGraph(jobID)), sender())
      }

    case WaitForAllVerticesToBeRunning(jobID) =>
      if(checkIfAllVerticesRunning(jobID)){
        sender() ! decorateMessage(AllVerticesRunning(jobID))
      }else{
        val waiting = waitForAllVerticesToBeRunning.getOrElse(jobID, Set[ActorRef]())
        waitForAllVerticesToBeRunning += jobID -> (waiting + sender())

        if(periodicCheck.isEmpty){
          periodicCheck =
            Some(
              context.system.scheduler.schedule(
                0 seconds,
                200 millis,
                self,
                decorateMessage(NotifyListeners)
              )
            )
        }
      }
    case WaitForAllVerticesToBeRunningOrFinished(jobID) =>
      if(checkIfAllVerticesRunningOrFinished(jobID)){
        sender() ! decorateMessage(AllVerticesRunning(jobID))
      }else{
        val waiting = waitForAllVerticesToBeRunningOrFinished.getOrElse(jobID, Set[ActorRef]())
        waitForAllVerticesToBeRunningOrFinished += jobID -> (waiting + sender())

        if(periodicCheck.isEmpty){
          periodicCheck =
            Some(
              context.system.scheduler.schedule(
                0 seconds,
                200 millis,
                self,
                decorateMessage(NotifyListeners)
              )
            )
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
      val gateways = instanceManager.getAllRegisteredInstances.asScala.map(_.getActorGateway)

      val responses = gateways.map{
        gateway => gateway.ask(NotifyWhenJobRemoved(jobID), timeout).mapTo[Boolean]
      }

      val jobRemovedOnJobManager = (self ? CheckIfJobRemoved(jobID))(timeout).mapTo[Boolean]

      val allFutures = responses ++ Seq(jobRemovedOnJobManager)

      import context.dispatcher
      Future.fold(allFutures)(true)(_ & _) map(decorateMessage(_)) pipeTo sender()

    case CheckIfJobRemoved(jobID) =>
      if(currentJobs.contains(jobID)) {
        context.system.scheduler.scheduleOnce(
          200 milliseconds,
          self,
          decorateMessage(CheckIfJobRemoved(jobID))
        )(context.dispatcher, sender())
      } else {
        sender() ! decorateMessage(true)
      }

    case NotifyWhenTaskManagerTerminated(taskManager) =>
      val waiting = waitForTaskManagerToBeTerminated.getOrElse(taskManager.path.name, Set())
      waitForTaskManagerToBeTerminated += taskManager.path.name -> (waiting + sender)

    case msg@Terminated(taskManager) =>
      super.handleMessage(msg)

      waitForTaskManagerToBeTerminated.remove(taskManager.path.name) foreach {
        _ foreach {
          listener =>
            listener ! decorateMessage(TaskManagerTerminated(taskManager))
        }
      }

    case NotifyWhenAccumulatorChange(jobID) =>

      val (updated, registered) = waitForAccumulatorUpdate.
        getOrElse(jobID, (false, Set[ActorRef]()))
      waitForAccumulatorUpdate += jobID -> (updated, registered + sender)
      sender ! true

    /**
     * Notification from the task manager that changed accumulator are transferred on next
     * Hearbeat. We need to keep this state to notify the listeners on next Heartbeat report.
     */
    case AccumulatorsChanged(jobID: JobID) =>
      waitForAccumulatorUpdate.get(jobID) match {
        case Some((updated, registered)) =>
          waitForAccumulatorUpdate.put(jobID, (true, registered))
        case None =>
      }

    /**
     * Disabled async processing of accumulator values and send accumulators to the listeners if
     * we previously received an [[AccumulatorsChanged]] message.
     */
    case msg : Heartbeat =>
      super.handleMessage(msg)

      waitForAccumulatorUpdate foreach {
        case (jobID, (updated, actors)) if updated =>
          currentJobs.get(jobID) match {
            case Some((graph, jobInfo)) =>
              val flinkAccumulators = graph.getFlinkAccumulators
              val userAccumulators = graph.aggregateUserAccumulators
              actors foreach {
                actor => actor ! UpdatedAccumulators(jobID, flinkAccumulators, userAccumulators)
              }
            case None =>
          }
          waitForAccumulatorUpdate.put(jobID, (false, actors))
        case _ =>
      }

    case RequestWorkingTaskManager(jobID) =>
      currentJobs.get(jobID) match {
        case Some((eg, _)) =>
          if(eg.getAllExecutionVertices.asScala.isEmpty){
            sender ! decorateMessage(WorkingTaskManager(None))
          } else {
            val resource = eg.getAllExecutionVertices.asScala.head.getCurrentAssignedResource

            if(resource == null){
              sender ! decorateMessage(WorkingTaskManager(None))
            } else {
              sender ! decorateMessage(
                WorkingTaskManager(
                  Some(resource.getInstance().getActorGateway)
                )
              )
            }
          }
        case None => sender ! decorateMessage(WorkingTaskManager(None))
      }

    case NotifyWhenJobStatus(jobID, state) =>
      val jobStatusListener = waitForJobStatus.getOrElseUpdate(jobID,
        scala.collection.mutable.HashMap[JobStatus, Set[ActorRef]]())

      val listener = jobStatusListener.getOrElse(state, Set[ActorRef]())

      jobStatusListener += state -> (listener + sender)

    case msg@JobStatusChanged(jobID, newJobStatus, _, _) =>
      super.handleMessage(msg)

      val cleanup = waitForJobStatus.get(jobID) match {
        case Some(stateListener) =>
          stateListener.remove(newJobStatus) match {
            case Some(listeners) =>
              listeners foreach {
                _ ! decorateMessage(JobStatusIs(jobID, newJobStatus))
              }
            case _ =>
          }
          stateListener.isEmpty

        case _ => false
      }

      if (cleanup) {
        waitForJobStatus.remove(jobID)
      }

    case DisableDisconnect =>
      disconnectDisabled = true

    case DisablePostStop =>
      postStopEnabled = false

    case RequestSavepoint(savepointPath) =>
      try {
        val savepoint = savepointStore.getState(savepointPath)
        sender ! ResponseSavepoint(savepoint)
      }
      catch {
        case e: Exception =>
          sender ! ResponseSavepoint(null)
      }

    case msg: Disconnect =>
      if (!disconnectDisabled) {
        super.handleMessage(msg)

        val taskManager = sender()

        waitForTaskManagerToBeTerminated.remove(taskManager.path.name) foreach {
          _ foreach {
            listener =>
              listener ! decorateMessage(TaskManagerTerminated(taskManager))
          }
        }
      }

    case NotifyWhenLeader =>
      if (leaderElectionService.hasLeadership) {
        sender() ! true
      } else {
        waitForLeader += sender()
      }

    case msg: GrantLeadership =>
      super.handleMessage(msg)

      waitForLeader.foreach(_ ! true)

      waitForLeader.clear()

    case NotifyWhenAtLeastNumTaskManagerAreRegistered(numRegisteredTaskManager) =>
      if (that.instanceManager.getNumberOfRegisteredTaskManagers >= numRegisteredTaskManager) {
        // there are already at least numRegisteredTaskManager registered --> send Acknowledge
        sender() ! Acknowledge
      } else {
        // wait until we see at least numRegisteredTaskManager being registered at the JobManager
        waitForNumRegisteredTaskManagers += ((numRegisteredTaskManager, sender()))
      }

    case msg:RegisterTaskManager =>
      super.handleMessage(msg)

      // dequeue all senders which wait for instanceManager.getNumberOfRegisteredTaskManagers or
      // fewer registered TaskManagers
      while (waitForNumRegisteredTaskManagers.nonEmpty &&
        waitForNumRegisteredTaskManagers.head._1 <=
          instanceManager.getNumberOfRegisteredTaskManagers) {
        val receiver = waitForNumRegisteredTaskManagers.dequeue()._2
        receiver ! Acknowledge
      }
  }

  def checkIfAllVerticesRunning(jobID: JobID): Boolean = {
    currentJobs.get(jobID) match {
      case Some((eg, _)) =>
        eg.getAllExecutionVertices.asScala.forall( _.getExecutionState == ExecutionState.RUNNING)
      case None => false
    }
  }

  def checkIfAllVerticesRunningOrFinished(jobID: JobID): Boolean = {
    currentJobs.get(jobID) match {
      case Some((eg, _)) =>
        eg.getAllExecutionVertices.asScala.forall {
          case vertex =>
            (vertex.getExecutionState == ExecutionState.RUNNING
              || vertex.getExecutionState == ExecutionState.FINISHED)
        }
      case None => false
    }
  }

  def notifyListeners(jobID: JobID): Unit = {
    if(checkIfAllVerticesRunning(jobID)) {
      waitForAllVerticesToBeRunning.remove(jobID) match {
        case Some(listeners) =>
          for (listener <- listeners) {
            listener ! decorateMessage(AllVerticesRunning(jobID))
          }
        case _ =>
      }
    }

    if(checkIfAllVerticesRunningOrFinished(jobID)) {
      waitForAllVerticesToBeRunningOrFinished.remove(jobID) match {
        case Some(listeners) =>
          for (listener <- listeners) {
            listener ! decorateMessage(AllVerticesRunning(jobID))
          }
        case _ =>
      }
    }
  }
}
