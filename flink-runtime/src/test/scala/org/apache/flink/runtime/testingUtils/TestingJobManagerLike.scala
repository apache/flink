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

import java.io.DataInputStream
import java.util.function.BiFunction

import akka.actor.{ActorRef, Cancellable, Terminated}
import akka.pattern.{ask, pipe}
import org.apache.flink.api.common.JobID
import org.apache.flink.core.fs.FSDataInputStream
import org.apache.flink.runtime.FlinkActor
import org.apache.flink.runtime.checkpoint.savepoint.Savepoint
import org.apache.flink.runtime.checkpoint._
import org.apache.flink.runtime.execution.ExecutionState
import org.apache.flink.runtime.jobgraph.JobStatus
import org.apache.flink.runtime.jobmanager.JobManager
import org.apache.flink.runtime.jobmanager.slots.ActorTaskManagerGateway
import org.apache.flink.runtime.messages.Acknowledge
import org.apache.flink.runtime.messages.ExecutionGraphMessages.JobStatusChanged
import org.apache.flink.runtime.messages.JobManagerMessages._
import org.apache.flink.runtime.messages.Messages.Disconnect
import org.apache.flink.runtime.messages.RegistrationMessages.RegisterTaskManager
import org.apache.flink.runtime.messages.TaskManagerMessages.Heartbeat
import org.apache.flink.runtime.state.memory.MemoryStateBackend
import org.apache.flink.runtime.state.{StateBackend, StateBackendLoader, StreamStateHandle}
import org.apache.flink.runtime.testingUtils.TestingJobManagerMessages._
import org.apache.flink.runtime.testingUtils.TestingMessages._
import org.apache.flink.runtime.testingUtils.TestingTaskManagerMessages.AccumulatorsChanged

import scala.collection.mutable
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.language.postfixOps

/** This mixin can be used to decorate a JobManager with messages for testing purpose.  */
trait TestingJobManagerLike extends FlinkActor {
  that: JobManager =>

  import context._

  import scala.collection.JavaConverters._

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

  val waitForClient = scala.collection.mutable.HashSet[ActorRef]()

  val waitForShutdown = scala.collection.mutable.HashSet[ActorRef]()

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
    case Alive => sender() ! Acknowledge.get()

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
      val gateways = instanceManager.getAllRegisteredInstances.asScala.map(_.getTaskManagerGateway)

      val responses = gateways.map{
        gateway =>  gateway match {
          case actorGateway: ActorTaskManagerGateway =>
            actorGateway.getActorGateway.ask(NotifyWhenJobRemoved(jobID), timeout).mapTo[Boolean]
          case _ =>
            throw new IllegalStateException("The task manager gateway is not of type " +
                                             s"${classOf[ActorTaskManagerGateway].getSimpleName}")
        }
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

    // see shutdown method for reply
    case NotifyOfComponentShutdown =>
      waitForShutdown += sender()

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
              val userAccumulators = graph.aggregateUserAccumulators
              actors foreach {
                 actor => actor ! UpdatedAccumulators(jobID, userAccumulators)
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
                  Some(
                    resource.getTaskManagerGateway() match {
                      case actorTaskManagerGateway: ActorTaskManagerGateway =>
                        actorTaskManagerGateway.getActorGateway
                      case _ => throw new IllegalStateException(
                        "The task manager gateway is not of type " +
                          s"${classOf[ActorTaskManagerGateway].getSimpleName}")
                    }
                  )
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
        val classloader = Thread.currentThread().getContextClassLoader

        val loadedBackend = StateBackendLoader.loadStateBackendFromConfig(
          flinkConfiguration, classloader, null)
        val backend = if (loadedBackend != null) loadedBackend else new MemoryStateBackend()

        val checkpointLocation = backend.resolveCheckpoint(savepointPath)

        val stream = new DataInputStream(checkpointLocation.getMetadataHandle.openInputStream())
        val savepoint = try {
          Checkpoints.loadCheckpointMetadata(stream, classloader)
        }
        finally {
          stream.close()
        }

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

    case CheckpointRequest(jobId, retentionPolicy) =>
      currentJobs.get(jobId) match {
        case Some((graph, _)) =>
          val checkpointCoordinator = graph.getCheckpointCoordinator()

          if (checkpointCoordinator != null) {
            // Immutable copy for the future
            val senderRef = sender()
            try {
              // Do this async, because checkpoint coordinator operations can
              // contain blocking calls to the state backend or ZooKeeper.
              val triggerResult = checkpointCoordinator.triggerCheckpoint(
                System.currentTimeMillis(),
                CheckpointProperties.forCheckpoint(retentionPolicy),
                null,
                false)

              if (triggerResult.isSuccess) {
                triggerResult.getPendingCheckpoint.getCompletionFuture.handleAsync[Void](
                  new BiFunction[CompletedCheckpoint, Throwable, Void] {
                    override def apply(success: CompletedCheckpoint, cause: Throwable): Void = {
                      if (success != null) {
                        senderRef ! CheckpointRequestSuccess(
                          jobId,
                          success.getCheckpointID,
                          success.getExternalPointer,
                          success.getTimestamp)
                      } else {
                        senderRef ! CheckpointRequestFailure(
                          jobId, new Exception("Failed to complete checkpoint", cause))
                      }
                      null
                    }
                  },
                  context.dispatcher)
              } else {
                senderRef ! CheckpointRequestFailure(jobId, new Exception(
                  "Failed to trigger checkpoint: " +  triggerResult.getFailureReason.message()))
              }
            } catch {
              case e: Exception =>
                senderRef ! CheckpointRequestFailure(jobId, new Exception(
                  "Failed to trigger checkpoint", e))
            }
          } else {
            sender() ! CheckpointRequestFailure(jobId, new IllegalStateException(
              "Checkpointing disabled. You can enable it via the execution environment of " +
                "your job."))
          }

        case None =>
          sender() ! CheckpointRequestFailure(jobId, new IllegalArgumentException("Unknown job."))
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

    case NotifyWhenClientConnects =>
      waitForClient += sender()
      sender() ! true

    case msg: RegisterJobClient =>
      super.handleMessage(msg)
      waitForClient.foreach(_ ! ClientConnected)
    case msg: RequestClassloadingProps =>
      super.handleMessage(msg)
      waitForClient.foreach(_ ! ClassLoadingPropsDelivered)

    case NotifyWhenAtLeastNumTaskManagerAreRegistered(numRegisteredTaskManager) =>
      if (that.instanceManager.getNumberOfRegisteredTaskManagers >= numRegisteredTaskManager) {
        // there are already at least numRegisteredTaskManager registered --> send Acknowledge
        sender() ! Acknowledge.get()
      } else {
        // wait until we see at least numRegisteredTaskManager being registered at the JobManager
        waitForNumRegisteredTaskManagers += ((numRegisteredTaskManager, sender()))
      }

    // TaskManager may be registered on these two messages
    case msg @ (_: RegisterTaskManager) =>
      super.handleMessage(msg)

      // dequeue all senders which wait for instanceManager.getNumberOfStartedTaskManagers or
      // fewer registered TaskManagers
      while (waitForNumRegisteredTaskManagers.nonEmpty &&
        waitForNumRegisteredTaskManagers.head._1 <=
          instanceManager.getNumberOfRegisteredTaskManagers) {
        val receiver = waitForNumRegisteredTaskManagers.dequeue()._2
        receiver ! Acknowledge.get()
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

  /**
    * No killing of the VM for testing.
    */
  override protected def shutdown(): Unit = {
    log.info("Shutting down TestingJobManager.")
    waitForShutdown.foreach(_ ! ComponentShutdown(self))
    waitForShutdown.clear()
  }
}
