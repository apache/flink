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

package org.apache.flink.runtime.jobmanager


import akka.actor.{Terminated, ActorRef, Actor, ActorLogging}
import org.apache.flink.runtime.ActorLogMessages
import org.apache.flink.runtime.event.job._
import org.apache.flink.runtime.executiongraph._
import org.apache.flink.runtime.jobgraph.{JobStatus, JobID}
import org.apache.flink.runtime.messages.ArchiveMessages.{ArchiveExecutionGraph, ArchiveJobEvent, ArchiveEvent}
import org.apache.flink.runtime.messages.EventCollectorMessages._
import org.apache.flink.runtime.messages.ExecutionGraphMessages.{JobStatusChanged, ExecutionStateChanged}
import org.apache.flink.runtime.messages.JobResult
import org.apache.flink.runtime.messages.JobResult.JobProgressResult
import scala.collection.convert.{WrapAsScala}
import scala.concurrent.duration._

class EventCollector(val timerTaskInterval: Int) extends Actor with ActorLogMessages with ActorLogging with WrapAsScala {
  import context.dispatcher

  val collectedEvents = collection.mutable.HashMap[JobID, List[AbstractEvent]]()

  val recentJobs = collection.mutable.HashMap[JobID, RecentJobEvent]()

  val recentExecutionGraphs = collection.mutable.HashMap[JobID, ExecutionGraph]()

  val archiveListeners = collection.mutable.HashSet[ActorRef]()

  val jobInformation = collection.mutable.HashMap[JobID, (String, Boolean, Long)]()

  override def preStart():Unit = {
    startArchiveExpiredEvent()
  }

  override def postStop(): Unit ={
    collectedEvents.clear()
    recentJobs.clear()
    recentExecutionGraphs.clear()
    archiveListeners.clear()
    jobInformation.clear()
  }

  def startArchiveExpiredEvent():Unit = {
    val schedulerDuration = FiniteDuration(2*timerTaskInterval, SECONDS)
    context.system.scheduler.schedule(schedulerDuration, schedulerDuration, self, ArchiveExpiredEvents)
  }

  override def receiveWithLogMessages: Receive = {
    case ArchiveExpiredEvents =>
      val currentTime = System.currentTimeMillis()

      collectedEvents.retain{
        (jobID, events) =>
          val (outdatedElements, currentElements) = events.partition{
            event => event.getTimestamp + timerTaskInterval < currentTime
          }

          outdatedElements foreach ( archiveEvent(jobID, _) )
          currentElements.nonEmpty
      }

      recentJobs.retain{
        (jobID, recentJobEvent) =>
          import JobStatus._
          val status = recentJobEvent.getJobStatus

          // only remove jobs which have stopped running
          if((status == FINISHED || status == CANCELED || status != FAILED) &&
            recentJobEvent.getTimestamp + timerTaskInterval < currentTime){
            archiveJobEvent(jobID, recentJobEvent)
            archiveExecutionGraph(jobID, recentExecutionGraphs.remove(jobID).get)
            jobInformation.remove(jobID)
            false
          }else{
            true
          }
      }

    case RequestJobEvents(jobID, includeManagementEvents) =>
      val events = collectedEvents.getOrElse(jobID, List())
      val filteredEvents = events filter { event => !event.isInstanceOf[ManagementEvent] || includeManagementEvents}

      sender() ! JobEvents(filteredEvents)

    case RequestJobProgress(jobID) =>
      sender() ! JobProgressResult(JobResult.SUCCESS, null, collectedEvents.getOrElse(jobID, List()))


    case RequestRecentJobs =>
      sender() ! RecentJobs(recentJobs.values.toList)

    case RegisterJob(executionGraph, profilingAvailable, submissionTimestamp) =>
      val jobID = executionGraph.getJobID

      executionGraph.registerExecutionListener(self)
      executionGraph.registerJobStatusListener(self)
      jobInformation += jobID -> (executionGraph.getJobName, profilingAvailable, submissionTimestamp)

    case ExecutionStateChanged(jobID, vertexID, subtask, executionID, newExecutionState, optionalMessage) =>
      val timestamp = System.currentTimeMillis()

      recentExecutionGraphs.get(jobID) match {
        case Some(graph) =>
          val vertex = graph.getJobVertex(vertexID)
          val taskName = if(vertex != null) vertex.getJobVertex.getName else "(null)"
          val totalNumberOfSubtasks = if(vertex != null) vertex.getParallelism else -1

          val vertexEvent = new VertexEvent(timestamp, vertexID, taskName, totalNumberOfSubtasks, subtask, executionID,
            newExecutionState, optionalMessage)

          val events = collectedEvents.getOrElse(jobID, List())
          val executionStateChangeEvent = new ExecutionStateChangeEvent(timestamp, vertexID, subtask,
            executionID, newExecutionState)

          collectedEvents += jobID -> (executionStateChangeEvent :: vertexEvent :: events)
        case None =>
          log.warning(s"Could not find execution graph with jobID ${jobID}.")
      }

    case JobStatusChanged(executionGraph, newJobStatus, optionalMessage) =>
      val jobID = executionGraph.getJobID()

      if(newJobStatus == JobStatus.RUNNING){
        this.recentExecutionGraphs += jobID -> executionGraph
      }

      val currentTime = System.currentTimeMillis()
      val (jobName, isProfilingEnabled, submissionTimestamp) = jobInformation(jobID)
      recentJobs.put(jobID, new RecentJobEvent(jobID, jobName, newJobStatus, isProfilingEnabled, submissionTimestamp,
        currentTime))

      val events = collectedEvents.getOrElse(jobID, List())
      collectedEvents += jobID -> ((new JobEvent(currentTime, newJobStatus, optionalMessage))::events)

    case ProcessProfilingEvent(profilingEvent) =>
      val events = collectedEvents.getOrElse(profilingEvent.getJobID, List())
      collectedEvents += profilingEvent.getJobID -> (profilingEvent::events)

    case RegisterArchiveListener(actorListener) =>
      context.watch(actorListener)
      archiveListeners += actorListener

    case Terminated(terminatedListener) =>
      archiveListeners -= terminatedListener
  }

  private def archiveEvent(jobID: JobID, event: AbstractEvent): Unit = {
    for(listener <- archiveListeners){
      listener ! ArchiveEvent(jobID, event)
    }
  }

  private def archiveJobEvent(jobID: JobID, event: RecentJobEvent): Unit = {
    for(listener <- archiveListeners){
      listener ! ArchiveJobEvent(jobID, event)
    }
  }

  private def archiveExecutionGraph(jobID: JobID, graph: ExecutionGraph): Unit = {
    for(listener <- archiveListeners){
      listener ! ArchiveExecutionGraph(jobID, graph)
    }
  }
}
