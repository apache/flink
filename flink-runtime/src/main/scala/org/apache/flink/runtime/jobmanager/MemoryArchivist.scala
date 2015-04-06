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

import java.util

import akka.actor.Actor

import org.apache.flink.api.common.JobID
import org.apache.flink.runtime.jobgraph.JobStatus
import org.apache.flink.runtime.messages.webmonitor._
import org.apache.flink.runtime.{ActorSynchronousLogging, ActorLogMessages}
import org.apache.flink.runtime.executiongraph.ExecutionGraph
import org.apache.flink.runtime.messages.ArchiveMessages._
import org.apache.flink.runtime.messages.JobManagerMessages._

import scala.collection.mutable

/**
 * Actor which stores terminated Flink jobs. The number of stored Flink jobs is set by max_entries.
 * If this number is exceeded, the oldest job will be discarded. One can interact with the actor by
 * the following messages:
 *
 *  - [[ArchiveExecutionGraph]] archives the attached [[ExecutionGraph]]
 *
 *  - [[RequestArchivedJobs]] returns all currently stored [[ExecutionGraph]]s to the sender
 *  encapsulated in a [[ArchivedJobs]] message.
 *
 *  - [[RequestJob]] returns the corresponding [[org.apache.flink.runtime.jobgraph.JobGraph]]
 *  encapsulated in [[JobFound]] if the job is stored by the MemoryArchivist. If not, then
 *  [[JobNotFound]] is returned.
 *
 *  - [[RequestJobStatus]] returns the last state of the corresponding job. If the job can be found,
 *  then a [[CurrentJobStatus]] message with the last state is returned to the sender, otherwise
 *  a [[JobNotFound]] message is returned
 *
 *  - [[RequestJobCounts]] returns the number of finished, canceled, and failed jobs as a Tuple3
 *
 * @param max_entries Maximum number of stored Flink jobs
 */
class MemoryArchivist(private val max_entries: Int)
  extends Actor
  with ActorLogMessages
  with ActorSynchronousLogging {
  /*
   * Map of execution graphs belonging to recently started jobs with the time stamp of the last
   * received job event. The insert order is preserved through a LinkedHashMap.
   */
  val graphs = mutable.LinkedHashMap[JobID, ExecutionGraph]()

  /* Counters for finished, canceled, and failed jobs */
  var finishedCnt: Int = 0
  var canceledCnt: Int = 0
  var failedCnt: Int = 0

  override def receiveWithLogMessages: Receive = {
    
    /* Receive Execution Graph to archive */
    case ArchiveExecutionGraph(jobID, graph) => 
      // wrap graph inside a soft reference
      graphs.update(jobID, graph)
      // update job counters
      graph.getState match {
        case JobStatus.FINISHED => finishedCnt += 1
        case JobStatus.CANCELED => canceledCnt += 1
        case JobStatus.FAILED => failedCnt += 1
      }
      trimHistory()

    case RequestArchivedJob(jobID: JobID) =>
      val graph = graphs.get(jobID)
      sender ! ArchivedJob(graph)

    case RequestArchivedJobs =>
      sender ! ArchivedJobs(graphs.values)

    case RequestJob(jobID) =>
      graphs.get(jobID) match {
        case Some(graph) => sender ! JobFound(jobID, graph)
        case None => sender ! JobNotFound(jobID)
      }

    case RequestJobStatus(jobID) =>
      graphs.get(jobID) match {
        case Some(graph) => sender ! CurrentJobStatus(jobID, graph.getState)
        case None => sender ! JobNotFound(jobID)
      }

    case RequestJobCounts =>
      sender ! (finishedCnt, canceledCnt, failedCnt)

    case _ : RequestJobsOverview =>
      try {
        sender ! createJobsOverview()
      }
      catch {
        case t: Throwable => log.error("Exception while creating the jobs overview", t)
      }

    case _ : RequestJobsWithIDsOverview =>
      try {
        sender ! createJobsWithIDsOverview()
      }
      catch {
        case t: Throwable => log.error("Exception while creating the jobs overview", t)
      }
  }

  /**
   * Handle unmatched messages with an exception.
   */
  override def unhandled(message: Any): Unit = {
    // let the actor crash
    throw new RuntimeException("Received unknown message " + message)
  }


  // --------------------------------------------------------------------------
  //  Request Responses
  // --------------------------------------------------------------------------
  
  private def createJobsOverview() : JobsOverview = {
    var runningOrPending = 0
    var finished = 0
    var canceled = 0
    var failed = 0
    
    graphs.values.foreach {
      _.getState() match {
        case JobStatus.FINISHED => finished += 1
        case JobStatus.CANCELED => canceled += 1
        case JobStatus.FAILED => failed += 1
        case _ => runningOrPending += 1
      }
    }
    
    new JobsOverview(runningOrPending, finished, canceled, failed)
  }

  private def createJobsWithIDsOverview() : JobsWithIDsOverview = {
    val runningOrPending = new util.ArrayList[JobID]()
    val finished = new util.ArrayList[JobID]()
    val canceled = new util.ArrayList[JobID]()
    val failed = new util.ArrayList[JobID]()

    graphs.values.foreach { graph =>
      graph.getState() match {
        case JobStatus.FINISHED => finished.add(graph.getJobID)
        case JobStatus.CANCELED => canceled.add(graph.getJobID)
        case JobStatus.FAILED => failed.add(graph.getJobID)
        case _ => runningOrPending.add(graph.getJobID)
      }
    }

    new JobsWithIDsOverview(runningOrPending, finished, canceled, failed)
  }
  
  // --------------------------------------------------------------------------
  //  Utilities
  // --------------------------------------------------------------------------

  /**
   * Remove old ExecutionGraphs belonging to a jobID
   * * if more than max_entries are in the queue.
   */
  private def trimHistory(): Unit = {
    while (graphs.size > max_entries) {
      // get first graph inserted
      val (jobID, value) = graphs.head
      graphs.remove(jobID)
    }
  }
}
