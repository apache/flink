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

import akka.actor.{ActorLogging, Actor}
import org.apache.flink.runtime.ActorLogMessages
import org.apache.flink.runtime.executiongraph.ExecutionGraph
import org.apache.flink.runtime.jobgraph.JobID
import org.apache.flink.runtime.messages.ArchiveMessages._
import org.apache.flink.runtime.messages.JobManagerMessages._

import scala.collection.mutable
import scala.ref.SoftReference

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
 * @param max_entries Maximum number of stored Flink jobs
 */
class MemoryArchivist(private val max_entries: Int) extends Actor with ActorLogMessages with
ActorLogging {
  /*
   * Map of execution graphs belonging to recently started jobs with the time stamp of the last
   * received job event. The insert order is preserved through a LinkedHashMap.
   */
  val graphs = mutable.LinkedHashMap[JobID, SoftReference[ExecutionGraph]]()

  override def receiveWithLogMessages: Receive = {
    
    /* Receive Execution Graph to archive */
    case ArchiveExecutionGraph(jobID, graph) => 
      // wrap graph inside a soft reference
      graphs.update(jobID, new SoftReference(graph))

      trimHistory()

    case RequestArchivedJobs =>
      sender ! ArchivedJobs(getAllGraphs)

    case RequestJob(jobID) =>
      getGraph(jobID) match {
        case Some(graph) => sender ! JobFound(jobID, graph)
        case None => sender ! JobNotFound(jobID)
      }

    case RequestJobStatus(jobID) =>
      getGraph(jobID) match {
        case Some(graph) => sender ! CurrentJobStatus(jobID, graph.getState)
        case None => sender ! JobNotFound(jobID)
      }
  }

  /**
   * Gets all graphs that have not been garbage collected.
   * @return An iterable with all valid ExecutionGraphs
   */
  protected def getAllGraphs: Iterable[ExecutionGraph] = graphs.values.flatMap(_.get)

  /**
   * Gets a graph with a jobID if it has not been garbage collected.
   * @param jobID
   * @return ExecutionGraph or null
   */
  protected def getGraph(jobID: JobID): Option[ExecutionGraph] = graphs.get(jobID) match {
    case Some(softRef) => softRef.get
    case None => None
  }
  

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
