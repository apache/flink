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

import scala.collection.mutable.LinkedHashMap
import scala.ref.SoftReference

class MemoryArchivist(private val max_entries: Int) extends Actor with ActorLogMessages with
ActorLogging {
  /**
   * Map of execution graphs belonging to recently started jobs with the time stamp of the last
   * received job event. The insert order is preserved through a LinkedHashMap.
   */
  val graphs = LinkedHashMap[JobID, SoftReference[ExecutionGraph]]()

  override def receiveWithLogMessages: Receive = {
    /* Receive Execution Graph to archive */
    case ArchiveExecutionGraph(jobID, graph) => {
      // wrap graph inside a soft reference
      graphs.update(jobID, new SoftReference(graph))

      // clear all execution edges of the graph
      val iter = graph.getAllExecutionVertices().iterator()
      while (iter.hasNext) {
        iter.next().clearExecutionEdges()
      }

      cleanup(jobID)
    }

    case RequestArchivedJobs => {
      sender ! ArchivedJobs(getAllGraphs())
    }

    case RequestJob(jobID) => {
      getGraph(jobID) match {
        case graph: ExecutionGraph => sender ! JobFound(jobID, graph)
        case _ => sender ! JobNotFound(jobID)
      }
    }

    case RequestJobStatus(jobID) => {
      getGraph(jobID) match {
        case graph: ExecutionGraph => sender ! CurrentJobStatus(jobID, graph.getState)
        case _ => sender ! JobNotFound(jobID)
      }
    }
  }

  /**
   * Gets all graphs that have not been garbage collected.
   * @return An iterable with all valid ExecutionGraphs
   */
  def getAllGraphs() = graphs.values.flatMap(ref => ref.get match {
    case Some(graph) => Seq(graph)
    case _ => Seq()
  })

  /**
   * Gets a graph with a jobID if it has not been garbage collected.
   * @param jobID
   * @return ExecutionGraph or null
   */
  def getGraph(jobID: JobID) = graphs.get(jobID) match {
    case Some(softRef) => softRef.get match {
      case Some(graph) => graph
      case None => null
    }
  }

  /**
   * Remove old ExecutionGraphs belonging to a jobID
   * * if more than max_entries are in the queue.
   * @param jobID
   */
  private def cleanup(jobID: JobID): Unit = {
    while (graphs.size > max_entries) {
      // get first graph inserted
      val (jobID, value) = graphs.iterator.next()
      graphs.remove(jobID)
    }
  }
}
