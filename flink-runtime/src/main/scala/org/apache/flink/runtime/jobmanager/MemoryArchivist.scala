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

class MemoryArchivist(private val max_entries: Int) extends Actor with ActorLogMessages with
ActorLogging {
  /**
   * Map of execution graphs belonging to recently started jobs with the time stamp of the last
   * received job event.
   */
  val graphs = collection.mutable.HashMap[JobID, ExecutionGraph]()
  val lru = collection.mutable.Queue[JobID]()

  override def receiveWithLogMessages: Receive = {
    case ArchiveExecutionGraph(jobID, graph) => {
      graphs.update(jobID, graph)
      cleanup(jobID)
    }

    case RequestArchivedJobs => {
      sender ! ArchivedJobs(graphs.values)
    }

    case RequestJob(jobID) => {
      graphs.get(jobID) match {
        case Some(graph) => sender ! JobFound(jobID, graph)
        case None => sender ! JobNotFound(jobID)
      }
    }

    case RequestJobStatus(jobID) => {
      graphs.get(jobID) match {
        case Some(eg) => sender ! CurrentJobStatus(jobID, eg.getState)
        case None => sender ! JobNotFound(jobID)
      }
    }
  }

  def cleanup(jobID: JobID): Unit = {
    if (!lru.contains(jobID)) {
      lru.enqueue(jobID)
    }

    while (lru.size > max_entries) {
      val removedJobID = lru.dequeue()
      graphs.remove(removedJobID)
    }
  }
}
