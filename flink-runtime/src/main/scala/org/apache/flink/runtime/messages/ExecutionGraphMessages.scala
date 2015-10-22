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

package org.apache.flink.runtime.messages

import java.text.SimpleDateFormat
import java.util.{UUID, Date}

import org.apache.flink.api.common.JobID
import org.apache.flink.runtime.execution.ExecutionState
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID
import org.apache.flink.runtime.jobgraph.{JobStatus, JobVertexID}

/**
 * This object contains the execution graph specific messages.
 */
object ExecutionGraphMessages {

  // --------------------------------------------------------------------------
  //  Messages
  // --------------------------------------------------------------------------
  
  /**
   * Denotes the execution state change of an
   * [[org.apache.flink.runtime.executiongraph.ExecutionVertex]]
   *
   * @param jobID to which the vertex belongs
   * @param vertexID of the ExecutionJobVertex to which the ExecutionVertex belongs
   * @param taskName
   * @param totalNumberOfSubTasks denotes the number of parallel subtasks
   * @param subtaskIndex denotes the index of the ExecutionVertex
   * @param executionID
   * @param newExecutionState
   * @param timestamp of the execution state change
   * @param optionalMessage
   */
  case class ExecutionStateChanged(
      jobID: JobID,
      vertexID: JobVertexID,
      taskName: String,
      totalNumberOfSubTasks: Int,
      subtaskIndex: Int,
      executionID: ExecutionAttemptID,
      newExecutionState: ExecutionState,
      timestamp: Long,
      optionalMessage: String)
    extends RequiresLeaderSessionID {

    override def toString: String = {
      val oMsg = if (optionalMessage != null) {
        s"\n$optionalMessage"
      } else {
        ""
      }
      
      s"${timestampToString(timestamp)}\t$taskName(${subtaskIndex +
        1}/$totalNumberOfSubTasks) switched to $newExecutionState $oMsg"
    }
  }

  /**
   * Denotes the job state change of a job.
   *
   * @param jobID identifying the corresponding job
   * @param newJobStatus
   * @param timestamp
   * @param error
   */
  case class JobStatusChanged(
      jobID: JobID,
      newJobStatus: JobStatus,
      timestamp: Long,
      error: Throwable)
    extends RequiresLeaderSessionID {
    
    override def toString: String = {
      s"${timestampToString(timestamp)}\tJob execution switched to status $newJobStatus."
    }
  }

  // --------------------------------------------------------------------------
  //  Utilities
  // --------------------------------------------------------------------------
  
  private val DATE_FORMATTER: SimpleDateFormat = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss")

  private def timestampToString(timestamp: Long): String = {
    DATE_FORMATTER.synchronized {
      DATE_FORMATTER.format(new Date(timestamp))
    }
  }
}
