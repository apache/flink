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
import java.util.Date

import org.apache.flink.runtime.execution.{ExecutionState}
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID
import org.apache.flink.runtime.jobgraph.{JobStatus, JobVertexID, JobID}

object ExecutionGraphMessages {

  case class ExecutionStateChanged(jobID: JobID, vertexID: JobVertexID,
                                   taskName: String, totalNumberOfSubTasks: Int, subtask: Int,
                                   executionID: ExecutionAttemptID,
                                   newExecutionState: ExecutionState, timestamp: Long,
                                   optionalMessage: String){
    override def toString: String = {
      s"${timestampToString(timestamp)}\t$taskName(${subtask +
        1}/${totalNumberOfSubTasks}) switched to $newExecutionState ${if(optionalMessage != null)
        s"\n${optionalMessage}" else ""}"
    }
  }

  case class JobStatusChanged(jobID: JobID, newJobStatus: JobStatus, timestamp: Long,
                              optionalMessage: String){
    override def toString: String = {
      s"${timestampToString(timestamp)}\tJob execution switched to status ${newJobStatus}."
    }
  }


  private val DATE_FORMATTER: SimpleDateFormat = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss")

  private def timestampToString(timestamp: Long): String = {
    DATE_FORMATTER.format(new Date(timestamp))
  }

}
