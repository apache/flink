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

import org.apache.flink.runtime.accumulators.AccumulatorEvent
import org.apache.flink.runtime.instance.InstanceConnectionInfo
import org.apache.flink.runtime.io.network.ConnectionInfoLookupResponse
import org.apache.flink.runtime.io.network.channels.ChannelID
import org.apache.flink.runtime.jobgraph.{JobVertexID, JobID, JobGraph}
import org.apache.flink.runtime.jobmanager.RunningJob
import org.apache.flink.runtime.taskmanager.TaskExecutionState

import scala.collection.convert.{WrapAsScala, WrapAsJava}

object JobManagerMessages {

  case class SubmitJob(jobGraph: JobGraph, listenToEvents: Boolean = false,
                       detach: Boolean = false)

  case class CancelJob(jobID: JobID)

  case class UpdateTaskExecutionState(taskExecutionState: TaskExecutionState)

  case class RequestNextInputSplit(jobID: JobID, vertexID: JobVertexID)

  case class LookupConnectionInformation(caller: InstanceConnectionInfo, jobID: JobID,
                                         sourceChannelID: ChannelID)

  case class ConnectionInformation(response: ConnectionInfoLookupResponse)

  case class ReportAccumulatorResult(accumulatorEvent: AccumulatorEvent)

  case class RequestJobStatus(jobID: JobID)

  case object RequestInstances

  case object RequestNumberRegisteredTaskManager

  case object RequestAvailableSlots

  case object RequestBlobManagerPort

  case class RequestFinalJobStatus(jobID: JobID)

  sealed trait JobResult{
    def jobID: JobID
  }

  case class JobResultSuccess(jobID: JobID, runtime: Long, accumulatorResults: java.util.Map[String,
    AnyRef]) extends JobResult {}

  case class JobResultCanceled(jobID: JobID, msg: String)

  case class JobResultFailed(jobID: JobID, msg:String)

  sealed trait SubmissionResponse{
    def jobID: JobID
  }

  case class SubmissionSuccess(jobID: JobID) extends SubmissionResponse
  case class SubmissionFailure(jobID: JobID, cause: Throwable) extends SubmissionResponse

  sealed trait CancellationResponse{
    def jobID: JobID
  }

  case class CancellationSuccess(jobID: JobID) extends CancellationResponse
  case class CancellationFailure(jobID: JobID, cause: Throwable) extends CancellationResponse

  case object RequestRunningJobs

  case class RunningJobsResponse(runningJobs: Seq[RunningJob]) {
    def this() = this(Seq())
    def asJavaList: java.util.List[RunningJob] = {
      import scala.collection.JavaConversions.seqAsJavaList
      seqAsJavaList(runningJobs)
    }
  }

}
