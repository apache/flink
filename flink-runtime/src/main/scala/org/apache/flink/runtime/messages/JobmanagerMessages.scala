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
import org.apache.flink.runtime.profiling.impl.types.ProfilingDataContainer
import org.apache.flink.runtime.taskmanager.TaskExecutionState

object JobManagerMessages {
  case class SubmitJob(jobGraph: JobGraph)
  case class CancelJob(jobID: JobID)
  case class UpdateTaskExecutionState(taskExecutionState: TaskExecutionState)
  case class RequestNextInputSplit(jobID: JobID, vertexID: JobVertexID)
  case class LookupConnectionInformation(caller: InstanceConnectionInfo, jobID: JobID, sourceChannelID: ChannelID)
  case class ConnectionInformation(response: ConnectionInfoLookupResponse)
  case class ReportAccumulatorResult(accumulatorEvent: AccumulatorEvent)
  case class RequestAccumulatorResult(jobID: JobID)

  case object RequestInstances
  case object RequestNumberRegisteredTaskManager
  case object RequestAvailableSlots
  case object RequestPollingInterval
}
