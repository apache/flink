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

import akka.actor.ActorRef
import org.apache.flink.api.common.JobID
import org.apache.flink.runtime.accumulators.AccumulatorRegistry
import org.apache.flink.runtime.executiongraph.{ExecutionAttemptID, ExecutionGraph}
import org.apache.flink.runtime.instance.InstanceGateway
import org.apache.flink.runtime.jobgraph.JobStatus
import java.util.Map
import org.apache.flink.api.common.accumulators.Accumulator

object TestingJobManagerMessages {

  case class RequestExecutionGraph(jobID: JobID)

  sealed trait ResponseExecutionGraph {
    def jobID: JobID
  }

  case class ExecutionGraphFound(jobID: JobID, executionGraph: ExecutionGraph) extends
  ResponseExecutionGraph

  case class ExecutionGraphNotFound(jobID: JobID) extends ResponseExecutionGraph

  case class WaitForAllVerticesToBeRunning(jobID: JobID)
  case class WaitForAllVerticesToBeRunningOrFinished(jobID: JobID)
  case class AllVerticesRunning(jobID: JobID)

  case class NotifyWhenJobRemoved(jobID: JobID)

  case class RequestWorkingTaskManager(jobID: JobID)
  case class WorkingTaskManager(gatewayOption: Option[InstanceGateway])

  case class NotifyWhenJobStatus(jobID: JobID, state: JobStatus)
  case class JobStatusIs(jobID: JobID, state: JobStatus)

  case object NotifyListeners

  case class NotifyWhenTaskManagerTerminated(taskManager: ActorRef)
  case class TaskManagerTerminated(taskManager: ActorRef)

  case class RequestAccumulatorValues(jobID: JobID)
  case class RequestAccumulatorValuesResponse(jobID: JobID,
    flinkAccumulators: Map[ExecutionAttemptID, Map[AccumulatorRegistry.Metric, Accumulator[_,_]]],
    userAccumulators: Map[String, Accumulator[_,_]])
}
