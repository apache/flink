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

import akka.actor.Props
import org.apache.flink.runtime.ActorLogMessages
import org.apache.flink.runtime.jobmanager.{EventCollector, JobManager, MemoryArchivist}
import org.apache.flink.runtime.testingUtils.TestingJobManagerMessages.{ExecutionGraphFound,
RequestExecutionGraph}


trait TestingJobManager extends ActorLogMessages {
  self: JobManager =>

  override def archiveProps = Props(new MemoryArchivist(archiveCount) with TestingMemoryArchivist)

  override def eventCollectorProps = Props(new EventCollector(recommendedPollingInterval) with
    TestingEventCollector)

  abstract override def receiveWithLogMessages: Receive = {
    receiveTestingMessages orElse super.receiveWithLogMessages
  }

  def receiveTestingMessages: Receive = {
    case RequestExecutionGraph(jobID) =>
      currentJobs.get(jobID) match {
        case Some(executionGraph) => sender() ! ExecutionGraphFound(jobID, executionGraph)
        case None => eventCollector.tell(RequestExecutionGraph(jobID), sender())
      }
  }
}
