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
import org.apache.flink.runtime.messages.JobManagerProfilerMessages.ReportProfilingData
import org.apache.flink.runtime.profiling.impl.types.{InternalInstanceProfilingData, InternalExecutionVertexThreadProfilingData}
import org.apache.flink.runtime.profiling.types.ThreadProfilingEvent

import scala.collection.convert.WrapAsScala

class JobManagerProfiler extends Actor with ActorLogMessages with ActorLogging with WrapAsScala {
  override def receiveWithLogMessages: Receive = {
    case ReportProfilingData(profilingContainer) => {

      profilingContainer.getIterator foreach {
        case x: InternalExecutionVertexThreadProfilingData =>
          log.info(s"Received InternalExecutionVertexThreadProfilingData ${x}.")
        case x: InternalInstanceProfilingData =>
          log.info(s"Received InternalInstanceProfilingData ${x}.")

        case x =>
          log.error(s"Received unknown profiling data: ${x.getClass.getName}" )
      }
    }
  }
}
