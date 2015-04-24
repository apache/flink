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

import akka.actor.Actor
import org.apache.flink.runtime.{ActorSynchronousLogging, ActorLogMessages}
import org.apache.flink.runtime.messages.JobManagerProfilerMessages.ReportProfilingData
import org.apache.flink.runtime.profiling.impl.types.{InternalInstanceProfilingData, InternalExecutionVertexThreadProfilingData}

import scala.collection.convert.WrapAsScala

/**
 * Basic skeleton for the JobManager profiler. Currently, it simply logs the received messages.
 */
class JobManagerProfiler
  extends Actor
  with ActorLogMessages
  with ActorSynchronousLogging
  with WrapAsScala {
  override def receiveWithLogMessages: Receive = {
    case ReportProfilingData(profilingContainer) =>
      profilingContainer.getIterator foreach {
        case x: InternalExecutionVertexThreadProfilingData =>
          log.info(s"Received InternalExecutionVertexThreadProfilingData $x.")
        case x: InternalInstanceProfilingData =>
          log.info(s"Received InternalInstanceProfilingData $x.")
        case x =>
          log.error(s"Received unknown profiling data: ${x.getClass.getName}" )
      }
  }

  /**
   * Handle unmatched messages with an exception.
   */
  override def unhandled(message: Any): Unit = {
    // let the actor crash
    throw new RuntimeException("Received unknown message " + message)
  }
}
