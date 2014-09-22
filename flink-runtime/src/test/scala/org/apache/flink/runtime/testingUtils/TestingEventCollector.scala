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

import akka.pattern.{ask, pipe}
import org.apache.flink.runtime.ActorLogMessages
import org.apache.flink.runtime.akka.AkkaUtils
import org.apache.flink.runtime.jobmanager.EventCollector
import org.apache.flink.runtime.testingUtils.TestingJobManagerMessages.{ExecutionGraphNotFound, ExecutionGraphFound, RequestExecutionGraph}

import scala.concurrent.Future
import scala.concurrent.duration._

trait TestingEventCollector extends ActorLogMessages {
  self: EventCollector =>

  import context.dispatcher
  import org.apache.flink.runtime.akka.AkkaUtils.FUTURE_TIMEOUT

  abstract override def receiveWithLogMessages: Receive = {
    receiveTestingMessages orElse super.receiveWithLogMessages
  }

  def receiveTestingMessages: Receive = {
    case RequestExecutionGraph(jobID) =>
      recentExecutionGraphs.get(jobID) match {
        case Some(executionGraph) => sender() ! ExecutionGraphFound(jobID, executionGraph)
        case None =>
          val responses = archiveListeners map {
            listener =>
              listener ? RequestExecutionGraph(jobID) filter {
                case x: ExecutionGraphFound => true
                case x: ExecutionGraphNotFound => false
              }
          }

          val notFound = akka.pattern.after(200 millis, this.context.system.scheduler){
            Future.successful{ExecutionGraphNotFound(jobID)}
          }

          Future firstCompletedOf(responses + notFound) pipeTo sender()
      }

  }
}
