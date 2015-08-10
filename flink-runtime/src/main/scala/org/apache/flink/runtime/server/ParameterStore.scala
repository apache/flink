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

package org.apache.flink.runtime.server

import java.util.UUID

import _root_.akka.actor._
import grizzled.slf4j.Logger
import org.apache.flink.runtime.messages.JobManagerMessages.{RequestLeaderSessionID, ResponseLeaderSessionID}
import org.apache.flink.runtime.{FlinkActor, LeaderSessionMessages, LogMessages}

import scala.language.postfixOps


class ParameterStore extends FlinkActor with LeaderSessionMessages with LogMessages{

  override val log = Logger(getClass)

  override val leaderSessionID: Option[UUID] = Some(UUID.randomUUID())

  /**
   * Run when the parameter store is started. Simply logs an informational message.
   */
  override def preStart(): Unit = {
    log.info(s"Starting ParameterStore at ${self.path.toSerializationFormat}.")
  }

  /**
   * Run post stopping
   */
  override def postStop(): Unit = {

    log.info(s"Stopping ParameterStore ${self.path.toSerializationFormat}.")

    log.debug(s"ParameterStore ${self.path} is completely stopped.")
  }

  override def handleMessage: Receive = {
    case RequestLeaderSessionID =>
      sender() ! ResponseLeaderSessionID(leaderSessionID)
  }

  /**
   * Handle unmatched messages with an exception.
   */
  override def unhandled(message: Any): Unit = {
    // crash
    throw new RuntimeException("Received unknown message " + message)
  }
}
