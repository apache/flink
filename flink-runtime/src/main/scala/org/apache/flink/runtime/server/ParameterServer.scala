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

import java.io.{File, IOException}
import java.net.InetSocketAddress
import java.util.UUID

import akka.actor._
import grizzled.slf4j.Logger
import org.apache.flink.configuration.{ConfigConstants, Configuration, GlobalConfiguration}
import org.apache.flink.runtime.akka.AkkaUtils
import org.apache.flink.runtime.messages.JobManagerMessages.{ResponseLeaderSessionID, RequestLeaderSessionID}
import org.apache.flink.runtime.messages.ServerMessages._
import org.apache.flink.runtime.process.ProcessReaper
import org.apache.flink.runtime.security.SecurityUtils
import org.apache.flink.runtime.security.SecurityUtils.FlinkSecuredRunner
import org.apache.flink.runtime.util.EnvironmentInformation
import org.apache.flink.runtime.{LeaderSessionMessages, FlinkActor, LogMessages}

import scala.concurrent._
import scala.concurrent.duration._
import scala.language.postfixOps


class ParameterServer(
    protected val flinkConfiguration: Configuration)
  extends FlinkActor
  with LeaderSessionMessages
  with LogMessages // order of the mixin is important, we want first logging
  {

  override val log = Logger(getClass)

  private val currentJobManager: Option[ActorRef] = None
  override val leaderSessionID: Option[UUID] = Some(UUID.randomUUID())



  /**
   * Run when the parameter server is started. Simply logs an informational message.
   */
  override def preStart(): Unit = {
    log.info(s"Starting ParameterServer at ${self.path.toSerializationFormat}.")
  }

  /**
   * Run post stopping
   */
  override def postStop(): Unit = {
    log.info(s"Stopping ParameterServer ${self.path.toSerializationFormat}.")

    log.debug(s"ParameterServer ${self.path} is completely stopped.")
  }

  override def handleMessage: Receive = {
    case RequestLeaderSessionID =>
      sender() ! ResponseLeaderSessionID(leaderSessionID)
  }

  /**
   * Handle unmatched messages with an exception.
   */
  override def unhandled(message: Any): Unit = {
    // let the actor crash
    throw new RuntimeException("Received unknown message " + message)
  }
}

/**
 * Parameter Server companion object. Contains the entry point (main method) to run the
 * Parameter Server in a standalone fashion. Also contains various utility methods to start
 * the ParameterServer and to look up the Parameter Server actor reference.
 */
object ParameterServer {

  /** Name of the ParameterServer actor */
  val PARAMETER_SERVER_NAME = "pmserver"

  /**
   * Starts the ParameterServer based on the given configuration, in the given actor system.
   *
   * @param configuration The configuration for the ParameterServer
   * @param actorSystem Teh actor system running the ParameterServer
   * @return A reference to ParameterServer
   */
  def startParameterServerActor(
      configuration: Configuration,
      actorSystem: ActorSystem)
    : ActorRef = {

    val parameterServerProps = Props(
      classOf[ParameterServer],
      configuration)

    actorSystem.actorOf(parameterServerProps)
  }
}
