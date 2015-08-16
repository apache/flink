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

import java.util.UUID

import akka.actor.ActorRef
import org.apache.flink.api.common.server.{UpdateStrategy, Update, Parameter}
import org.apache.flink.runtime.instance.{InstanceID, ActorGateway}
import org.apache.flink.runtime.server.KeyGatewayMapping

import scala.collection.mutable

/**
 * The parameter server specific messages
 */
object ServerMessages {

  // ================= Server - Task Manager - Job Manager =================

  /**
   * Sent by task manager to its ParameterServer to let it initialize itself and register with
   * the Job Manager as a server.
   *
   * @param jobManager Gateway to Job Manager
   * @param taskManager Gateway to task manager
   * @param taskManagerID Instance ID of the task manager
   */
  case class KickOffParameterServer(
      jobManager: ActorGateway,
      taskManager: ActorGateway,
      taskManagerID: InstanceID)

  /**
   * Sent by the ParameterServer to the JobManager to register itself. Also used as a heartbeat
   * message.
   *
   * @param taskManagerID Instance id of the parent task manager of the server
   * @param serverGateway Gateway to the server being made available on the network, i.e. ourselves
   */
  case class ServerHeartbeat(
      taskManagerID: InstanceID,
      serverGateway: ActorGateway)

  /**
   * Sent by JobManager in response to [[ServerHeartbeat]] and [[RequestKeyGateway]]
   *
   * @param keyGatewayMapping Map of where a request about a key must be forwarded
   * @param copyPartner Where all the data on this server must be copied to.
   */
  case class ServerRegistrationAcknowledge(
      keyGatewayMapping: Iterable[KeyGatewayMapping],
      copyPartner: ActorGateway)

  /**
   * Refusal to register a Server. This shouldn't happen.
   *
   * @param error Reason for refusing registration / heartbeat
   */
  case class ServerRegistrationRefuse(error: String)

  /**
   * Mechanism for registering a new key at some Server. Job Manager controls where this key will
   * go based on all the servers available.
   *
   * @param key Key to be mapped to a server gateway
   */
  case class RequestKeyGateway(key: String, serverGateway: ActorGateway)

  /**
   * Message to itself to trigger a heart beat to the Job Manager and checking connection to the
   * store manager
   */
  case object TriggerHeartbeat

  /**
   * Alarm at a server to itself to take care of pending client requests
   */
  case object ServerClearQueueReminder

  /**
   * Sent by the Parameter Server to the parent Task Manager and Job Manager to let them know of a
   * fatal error
   *
   * @param error Error that occurred in the Parameter Server
   */
  case class ServerError(serverTaskManagerID: InstanceID, error: Throwable)


  // ================ CLIENT MESSAGES ==========================================
  /**
   * All messages sent by a client to its parent server and vice-versa
   */

  sealed trait ClientRequests{
    def key: String
  }

  sealed trait ClientResponses

  /**
   * This message is sent by the [[org.apache.flink.api.common.functions.RuntimeContext]]
   * to register a key
   *
   * @param clientID Integer id of the client sending the request
   * @param key Key which this client wants to register to
   * @param value The value to be registered
   * @param strategy Update strategy to be adopted (should match across all clients)
   * @param slack Slack to be used for SSP (should match across all clients)
   */
  case class RegisterClient(
      clientID: Int,
      key: String,
      value: Parameter,
      strategy: UpdateStrategy,
      slack: Int)
    extends ClientRequests

  /**
   * Update a parameter at the [[org.apache.flink.runtime.server.ParameterServer]]'s store
   *
   * @param key Key whose value needs to be updates
   * @param value Update sent
   */
  case class UpdateParameter(key: String, value: Update)
    extends ClientRequests

  /**
   * Pull a parameter from the [[org.apache.flink.runtime.server.ParameterServer]]
   *
   * @param key Key whose value needs to be pulled
   */
  case class PullParameter(key: String)
    extends ClientRequests

  /**
   * Acknowledge the registration of the client
   */
  case class RegistrationSuccess() extends ClientResponses

  /**
   * Update request successful
   */
  case class UpdateSuccess() extends ClientResponses

  /**
   * Pull request successful
   *
   * @param value The parameter stored under key
   */
  case class PullSuccess(value: Parameter) extends ClientResponses

  /**
   * Any failure response to a message must send back the reason for failure
   *
   * @param error Reason for failure of client request
   */
  case class ClientFailure(error: Throwable) extends ClientResponses


  // ==================== DATA MESSAGES ==========================================

  /**
   * All messages by a client are forwarded by the parent server to an appropriate server under
   * this encapsulation.
   *
   * @param messageID Unique identifier for this message, which must be included in a response too
   * @param message Actual request message
   */

  case class ServerRequest(messageID: UUID, message: ClientRequests, origin: ActorGateway)

  /**
   * Acknowledgement of a successful client request receipt sent back to the original server.
   *
   * @param messageID What message id is this in response to
   * @param state Whether we can serve it or not
   * @param error If not, why not?
   */
  case class ServerAcknowledgement(messageID: UUID, state: Boolean, error: Throwable = null)

  /**
   * Result of a client request sent back to the original server
   *
   * @param messageID What message id is this in response to
   * @param result Response to be forwarded to the client
   */
  case class ServerResponse(messageID: UUID, result: ClientResponses)

  /**
   * Message sent by a server to itself to retry sending a client request
   *
   * @param message Actual client message
   * @param retryNumber Retry number
   * @param sender Actor Ref to the client who sent this message originally
   */
  case class ServerRetry(message: ClientRequests, retryNumber: Int, sender: ActorRef)

  /**
   * The following two messages are used by the Server to communicate with the store manager which
   * manages all the data stored at this server.
   *
   */
  case class StoreMessage(messageID: UUID, request: ClientRequests)

  case class StoreReply(messageID: UUID, reply: ClientResponses)

  class InvalidServerAccessException extends Exception
}
