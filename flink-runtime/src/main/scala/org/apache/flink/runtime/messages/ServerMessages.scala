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

import org.apache.flink.api.common.server.{UpdateStrategy, Update, Parameter}
import org.apache.flink.runtime.instance.{InstanceID, ActorGateway}

import scala.collection.mutable

/**
 * The parameter server specific messages
 */
object ServerMessages {

  // ================= Server - Task Manager - Job Manager =================

  /**
   * Sent by task manager to its ParameterServer to let it initialize itself and register with
   * the Job Manager.
   *
   * @param jobManager Gateway to Job Manager
   * @param taskManagerID
   */
  case class KickOffParameterServer(jobManager: ActorGateway, taskManagerID: InstanceID)

  /**
   * Sent by the ParameterServer to the JobManager to register itself. Also used as a heartbeat
   * message
   *
   * @param taskManagerID Instance id of the parent task manager of the server
   * @param serverGateway Gateway to the server being made available on the network.
   */
  case class ServerAvailable(taskManagerID: InstanceID, serverGateway: ActorGateway)

  /**
   * Sent by the Job Manager to all servers whenever a new server is added. Also, sent when a
   * server heartbeat message is received.
   *
   * @param serverList Map of all currently available servers registered
   */
  case class ServerRegistrationAcknowledge(serverList: mutable.HashMap[InstanceID, ActorGateway])

  /**
   * Refusal to register a Server. This shouldn't happen.
   *
   * @param error Reason for refusing registration / heartbeat
   */
  case class ServerRegistrationRefuse(error: String)


  // ================= REGISTRATION RELATED ============================
  /**
   * This message is sent by the [[org.apache.flink.api.common.functions.RuntimeContext]]
   * to register a key
   *
   * @param id Integer id of the client registering
   * @param key Key under which the value is to be registered
   * @param value The value to be registered
   * @param strategy Update strategy to be adopted
   * @param slack Slack to be used for SSP
   */
  case class RegisterClient(
      id: Int, key: String, value: Parameter, strategy: UpdateStrategy, slack: Int)

  /**
   * Acknowledge the registration of the client
   */
  case object ClientRegistrationSuccess

  def getClientRegistrationSuccess: AnyRef = {
    ClientRegistrationSuccess
  }

  /**
   * Refuse to register a client
   *
   * @param error Return the reason for refusing the registration
   */
  case class ClientRegistrationRefuse(error: String)


  // ================= UPDATE RELATED ==================================
  /**
   * Update a parameter at the [[org.apache.flink.runtime.server.ParameterServer]]'s store
   *
   * @param key Key whose value needs to be updates
   * @param value Update sent
   */
  case class UpdateParameter(key: String, value: Update)

  /**
   * Update success message
   */
  case object UpdateSuccess

  // java convenience object

  def getUpdateSuccess: AnyRef = {
    UpdateSuccess
  }

  /**
   * Update failure
   *
   * @param error Reason for failure of update
   */
  case class UpdateFailure(error: String)


  // ========================= PULL RELATED =========================
  /**
   * Pull a parameter from the [[org.apache.flink.runtime.server.ParameterServer]]
   *
   * @param key Key whose value needs to be pulled
   */
  case class PullParameter(key: String)

  /**
   * Pull request successful
   *
   * @param value The parameter stored under key
   */
  case class PullSuccess(value: Parameter)

  /**
   * Pull request failure
   *
   * @param error Reason for unsuccessful pull
   */
  case class PullFailure(error: String)

}
