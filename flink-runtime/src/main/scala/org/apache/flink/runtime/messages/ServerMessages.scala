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

/**
 * The parameter server specific messages
 */
object ServerMessages {

  // ================= REGISTRATION RELATED ============================
  /**
   * This message is sent by the [[org.apache.flink.runtime.client.ParameterClient]]
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
   * @param reason Return the reason for refusing the registration
   */
  case class ClientRegistrationRefuse(reason: String)


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
