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

import org.apache.flink.runtime.instance.{InstanceConnectionInfo, InstanceID, HardwareDescription}

object RegistrationMessages {

  /**
   * Registers a task manager at the job manager. A successful registration is acknowledged by
   * [[AcknowledgeRegistration]].
   *
   * @param connectionInfo
   * @param hardwareDescription
   * @param numberOfSlots
   */
  case class RegisterTaskManager(connectionInfo: InstanceConnectionInfo,
                                 hardwareDescription: HardwareDescription,
                                 numberOfSlots: Int)

  /**
   * Denotes the successful registration of a task manager at the job manager. This is the
   * response triggered by the [[RegisterTaskManager]] message.
   *
   * @param instanceID
   * @param blobPort
   */
  case class AcknowledgeRegistration(instanceID: InstanceID, blobPort: Int)

  /**
   * Denotes that the TaskManager has already been registered at the JobManager.
   *
   * @param instanceID
   * @param blobPort
   */
  case class AlreadyRegistered(instanceID: InstanceID, blobPort: Int)

  /**
   * Denotes the unsuccessful registration of a task manager at the job manager. This is the
   * response triggered by the [[RegisterTaskManager]] message.
   *
   * @param reason Reason why the task manager registration was refused
   */
  case class RefuseRegistration(reason: String)

}
