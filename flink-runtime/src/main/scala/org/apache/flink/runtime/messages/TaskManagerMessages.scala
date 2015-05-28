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

import org.apache.flink.runtime.instance.InstanceID

/**
 * Miscellaneous actor messages exchanged with the TaskManager.
 */
object TaskManagerMessages {
  
  /**
   * This message informs the TaskManager about a fatal error that prevents
   * it from continuing.
   * 
   * @param description The description of the problem
   */
  case class FatalError(description: String, cause: Throwable)
  
  /**
   * Tells the task manager to send a heartbeat message to the job manager.
   */
  case object SendHeartbeat {

    /**
     * Accessor for the case object instance, to simplify Java interoperability.
     * @return The SendHeartbeat case object instance.
     */
    def get() : SendHeartbeat.type = SendHeartbeat
  }

  /**
   * Reports liveliness of the TaskManager instance with the given instance ID to the
   * This message is sent to the job. This message reports the TaskManagers
   * metrics, as a byte array.
   *
   * @param instanceID The instance ID of the reporting TaskManager.
   * @param metricsReport utf-8 encoded JSON metrics report from the metricRegistry.
   */
  case class Heartbeat(instanceID: InstanceID, metricsReport: Array[Byte])


  // --------------------------------------------------------------------------
  //  Reporting the current TaskManager stack trace
  // --------------------------------------------------------------------------

  /**
   * Tells the TaskManager to send a stack trace of all threads to the sender.
   * The response to this message is the [[StackTrace]] message.
   */
  case object SendStackTrace {

    /**
     * Accessor for the case object instance, to simplify Java interoperability.
     * @return The SendStackTrace case object instance.
     */
    def get() : SendStackTrace.type = SendStackTrace
  }

  /**
   * Communicates the stack trace of the TaskManager with the given ID.
   * This message is the response to [[SendStackTrace]].
   *
   * @param instanceID The ID of the responding task manager.
   * @param stackTrace The stack trace, as a string.
   */
  case class StackTrace(instanceID: InstanceID, stackTrace: String)


  // --------------------------------------------------------------------------
  //  Utility messages used for notifications during TaskManager startup
  // --------------------------------------------------------------------------

  /**
   * Requests a notification from the task manager as soon as the task manager has been
   * registered at the job manager. Once the task manager is registered at the job manager a
   * [[RegisteredAtJobManager]] message will be sent to the sender.
   */
  case object NotifyWhenRegisteredAtJobManager

  /**
   * Acknowledges that the task manager has been successfully registered at the job manager. This
   * message is a response to [[NotifyWhenRegisteredAtJobManager]].
   */
  case object RegisteredAtJobManager


  // --------------------------------------------------------------------------
  //  Utility getters for case objects to simplify access from Java
  // --------------------------------------------------------------------------

  /**
   * Accessor for the case object instance, to simplify Java interoperability.
   * @return The NotifyWhenRegisteredAtJobManager case object instance.
   */
  def getNotifyWhenRegisteredAtJobManagerMessage:
            NotifyWhenRegisteredAtJobManager.type = NotifyWhenRegisteredAtJobManager

  /**
   * Accessor for the case object instance, to simplify Java interoperability.
   * @return The RegisteredAtJobManager case object instance.
   */
  def getRegisteredAtJobManagerMessage:
            RegisteredAtJobManager.type = RegisteredAtJobManager

}
