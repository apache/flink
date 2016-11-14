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
 * Generic messages between JobManager, TaskManager, JobClient.
 */
object Messages {

  /**
   * Signals that the receiver (JobManager/TaskManager) shall disconnect the sender.
   *
   * The TaskManager may send this on shutdown to let the JobManager realize the TaskManager
   * loss more quickly.
   *
   * The JobManager may send this message to its TaskManagers to let them clean up their
   * tasks that depend on the JobManager and go into a clean state.
   *
   * @param cause The reason for disconnecting, to be displayed in log and error messages.
   */
  case class Disconnect(instanceId: InstanceID, cause: Exception) extends RequiresLeaderSessionID
}
