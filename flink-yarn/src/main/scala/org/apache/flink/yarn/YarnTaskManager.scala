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

package org.apache.flink.yarn

import org.apache.flink.runtime.ActorLogMessages
import org.apache.flink.runtime.taskmanager.TaskManager
import org.apache.flink.yarn.Messages.StopYarnSession

trait YarnTaskManager extends ActorLogMessages {
  that: TaskManager =>

  abstract override def receiveWithLogMessages: Receive = {
    receiveYarnMessages orElse super.receiveWithLogMessages
  }

  def receiveYarnMessages: Receive = {
    case StopYarnSession(status) =>
      log.info(s"Stopping YARN TaskManager with final application status $status")
      context.system.shutdown()
  }
}
