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

import org.apache.flink.runtime.executiongraph.ExecutionAttemptID
import org.apache.flink.runtime.jobgraph.JobID
import org.apache.flink.runtime.taskmanager.Task

/**
 * Additional messages that the [[TestingTaskManager]] understands.
 */
object TestingTaskManagerMessages {
  
  case class NotifyWhenTaskRemoved(executionID: ExecutionAttemptID)
  
  case class ResponseRunningTasks(tasks: Map[ExecutionAttemptID, Task]){
    import collection.JavaConverters._
    def asJava: java.util.Map[ExecutionAttemptID, Task] = tasks.asJava
  }
  
  case class ResponseBroadcastVariablesWithReferences(number: Int)

  case class CheckIfJobRemoved(jobID: JobID)
  
  case object RequestRunningTasks
  
  case object RequestBroadcastVariablesWithReferences
  
  // --------------------------------------------------------------------------
  // Utility methods to allow simpler case object access from Java
  // --------------------------------------------------------------------------
  
  def getRequestRunningTasksMessage() : AnyRef = {
    RequestRunningTasks
  }
  
  def getRequestBroadcastVariablesWithReferencesMessage() : AnyRef = {
    RequestBroadcastVariablesWithReferences
  }
}

