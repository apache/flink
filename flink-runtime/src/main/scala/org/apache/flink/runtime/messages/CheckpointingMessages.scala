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

import org.apache.flink.api.common.JobID
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID
import org.apache.flink.runtime.jobgraph.JobVertexID
import org.apache.flink.runtime.state.StateHandle

/**
 * Actor messages specific to checkpoints (triggering, acknowledging,
 * state transfer, ...)
 */
object CheckpointingMessages {

  /**
   * Abstract base trait for all checkpoint messages.
   */
  trait CheckpointingMessage

  // --------------------------------------------------------------------------

  case class BarrierReq(attemptID: ExecutionAttemptID,
                        checkpointID: Long) extends CheckpointingMessage

  case class BarrierAck(jobID: JobID,
                        jobVertexID:JobVertexID,
                        instanceID: Int,
                        checkpointID: Long) extends CheckpointingMessage

  case class StateBarrierAck(jobID: JobID,
                             jobVertexID: JobVertexID,
                             instanceID: Integer,
                             checkpointID: java.lang.Long,
                             states: StateHandle) extends CheckpointingMessage
}
