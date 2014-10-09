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

import org.apache.flink.runtime.jobgraph.JobGraph

/**
 * This object contains the [[org.apache.flink.runtime.client.JobClient]] specific messages
 */
object JobClientMessages {

  /**
   * This message submits a jobGraph to the JobClient which sends it to the JobManager. The
   * JobClient waits until the job has been executed. If listenToEvents is true,
   * then the JobClient prints all state change messages to the console. The
   * JobClient sends the result of the execution back to the sender. If the execution is
   * successful then a [[org.apache.flink.runtime.messages.JobManagerMessages.JobResult]] is sent
   * back. If a [[org.apache.flink.runtime.messages.JobManagerMessages.SubmissionFailure]]
   * happens, then the cause is sent back to the sender().
   *
   * @param jobGraph containing the job description
   * @param listenToEvents if true then print state change messages
   */
  case class SubmitJobAndWait(jobGraph: JobGraph, listenToEvents: Boolean = false)

  /**
   * This message submits a jobGraph to the JobClient which sends it to the JobManager. The
   * JobClient awaits the
   * [[org.apache.flink.runtime.messages.JobManagerMessages.SubmissionResponse]]
   * from the JobManager and sends it back to the sender().
   *
   * @param jobGraph containing the job description
   */
  case class SubmitJobDetached(jobGraph: JobGraph)
}
