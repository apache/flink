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
import org.apache.flink.runtime.util.SerializedThrowable

/**
 * This object contains the [[org.apache.flink.runtime.client.JobClient]] specific messages
 */
object JobClientMessages {

  /**
   * This message is sent to the JobClient (via ask) to submit a job and
   * get a response when the job execution has finished.
   * 
   * The response to this message is a
   * [[org.apache.flink.runtime.client.SerializedJobExecutionResult]]
   *
   * @param jobGraph The job to be executed.
   */
  case class SubmitJobAndWait(jobGraph: JobGraph)

  /**
   * This message is sent to the JobClient (via ask) to submit a job and 
   * return as soon as the result of the submit operation is known. 
   *
   * The response to this message is a
   * [[org.apache.flink.api.common.JobSubmissionResult]]
   *
   * @param jobGraph The job to be executed.
   */
  case class SubmitJobDetached(jobGraph: JobGraph)
}
