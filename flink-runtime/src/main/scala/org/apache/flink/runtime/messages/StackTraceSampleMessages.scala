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

import scala.concurrent.duration.FiniteDuration

/**
  * A set of messages exchanged between the job manager and task manager
  * instances in order to sample the stack traces of running tasks.
  */
object StackTraceSampleMessages {

  trait StackTraceSampleMessages extends RequiresLeaderSessionID {}

  /**
    * Triggers the sampling of a running task (sent by the job manager to the
    * task managers).
    *
    * @param sampleId ID of this sample.
    * @param jobId ID of the job the sample belongs to.
    * @param executionId ID of the task to sample.
    * @param numSamples Number of stack trace samples to collect.
    * @param delayBetweenSamples Delay between consecutive samples.
    * @param maxStackTraceDepth Maximum depth of the stack trace. 0 indicates
    *                           no maximum and collects the complete stack
    *                           trace.
    */
  case class TriggerStackTraceSample(
      sampleId: Int,
      jobId: JobID,
      executionId: ExecutionAttemptID,
      numSamples: Int,
      delayBetweenSamples: FiniteDuration,
      maxStackTraceDepth: Int = 0)
    extends StackTraceSampleMessages with java.io.Serializable

  /**
    * Response after a successful stack trace sample (sent by the task managers
    * to the job manager).
    *
    * @param sampleId ID of the this sample.
    * @param jobId ID of the job the sample belongs to.
    * @param executionId ID of the sampled task.
    * @param samples Stack trace samples (head is most recent sample).
    */
  case class ResponseStackTraceSampleSuccess(
      sampleId: Int,
      jobId: JobID,
      executionId: ExecutionAttemptID,
      samples: java.util.List[Array[StackTraceElement]])
    extends StackTraceSampleMessages

  /**
    * Response after a failed stack trace sample (sent by the task managers to
    * the job manager).
    *
    * @param sampleId ID of the this sample.
    * @param jobId ID of the job the sample belongs to.
    * @param executionId ID of the sampled task.
    * @param cause Failure cause.
    */
  case class ResponseStackTraceSampleFailure(
      sampleId: Int,
      jobId: JobID,
      executionId: ExecutionAttemptID,
      cause: Exception)
    extends StackTraceSampleMessages

  /**
    *
    * @param sampleId ID of the this sample.
    * @param jobId ID of the job the sample belongs to.
    * @param executionId ID of the task to sample.
    * @param delayBetweenSamples Delay between consecutive samples.
    * @param maxStackTraceDepth Maximum depth of the stack trace. 0 indicates
    *                           no maximum and collects the complete stack
    *                           trace.
    * @param numRemainingSamples Number of remaining samples before this
    *                            sample is finished.
    * @param currentTraces The current list of gathered stack traces.
    */
  case class SampleTaskStackTrace(
      sampleId: Int,
      jobId: JobID,
      executionId: ExecutionAttemptID,
      delayBetweenSamples: FiniteDuration,
      maxStackTraceDepth: Int,
      numRemainingSamples: Int,
      currentTraces: java.util.List[Array[StackTraceElement]])
    extends StackTraceSampleMessages

}
