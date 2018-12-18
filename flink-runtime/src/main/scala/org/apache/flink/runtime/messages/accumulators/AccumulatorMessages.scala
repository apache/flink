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

package org.apache.flink.runtime.messages.accumulators

import org.apache.flink.api.common.JobID
import org.apache.flink.runtime.accumulators.StringifiedAccumulatorResult
import org.apache.flink.util.{OptionalFailure, SerializedValue}

/**
 * Base trait of all accumulator messages
 */
sealed trait AccumulatorMessage {

  /** ID of the job that the accumulator belongs to */
  val jobID: JobID
}

/**
 * Base trait of responses to [[RequestAccumulatorResults]]
 */
sealed trait AccumulatorResultsResponse extends AccumulatorMessage

/**
 * Requests the accumulator results of the job identified by [[jobID]] from the job manager.
 * The result is sent back to the sender as a [[AccumulatorResultsResponse]] message.
 *
 * @param jobID Job Id of the job that the accumulator belongs to
 */
case class RequestAccumulatorResults(jobID: JobID)
  extends AccumulatorMessage

/**
 * Requests the accumulator results of the job as strings. This is used to request accumulators to
 * be displayed for example in the web frontend. Because it transports only
 * Strings, not custom objects, it is safe for user-defined types and class loading.
 *
 * @param jobID Job Id of the job that the accumulator belongs to
 */
case class RequestAccumulatorResultsStringified(jobID: JobID)
  extends AccumulatorMessage

/**
 * Contains the retrieved accumulator results from the job manager. This response is triggered
 * by [[RequestAccumulatorResults]].
 *
 * @param jobID Job Id of the job that the accumulator belongs to
 * @param result The accumulator result values, in serialized form.
 */
case class AccumulatorResultsFound(
    jobID: JobID,
    result: java.util.Map[String, SerializedValue[OptionalFailure[Object]]])
  extends AccumulatorResultsResponse

/**
 * Contains the retrieved accumulator result strings from the job manager.
 *
 * @param jobID Job Id of the job that the accumulator belongs to
 * @param result The accumulator result values, in string form.
 */
case class AccumulatorResultStringsFound(jobID: JobID,
                                         result: Array[StringifiedAccumulatorResult])
  extends AccumulatorResultsResponse

/**
 * Denotes that no accumulator results for [[jobID]] could be found at the job manager.
 *
 * @param jobID Job Id of the job that the accumulators were requested for.
 */
case class AccumulatorResultsNotFound(jobID: JobID)
  extends AccumulatorResultsResponse

/**
 * Denotes that the accumulator results for [[jobID]] could be obtained from
 * the JobManager because of an exception.
 *
 * @param jobID Job Id of the job that the accumulators were requested for.
 * @param cause Reason why the accumulators were not found.
 */
case class AccumulatorResultsErroneous(jobID: JobID, cause: Exception)
  extends AccumulatorResultsResponse
