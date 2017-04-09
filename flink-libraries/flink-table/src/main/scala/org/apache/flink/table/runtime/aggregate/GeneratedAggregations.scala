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

package org.apache.flink.table.runtime.aggregate

import org.apache.flink.api.common.functions.Function
import org.apache.flink.types.Row

/**
  * Base class for code-generated aggregations.
  */
abstract class GeneratedAggregations extends Function {

  /**
    * Calculate the results from accumulators, and set the results to the output
    *
    * @param accumulators the accumulators (saved in a row) which contains the current
    *                     aggregated results
    * @param output       output results collected in a row
    */
  def setAggregationResults(accumulators: Row, output: Row)

  /**
    * Copies forwarded fields from input row to output row.
    *
    * @param input  input values bundled in a row
    * @param output output results collected in a row
    */
  def setForwardedFields(input: Row, output: Row)

  /**
    * Accumulate the input values to the accumulators
    *
    * @param accumulators the accumulators (saved in a row) which contains the current
    *                     aggregated results
    * @param input        input values bundled in a row
    */
  def accumulate(accumulators: Row, input: Row)

  /**
    * Retract the input values from the accumulators
    *
    * @param accumulators the accumulators (saved in a row) which contains the current
    *                     aggregated results
    * @param input        input values bundled in a row
    */
  def retract(accumulators: Row, input: Row)

  /**
    * Init the accumulators, and save them to a accumulators Row.
    *
    * @return a row of accumulators which contains the aggregated results
    */
  def createAccumulators(): Row

  /**
    * Creates an output row object with the correct arity.
    *
    * @return an output row object with the correct arity.
    */
  def createOutputRow(): Row

}
