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
    * Sets the results of the aggregations (partial or final) to the output row.
    * Final results are computed with the aggregation function.
    * Partial results are the accumulators themselves.
    *
    * @param accumulators the accumulators (saved in a row) which contains the current
    *                     aggregated results
    * @param output       output results collected in a row
    */
  def setAggregationResults(accumulators: Row, output: Row)

  /**
    * Copies forwarded fields, such as grouping keys, from input row to output row.
    *
    * @param input        input values bundled in a row
    * @param output       output results collected in a row
    */
  def setForwardedFields(input: Row, output: Row)

  /**
    * Sets constant flags (boolean fields) to an output row.
    *
    * @param output The output row to which the constant flags are set.
    */
  def setConstantFlags(output: Row)

  /**
    * Accumulates the input values to the accumulators.
    *
    * @param accumulators the accumulators (saved in a row) which contains the current
    *                     aggregated results
    * @param input        input values bundled in a row
    */
  def accumulate(accumulators: Row, input: Row)

  /**
    * Retracts the input values from the accumulators.
    *
    * @param accumulators the accumulators (saved in a row) which contains the current
    *                     aggregated results
    * @param input        input values bundled in a row
    */
  def retract(accumulators: Row, input: Row)

  /**
    * Initializes the accumulators and save them to a accumulators row.
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

  /**
    * Merges two rows of accumulators into one row.
    *
    * @param a First row of accumulators
    * @param b The other row of accumulators
    * @return A row with the merged accumulators of both input rows.
    */
  def mergeAccumulatorsPair(a: Row, b: Row): Row

  /**
    * Resets all the accumulators.
    *
    * @param accumulators the accumulators (saved in a row) which contains the current
    *                     aggregated results
    */
  def resetAccumulator(accumulators: Row)
}

class SingleElementIterable[T] extends java.lang.Iterable[T] {

  class SingleElementIterator extends java.util.Iterator[T] {

    var element: T = _
    var newElement: Boolean = false

    override def hasNext: Boolean = newElement

    override def next(): T = {
      if (newElement) {
        newElement = false
        element
      } else {
        throw new java.util.NoSuchElementException
      }
    }

    override def remove(): Unit = new java.lang.UnsupportedOperationException
  }

  val it = new SingleElementIterator

  def setElement(element: T): Unit = it.element = element

  override def iterator(): java.util.Iterator[T] = {
    it.newElement = true
    it
  }
}

