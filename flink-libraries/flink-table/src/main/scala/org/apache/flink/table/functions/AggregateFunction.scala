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
package org.apache.flink.table.functions

/**
  * Base class for User-Defined Aggregates.
  *
  * @tparam T the type of the aggregation result
  */
abstract class AggregateFunction[T] extends UserDefinedFunction {
  /**
    * Create and init the Accumulator for this [[AggregateFunction]].
    *
    * @return the accumulator with the initial value
    */
  def createAccumulator(): Accumulator

  /**
    * Called every time when an aggregation result should be materialized.
    * The returned value could be either a speculative result (periodically
    * emitted as data arrive) or the final result of the aggregation (when
    * the state of the aggregation is completely removed).
    *
    * @param accumulator the accumulator which contains the current
    *         aggregated results
    * @return the aggregation result
    */
  def getValue(accumulator: Accumulator): T

  /**
    * Process the input values and update the provided accumulator instance.
    *
    * @param accumulator the accumulator which contains the current
    *         aggregated results
    * @param input the input value (usually obtained from a new arrived data)
    */
  def accumulate(accumulator: Accumulator, input: Any): Unit

  /**
    * Merge two accumulator instances into one accumulator instance.
    *
    * @param a one of the two accumulators
    * @param b the other accumulator
    * @return the resulting accumulator
    */
  def merge(a: Accumulator, b: Accumulator): Accumulator
}

/**
  * Base class for aggregate Accumulator. The accumulator is used to keep the
  * aggregated values which are needed to compute an aggregation result.
  *
  * TODO: We have the plan to have the accumulator and return types of
  * functions dynamically provided by the users. This needs the refactoring
  * of the AggregateFunction interface with the code generation. We will remove
  * the [[Accumulator]] once codeGen for UDAGG is completed (FLINK-5813).
  */
trait Accumulator
