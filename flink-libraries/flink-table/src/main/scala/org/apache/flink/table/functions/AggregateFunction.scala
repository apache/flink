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

import java.util.{List => JList}

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.api.TableException

/**
  * Base class for User-Defined Aggregates.
  *
  * @tparam T the type of the aggregation result
  */
abstract class AggregateFunction[T] extends UserDefinedFunction {
  /**
    * Creates and init the Accumulator for this [[AggregateFunction]].
    *
    * @return the accumulator with the initial value
    */
  def createAccumulator(): Accumulator

  /**
    * Retracts the input values from the accumulator instance. The current design assumes the
    * inputs are the values that have been previously accumulated.
    *
    * @param accumulator the accumulator which contains the current
    *                    aggregated results
    * @param input       the input value (usually obtained from a new arrived data)
    */
  def retract(accumulator: Accumulator, input: Any): Unit = {
    throw TableException("Retract is an optional method. There is no default implementation. You " +
                           "must implement one for yourself.")
  }

  /**
    * Called every time when an aggregation result should be materialized.
    * The returned value could be either an early and incomplete result
    * (periodically emitted as data arrive) or the final result of the
    * aggregation.
    *
    * @param accumulator the accumulator which contains the current
    *                    aggregated results
    * @return the aggregation result
    */
  def getValue(accumulator: Accumulator): T

  /**
    * Processes the input values and update the provided accumulator instance.
    *
    * @param accumulator the accumulator which contains the current
    *                    aggregated results
    * @param input       the input value (usually obtained from a new arrived data)
    */
  def accumulate(accumulator: Accumulator, input: Any): Unit

  /**
    * Merges a list of accumulator instances into one accumulator instance.
    *
    * IMPORTANT: You may only return a new accumulator instance or the first accumulator of the
    * input list. If you return another instance, the result of the aggregation function might be
    * incorrect.
    *
    * @param accumulators the [[java.util.List]] of accumulators that will be merged
    * @return the resulting accumulator
    */
  def merge(accumulators: JList[Accumulator]): Accumulator

  /**
    * Resets the Accumulator for this [[AggregateFunction]].
    *
    * @param accumulator the accumulator which needs to be reset
    */
  def resetAccumulator(accumulator: Accumulator): Unit

  /**
    * Returns the [[TypeInformation]] of the accumulator.
    * This function is optional and can be implemented if the accumulator type cannot automatically
    * inferred from the instance returned by [[createAccumulator()]].
    *
    * @return The type information for the accumulator.
    */
  def getAccumulatorType: TypeInformation[_] = null
}

/**
  * Base class for aggregate Accumulator. The accumulator is used to keep the
  * aggregated values which are needed to compute an aggregation result.
  * The state of the function must be put into the accumulator.
  *
  * TODO: We have the plan to have the accumulator and return types of
  * functions dynamically provided by the users. This needs the refactoring
  * of the AggregateFunction interface with the code generation. We will remove
  * the [[Accumulator]] once codeGen for UDAGG is completed (FLINK-5813).
  */
trait Accumulator
