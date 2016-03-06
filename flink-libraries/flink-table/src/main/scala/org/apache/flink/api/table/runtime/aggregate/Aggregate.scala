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
package org.apache.flink.api.table.runtime.aggregate

import org.apache.calcite.sql.`type`.SqlTypeName
import org.apache.flink.api.table.Row

/**
 * The interface for all Flink aggregate functions, which expressed in terms of initiate(),
 * prepare(), merge() and evaluate(). The aggregate functions would be executed in 2 phases:
 * -- In Map phase, use prepare() to transform aggregate field value into intermediate
 * aggregate value.
 * -- In GroupReduce phase, use merge() to merge grouped intermediate aggregate values
 * into aggregate buffer. Then use evaluate() to calculate the final aggregated value.
 * For associative decomposable aggregate functions, they support partial aggregate. To optimize
 * the performance, a Combine phase would be added between Map phase and GroupReduce phase,
 * -- In Combine phase, use merge() to merge sub-grouped intermediate aggregate values
 * into aggregate buffer.
 *
 * The intermediate aggregate value is stored inside Row, aggOffsetInRow is used as the start
 * field index in Row, so different aggregate functions could share the same Row as intermediate
 * aggregate value/aggregate buffer, as their aggregate values could be stored in distinct fields
 * of Row with no conflict. The intermediate aggregate value is required to be a sequence of JVM
 * primitives, and Flink use intermediateDataType() to get its data types in SQL side.
 *
 * @tparam T Aggregated value type.
 */
trait Aggregate[T] extends Serializable {

  /**
   * Initiate the intermediate aggregate value in Row.
   * @param intermediate
   */
  def initiate(intermediate: Row): Unit

  /**
   * Transform the aggregate field value into intermediate aggregate data.
   * @param value
   * @param intermediate
   */
  def prepare(value: Any, intermediate: Row): Unit

  /**
   * Merge intermediate aggregate data into aggregate buffer.
   * @param intermediate
   * @param buffer
   */
  def merge(intermediate: Row, buffer: Row): Unit

  /**
   * Calculate the final aggregated result based on aggregate buffer.
   * @param buffer
   * @return
   */
  def evaluate(buffer: Row): T

  /**
   * Intermediate aggregate value types.
   * @return
   */
  def intermediateDataType: Array[SqlTypeName]

  /**
   * Set the aggregate data offset in Row.
   * @param aggOffset
   */
  def setAggOffsetInRow(aggOffset: Int)

  /**
    * Whether aggregate function support partial aggregate.
   * @return
   */
  def supportPartial: Boolean = false
}
