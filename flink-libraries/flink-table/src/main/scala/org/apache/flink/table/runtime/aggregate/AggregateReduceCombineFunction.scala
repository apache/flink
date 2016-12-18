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

import java.lang.Iterable

import org.apache.flink.api.common.functions.CombineFunction
import org.apache.flink.types.Row

import scala.collection.JavaConversions._


/**
 * It wraps the aggregate logic inside of
 * [[org.apache.flink.api.java.operators.GroupReduceOperator]] and
 * [[org.apache.flink.api.java.operators.GroupCombineOperator]]
 *
 * @param aggregates   The aggregate functions.
 * @param groupKeysMapping The index mapping of group keys between intermediate aggregate Row
 *                         and output Row.
 * @param aggregateMapping The index mapping between aggregate function list and aggregated value
 *                         index in output Row.
 */
class AggregateReduceCombineFunction(
    private val aggregates: Array[Aggregate[_ <: Any]],
    private val groupKeysMapping: Array[(Int, Int)],
    private val aggregateMapping: Array[(Int, Int)],
    private val intermediateRowArity: Int,
    private val finalRowArity: Int)
  extends AggregateReduceGroupFunction(
    aggregates,
    groupKeysMapping,
    aggregateMapping,
    intermediateRowArity,
    finalRowArity)
  with CombineFunction[Row, Row] {

  /**
   * For sub-grouped intermediate aggregate Rows, merge all of them into aggregate buffer,
   *
   * @param records  Sub-grouped intermediate aggregate Rows iterator.
   * @return Combined intermediate aggregate Row.
   *
   */
  override def combine(records: Iterable[Row]): Row = {

    // Initiate intermediate aggregate value.
    aggregates.foreach(_.initiate(aggregateBuffer))

    // Merge intermediate aggregate value to buffer.
    var last: Row = null
    records.foreach((record) => {
      aggregates.foreach(_.merge(record, aggregateBuffer))
      last = record
    })

    // Set group keys to aggregateBuffer.
    for (i <- groupKeysMapping.indices) {
      aggregateBuffer.setField(i, last.getField(i))
    }

    aggregateBuffer
  }
}
