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
import org.apache.flink.table.functions.{Accumulator, AggregateFunction}
import org.apache.flink.types.Row

/**
  * It wraps the aggregate logic inside of
  * [[org.apache.flink.api.java.operators.GroupReduceOperator]] and
  * [[org.apache.flink.api.java.operators.GroupCombineOperator]].
  * It is used for tumbling time-window on batch.
  *
  * @param windowSize       Tumbling time window size
  * @param windowStartPos   The relative window-start field position to the last field of output row
  * @param windowEndPos     The relative window-end field position to the last field of output row
  * @param aggregates       The aggregate functions.
  * @param groupKeysMapping The index mapping of group keys between intermediate aggregate Row
  *                         and output Row.
  * @param aggregateMapping The index mapping between aggregate function list and aggregated value
  *                         index in output Row.
  * @param finalRowArity    The output row field count
  */
class DataSetTumbleTimeWindowAggReduceCombineFunction(
    windowSize: Long,
    windowStartPos: Option[Int],
    windowEndPos: Option[Int],
    aggregates: Array[AggregateFunction[_ <: Any]],
    groupKeysMapping: Array[(Int, Int)],
    aggregateMapping: Array[(Int, Int)],
    finalRowArity: Int)
  extends DataSetTumbleTimeWindowAggReduceGroupFunction(
    windowSize,
    windowStartPos,
    windowEndPos,
    aggregates,
    groupKeysMapping,
    aggregateMapping,
    finalRowArity)
    with CombineFunction[Row, Row] {

  /**
    * For sub-grouped intermediate aggregate Rows, merge all of them into aggregate buffer,
    *
    * @param records Sub-grouped intermediate aggregate Rows iterator.
    * @return Combined intermediate aggregate Row.
    *
    */
  override def combine(records: Iterable[Row]): Row = {

    var last: Row = null
    val iterator = records.iterator()

    // reset first accumulator in merge list
    var i = 0
    while (i < aggregates.length) {
      aggregates(i).resetAccumulator(accumulatorList(i).get(0))
      i += 1
    }

    while (iterator.hasNext) {
      val record = iterator.next()

      i = 0
      while (i < aggregates.length) {
        // insert received accumulator into acc list
        val newAcc = record.getField(groupKeysMapping.length + i).asInstanceOf[Accumulator]
        accumulatorList(i).set(1, newAcc)
        // merge acc list
        val retAcc = aggregates(i).merge(accumulatorList(i))
        // insert result into acc list
        accumulatorList(i).set(0, retAcc)
        i += 1
      }

      last = record
    }

    // set the partial merged result to the aggregateBuffer
    i = 0
    while (i < aggregates.length) {
      aggregateBuffer.setField(groupKeysMapping.length + i, accumulatorList(i).get(0))
      i += 1
    }

    // set group keys to aggregateBuffer.
    i = 0
    while (i < groupKeysMapping.length) {
      aggregateBuffer.setField(i, last.getField(i))
      i += 1
    }

    // set the rowtime attribute
    val rowtimePos = groupKeysMapping.length + aggregates.length

    aggregateBuffer.setField(rowtimePos, last.getField(rowtimePos))

    aggregateBuffer
  }

}
