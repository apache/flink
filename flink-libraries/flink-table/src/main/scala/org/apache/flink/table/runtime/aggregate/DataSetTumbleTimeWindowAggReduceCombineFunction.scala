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

/**
  * It wraps the aggregate logic inside of
  * [[org.apache.flink.api.java.operators.GroupReduceOperator]] and
  * [[org.apache.flink.api.java.operators.GroupCombineOperator]].
  * It is used for tumbling time-window on batch.
  *
  * @param rowtimePos The rowtime field index in input row
  * @param windowSize Tumbling time window size
  * @param windowStartPos The relative window-start field position to the last field of output row
  * @param windowEndPos The relative window-end field position to the last field of output row
  * @param aggregates The aggregate functions.
  * @param groupKeysMapping The index mapping of group keys between intermediate aggregate Row
  *                         and output Row.
  * @param aggregateMapping The index mapping between aggregate function list and aggregated value
  *                         index in output Row.
  * @param intermediateRowArity The intermediate row field count
  * @param finalRowArity The output row field count
  */
class DataSetTumbleTimeWindowAggReduceCombineFunction(
    rowtimePos: Int,
    windowSize: Long,
    windowStartPos: Option[Int],
    windowEndPos: Option[Int],
    aggregates: Array[Aggregate[_ <: Any]],
    groupKeysMapping: Array[(Int, Int)],
    aggregateMapping: Array[(Int, Int)],
    intermediateRowArity: Int,
    finalRowArity: Int)
  extends DataSetTumbleTimeWindowAggReduceGroupFunction(
    rowtimePos,
    windowSize,
    windowStartPos,
    windowEndPos,
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

    // initiate intermediate aggregate value.
    aggregates.foreach(_.initiate(aggregateBuffer))

    // merge intermediate aggregate value to buffer.
    var last: Row = null

    val iterator = records.iterator()
    while (iterator.hasNext) {
      val record = iterator.next()
      aggregates.foreach(_.merge(record, aggregateBuffer))
      last = record
    }

    // set group keys to aggregateBuffer.
    for (i <- groupKeysMapping.indices) {
      aggregateBuffer.setField(i, last.getField(i))
    }

    // set the rowtime attribute
    aggregateBuffer.setField(rowtimePos, last.getField(rowtimePos))

    aggregateBuffer
  }

}
