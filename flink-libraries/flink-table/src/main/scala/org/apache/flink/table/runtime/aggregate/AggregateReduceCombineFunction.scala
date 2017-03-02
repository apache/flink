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
import java.util.{ArrayList => JArrayList}

import org.apache.flink.api.common.functions.CombineFunction
import org.apache.flink.table.functions.{Accumulator, AggregateFunction}
import org.apache.flink.types.Row

/**
  * It wraps the aggregate logic inside of
  * [[org.apache.flink.api.java.operators.GroupReduceOperator]] and
  * [[org.apache.flink.api.java.operators.GroupCombineOperator]]
  *
  * @param aggregates          The aggregate functions.
  * @param groupKeysMapping    The index mapping of group keys between intermediate aggregate Row
  *                            and output Row.
  * @param aggregateMapping    The index mapping between aggregate function list and aggregated
  *                            value
  *                            index in output Row.
  * @param groupingSetsMapping The index mapping of keys in grouping sets between intermediate
  *                            Row and output Row.
  * @param finalRowArity       the arity of the final resulting row
  */
class AggregateReduceCombineFunction(
    private val aggregates: Array[AggregateFunction[_ <: Any]],
    private val groupKeysMapping: Array[(Int, Int)],
    private val aggregateMapping: Array[(Int, Int)],
    private val groupingSetsMapping: Array[(Int, Int)],
    private val finalRowArity: Int)
  extends AggregateReduceGroupFunction(
    aggregates,
    groupKeysMapping,
    aggregateMapping,
    groupingSetsMapping,
    finalRowArity) with CombineFunction[Row, Row] {

  /**
    * For sub-grouped intermediate aggregate Rows, merge all of them into aggregate buffer,
    *
    * @param records Sub-grouped intermediate aggregate Rows iterator.
    * @return Combined intermediate aggregate Row.
    *
    */
  override def combine(records: Iterable[Row]): Row = {

    // merge intermediate aggregate value to buffer.
    var last: Row = null
    accumulatorList.foreach(_.clear())

    val iterator = records.iterator()

    var count: Int = 0
    while (iterator.hasNext) {
      val record = iterator.next()
      count += 1
      // per each aggregator, collect its accumulators to a list
      for (i <- aggregates.indices) {
        accumulatorList(i).add(record.getField(groupKeysMapping.length + i)
                                 .asInstanceOf[Accumulator])
      }
      // if the number of buffered accumulators is bigger than maxMergeLen, merge them into one
      // accumulator
      if (count > maxMergeLen) {
        count = 0
        for (i <- aggregates.indices) {
          val agg = aggregates(i)
          val accumulator = agg.merge(accumulatorList(i))
          accumulatorList(i).clear()
          accumulatorList(i).add(accumulator)
        }
      }
      last = record
    }

    for (i <- aggregates.indices) {
      val agg = aggregates(i)
      aggregateBuffer.setField(groupKeysMapping.length + i, agg.merge(accumulatorList(i)))
    }

    // set group keys to aggregateBuffer.
    for (i <- groupKeysMapping.indices) {
      aggregateBuffer.setField(i, last.getField(i))
    }

    aggregateBuffer
  }
}
