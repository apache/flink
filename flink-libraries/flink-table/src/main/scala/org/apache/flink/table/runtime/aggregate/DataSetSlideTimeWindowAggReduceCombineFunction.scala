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
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.functions.{Accumulator, AggregateFunction}
import org.apache.flink.types.Row

/**
  * Wraps the aggregate logic inside of
  * [[org.apache.flink.api.java.operators.GroupReduceOperator]] and
  * [[org.apache.flink.api.java.operators.GroupCombineOperator]].
  *
  * It is used for sliding on batch for time-windows.
  *
  * @param aggregates aggregate functions
  * @param groupingKeysLength number of grouping keys
  * @param timeFieldPos position of aligned time field
  * @param windowSize window size of the sliding window
  * @param windowSlide window slide of the sliding window
  * @param returnType return type of this function
  */
class DataSetSlideTimeWindowAggReduceCombineFunction(
    aggregates: Array[AggregateFunction[_ <: Any]],
    groupingKeysLength: Int,
    timeFieldPos: Int,
    windowSize: Long,
    windowSlide: Long,
    returnType: TypeInformation[Row])
  extends DataSetSlideTimeWindowAggReduceGroupFunction(
    aggregates,
    groupingKeysLength,
    timeFieldPos,
    windowSize,
    windowSlide,
    returnType)
  with CombineFunction[Row, Row] {

  override def combine(records: Iterable[Row]): Row = {

    // reset first accumulator
    var i = 0
    while (i < aggregates.length) {
      val accumulator = aggregates(i).createAccumulator()
      accumulatorList(i).set(0, accumulator)
      i += 1
    }

    val iterator = records.iterator()
    while (iterator.hasNext) {
      val record = iterator.next()

      i = 0
      while (i < aggregates.length) {
        // insert received accumulator into acc list
        val newAcc = record.getField(groupingKeysLength + i).asInstanceOf[Accumulator]
        accumulatorList(i).set(1, newAcc)
        // merge acc list
        val retAcc = aggregates(i).merge(accumulatorList(i))
        // insert result into acc list
        accumulatorList(i).set(0, retAcc)
        i += 1
      }

      // check if this record is the last record
      if (!iterator.hasNext) {

        // set group keys
        i = 0
        while (i < groupingKeysLength) {
          intermediateRow.setField(i, record.getField(i))
          i += 1
        }

        // set accumulators
        i = 0
        while (i < aggregates.length) {
          intermediateRow.setField(groupingKeysLength + i, accumulatorList(i).get(0))
          i += 1
        }

        intermediateRow.setField(timeFieldPos, record.getField(timeFieldPos))

        return intermediateRow
      }
    }

    // this code path should never be reached as we return before the loop finishes
    // we need this to prevent a compiler error
    throw new IllegalArgumentException("Group is empty. This should never happen.")
  }
}
