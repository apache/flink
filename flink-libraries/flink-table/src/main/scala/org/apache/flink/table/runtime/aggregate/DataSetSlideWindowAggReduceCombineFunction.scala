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
import org.apache.flink.table.codegen.GeneratedAggregationsFunction
import org.apache.flink.types.Row

/**
  * Wraps the aggregate logic inside of
  * [[org.apache.flink.api.java.operators.GroupReduceOperator]] and
  * [[org.apache.flink.api.java.operators.GroupCombineOperator]].
  *
  * It is used for sliding on batch for both time and count-windows.
  *
  * @param genAggregations Code-generated [[GeneratedAggregations]]
  * @param keysAndAggregatesArity The total arity of keys and aggregates
  * @param finalRowWindowStartPos relative window-start position to last field of output row
  * @param finalRowWindowEndPos relative window-end position to last field of output row
  * @param windowSize size of the window, used to determine window-end for output row
  */
class DataSetSlideWindowAggReduceCombineFunction(
    genAggregations: GeneratedAggregationsFunction,
    keysAndAggregatesArity: Int,
    finalRowWindowStartPos: Option[Int],
    finalRowWindowEndPos: Option[Int],
    windowSize: Long)
  extends DataSetSlideWindowAggReduceGroupFunction(
    genAggregations,
    keysAndAggregatesArity,
    finalRowWindowStartPos,
    finalRowWindowEndPos,
    windowSize)
  with CombineFunction[Row, Row] {

  private val intermediateRow: Row = new Row(keysAndAggregatesArity + 1)

  override def combine(records: Iterable[Row]): Row = {

    // reset accumulator
    function.resetAccumulator(accumulators)

    val iterator = records.iterator()
    var record: Row = null
    while (iterator.hasNext) {
      record = iterator.next()
      function.mergeAccumulatorsPair(accumulators, record)
    }
    // set group keys and partial accumulated result
    function.setForwardedFields(record, accumulators, intermediateRow)

    intermediateRow.setField(windowStartPos, record.getField(windowStartPos))

    intermediateRow
  }
}
