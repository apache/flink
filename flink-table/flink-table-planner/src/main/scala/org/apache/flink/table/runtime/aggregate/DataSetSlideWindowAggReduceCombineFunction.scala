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
import org.apache.flink.configuration.Configuration
import org.apache.flink.table.codegen.GeneratedAggregationsFunction
import org.apache.flink.types.Row

/**
  * Wraps the aggregate logic inside of
  * [[org.apache.flink.api.java.operators.GroupReduceOperator]] and
  * [[org.apache.flink.api.java.operators.GroupCombineOperator]].
  *
  * It is used for sliding on batch for both time and count-windows.
  *
  * @param genPreAggregations Code-generated [[GeneratedAggregations]] for partial aggregation.
  * @param genFinalAggregations Code-generated [[GeneratedAggregations]] for final aggregation.
  * @param keysAndAggregatesArity The total arity of keys and aggregates
  * @param finalRowWindowStartPos relative window-start position to last field of output row
  * @param finalRowWindowEndPos relative window-end position to last field of output row
  * @param finalRowWindowRowtimePos relative window-rowtime position to the last field of the
  *                                 output row
  * @param windowSize size of the window, used to determine window-end for output row
  */
class DataSetSlideWindowAggReduceCombineFunction(
    genPreAggregations: GeneratedAggregationsFunction,
    genFinalAggregations: GeneratedAggregationsFunction,
    keysAndAggregatesArity: Int,
    finalRowWindowStartPos: Option[Int],
    finalRowWindowEndPos: Option[Int],
    finalRowWindowRowtimePos: Option[Int],
    windowSize: Long)
  extends DataSetSlideWindowAggReduceGroupFunction(
    genFinalAggregations,
    keysAndAggregatesArity,
    finalRowWindowStartPos,
    finalRowWindowEndPos,
    finalRowWindowRowtimePos,
    windowSize)
  with CombineFunction[Row, Row] {

  private val intermediateRow: Row = new Row(keysAndAggregatesArity + 1)

  protected var preAggfunction: GeneratedAggregations = _

  override def open(config: Configuration): Unit = {
    super.open(config)

    LOG.debug(s"Compiling AggregateHelper: $genPreAggregations.name \n\n " +
      s"Code:\n$genPreAggregations.code")
    val clazz = compile(
      getRuntimeContext.getUserCodeClassLoader,
      genPreAggregations.name,
      genPreAggregations.code)
    LOG.debug("Instantiating AggregateHelper.")
    preAggfunction = clazz.newInstance()
  }

  override def combine(records: Iterable[Row]): Row = {

    // reset accumulator
    preAggfunction.resetAccumulator(accumulators)

    val iterator = records.iterator()
    var record: Row = null
    while (iterator.hasNext) {
      record = iterator.next()
      preAggfunction.mergeAccumulatorsPair(accumulators, record)
    }
    // set group keys and partial accumulated result
    preAggfunction.setAggregationResults(accumulators, intermediateRow)
    preAggfunction.setForwardedFields(record, intermediateRow)

    intermediateRow.setField(windowStartPos, record.getField(windowStartPos))

    intermediateRow
  }
}
