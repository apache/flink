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
  * It wraps the aggregate logic inside of
  * [[org.apache.flink.api.java.operators.GroupReduceOperator]] and
  * [[org.apache.flink.api.java.operators.GroupCombineOperator]].
  * It is used for tumbling time-window on batch.
  *
  * @param genPreAggregations        Code-generated [[GeneratedAggregations]] for partial aggs.
  * @param genFinalAggregations        Code-generated [[GeneratedAggregations]] for final aggs.
  * @param windowSize             Tumbling time window size
  * @param windowStartPos         The relative window-start field position to the last field of
  *                               output row
  * @param windowEndPos           The relative window-end field position to the last field of
  *                               output row
  * @param windowRowtimePos       The relative window-rowtime field position to the last field of
  *                               output row
  * @param keysAndAggregatesArity The total arity of keys and aggregates
  */
class DataSetTumbleTimeWindowAggReduceCombineFunction(
    genPreAggregations: GeneratedAggregationsFunction,
    genFinalAggregations: GeneratedAggregationsFunction,
    windowSize: Long,
    windowStartPos: Option[Int],
    windowEndPos: Option[Int],
    windowRowtimePos: Option[Int],
    keysAndAggregatesArity: Int)
  extends DataSetTumbleTimeWindowAggReduceGroupFunction(
    genFinalAggregations,
    windowSize,
    windowStartPos,
    windowEndPos,
    windowRowtimePos,
    keysAndAggregatesArity)
    with CombineFunction[Row, Row] {

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

    // reset accumulator
    preAggfunction.resetAccumulator(accumulators)

    while (iterator.hasNext) {
      val record = iterator.next()
      preAggfunction.mergeAccumulatorsPair(accumulators, record)
      last = record
    }

    // set group keys and partial merged result to aggregateBuffer
    preAggfunction.setAggregationResults(accumulators, aggregateBuffer)
    preAggfunction.setForwardedFields(last, aggregateBuffer)

    // set the rowtime attribute
    val rowtimePos = keysAndAggregatesArity

    aggregateBuffer.setField(rowtimePos, last.getField(rowtimePos))

    aggregateBuffer
  }

}
