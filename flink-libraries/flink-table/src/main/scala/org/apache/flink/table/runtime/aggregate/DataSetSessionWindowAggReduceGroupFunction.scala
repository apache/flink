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

import org.apache.flink.api.common.functions.RichGroupReduceFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.table.codegen.{Compiler, GeneratedAggregationsFunction}
import org.apache.flink.table.util.Logging
import org.apache.flink.types.Row
import org.apache.flink.util.Collector

/**
  * It wraps the aggregate logic inside of
  * [[org.apache.flink.api.java.operators.GroupReduceOperator]]. It is used for Session time-window
  * on batch.
  *
  * Note:
  *
  * This can handle two input types (depending if input is combined or not):
  *
  *  1. when partial aggregate is not supported, the input data structure of reduce is
  * |groupKey1|groupKey2|sum1|count1|sum2|count2|rowTime|
  *  2. when partial aggregate is supported, the input data structure of reduce is
  * |groupKey1|groupKey2|sum1|count1|sum2|count2|windowStart|windowEnd|
  *
  * @param genAggregations Code-generated [[GeneratedAggregations]]
  * @param keysAndAggregatesArity    The total arity of keys and aggregates
  * @param finalRowWindowStartPos The relative window-start field position.
  * @param finalRowWindowEndPos   The relative window-end field position.
  * @param finalRowWindowRowtimePos The relative window-rowtime field position.
  * @param gap                    Session time window gap.
  */
class DataSetSessionWindowAggReduceGroupFunction(
    genAggregations: GeneratedAggregationsFunction,
    keysAndAggregatesArity: Int,
    finalRowWindowStartPos: Option[Int],
    finalRowWindowEndPos: Option[Int],
    finalRowWindowRowtimePos: Option[Int],
    gap: Long,
    isInputCombined: Boolean)
  extends RichGroupReduceFunction[Row, Row]
    with Compiler[GeneratedAggregations]
    with Logging {

  private var collector: DataSetTimeWindowPropertyCollector = _
  private val intermediateRowWindowStartPos = keysAndAggregatesArity
  private val intermediateRowWindowEndPos = keysAndAggregatesArity + 1

  private var output: Row = _
  private var accumulators: Row = _

  private var function: GeneratedAggregations = _

  override def open(config: Configuration) {
    LOG.debug(s"Compiling AggregateHelper: $genAggregations.name \n\n " +
                s"Code:\n$genAggregations.code")
    val clazz = compile(
      getRuntimeContext.getUserCodeClassLoader,
      genAggregations.name,
      genAggregations.code)
    LOG.debug("Instantiating AggregateHelper.")
    function = clazz.newInstance()

    output = function.createOutputRow()
    accumulators = function.createAccumulators()
    collector = new DataSetTimeWindowPropertyCollector(
      finalRowWindowStartPos,
      finalRowWindowEndPos,
      finalRowWindowRowtimePos)
  }

  /**
    * For grouped intermediate aggregate Rows, divide window according to the window-start
    * and window-end, merge data (within a unified window) into an aggregate buffer, calculate
    * aggregated values output from aggregate buffer, and then set them into output
    * Row based on the mapping relationship between intermediate aggregate data and output data.
    *
    * @param records Grouped intermediate aggregate Rows iterator.
    * @param out     The collector to hand results to.
    *
    */
  override def reduce(records: Iterable[Row], out: Collector[Row]): Unit = {

    var windowStart: java.lang.Long = null
    var windowEnd: java.lang.Long = null
    var currentRowTime: java.lang.Long = null

    // reset accumulator
    function.resetAccumulator(accumulators)

    val iterator = records.iterator()

    while (iterator.hasNext) {
      val record = iterator.next()
      currentRowTime = record.getField(intermediateRowWindowStartPos).asInstanceOf[Long]
      // initial traversal or opening a new window
      if (null == windowEnd ||
        (null != windowEnd && currentRowTime > windowEnd)) {

        // calculate the current window and open a new window
        if (null != windowEnd) {
          // evaluate and emit the current window's result.
          doEvaluateAndCollect(out, windowStart, windowEnd)
          // reset accumulator
          function.resetAccumulator(accumulators)
        } else {
          // set keys to output
          function.setForwardedFields(record, output)
        }

        windowStart = record.getField(intermediateRowWindowStartPos).asInstanceOf[Long]
      }

      function.mergeAccumulatorsPair(accumulators, record)

      windowEnd = if (isInputCombined) {
        // partial aggregate is supported
        record.getField(intermediateRowWindowEndPos).asInstanceOf[Long]
      } else {
        // partial aggregate is not supported, window-start equal rowtime + gap
        currentRowTime + gap
      }
    }
    // evaluate and emit the current window's result.
    doEvaluateAndCollect(out, windowStart, windowEnd)
  }

  /**
    * Evaluate and emit the data of the current window.
    *
    * @param out             the collection of the aggregate results
    * @param windowStart     the window's start attribute value is the min (rowtime) of all rows
    *                        in the window.
    * @param windowEnd       the window's end property value is max (rowtime) + gap for all rows
    *                        in the window.
    */
  def doEvaluateAndCollect(
      out: Collector[Row],
      windowStart: Long,
      windowEnd: Long): Unit = {

    // set value for the final output
    function.setAggregationResults(accumulators, output)

    // adds TimeWindow properties to output then emit output
    if (finalRowWindowStartPos.isDefined || finalRowWindowEndPos.isDefined) {
      collector.wrappedCollector = out
      collector.windowStart = windowStart
      collector.windowEnd = windowEnd

      collector.collect(output)
    } else {
      out.collect(output)
    }
  }

}
