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
  * [[org.apache.flink.api.java.operators.GroupReduceOperator]].
  *
  * It is used for sliding on batch for both time and count-windows.
  *
  * @param genAggregations Code-generated [[GeneratedAggregations]]
  * @param keysAndAggregatesArity The total arity of keys and aggregates
  * @param finalRowWindowStartPos relative window-start position to last field of output row
  * @param finalRowWindowEndPos relative window-end position to last field of output row
  * @param finalRowWindowRowtimePos relative window-rowtime position to the last field of the
  *                                 output row
  * @param windowSize size of the window, used to determine window-end for output row
  */
class DataSetSlideWindowAggReduceGroupFunction(
    genAggregations: GeneratedAggregationsFunction,
    keysAndAggregatesArity: Int,
    finalRowWindowStartPos: Option[Int],
    finalRowWindowEndPos: Option[Int],
    finalRowWindowRowtimePos: Option[Int],
    windowSize: Long)
  extends RichGroupReduceFunction[Row, Row]
    with Compiler[GeneratedAggregations]
    with Logging {

  private var collector: DataSetTimeWindowPropertyCollector = _
  protected val windowStartPos: Int = keysAndAggregatesArity

  private var output: Row = _
  protected var accumulators: Row = _

  protected var function: GeneratedAggregations = _

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

  override def reduce(records: Iterable[Row], out: Collector[Row]): Unit = {

    // reset accumulator
    function.resetAccumulator(accumulators)

    val iterator = records.iterator()
    var record: Row = null
    while (iterator.hasNext) {
      record = iterator.next()
      function.mergeAccumulatorsPair(accumulators, record)
    }

    // set group keys value to final output
    function.setForwardedFields(record, output)

    // get final aggregate value and set to output
    function.setAggregationResults(accumulators, output)

    // adds TimeWindow properties to output then emit output
    if (finalRowWindowStartPos.isDefined ||
        finalRowWindowEndPos.isDefined ||
        finalRowWindowRowtimePos.isDefined) {

      collector.wrappedCollector = out
      collector.windowStart = record.getField(windowStartPos).asInstanceOf[Long]
      collector.windowEnd = collector.windowStart + windowSize

      collector.collect(output)
    } else {
      out.collect(output)
    }
  }
}
