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
  * [[org.apache.flink.api.java.operators.GroupReduceOperator]]. It is used for tumbling time-window
  * on batch.
  *
  * @param genAggregations  Code-generated [[GeneratedAggregations]]
  * @param windowSize       Tumbling time window size
  * @param windowStartPos   The relative window-start field position to the last field of output row
  * @param windowEndPos     The relative window-end field position to the last field of output row
  * @param windowRowtimePos The relative window-rowtime field position to the last field of
  *                         output row
  * @param keysAndAggregatesArity    The total arity of keys and aggregates
  */
class DataSetTumbleTimeWindowAggReduceGroupFunction(
    genAggregations: GeneratedAggregationsFunction,
    windowSize: Long,
    windowStartPos: Option[Int],
    windowEndPos: Option[Int],
    windowRowtimePos: Option[Int],
    keysAndAggregatesArity: Int)
  extends RichGroupReduceFunction[Row, Row]
    with Compiler[GeneratedAggregations]
    with Logging {

  private var collector: DataSetTimeWindowPropertyCollector = _
  protected var aggregateBuffer: Row = new Row(keysAndAggregatesArity + 1)

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
      windowStartPos,
      windowEndPos,
      windowRowtimePos)
  }

  override def reduce(records: Iterable[Row], out: Collector[Row]): Unit = {

    var last: Row = null
    val iterator = records.iterator()

    // reset accumulator
    function.resetAccumulator(accumulators)

    while (iterator.hasNext) {
      val record = iterator.next()
      function.mergeAccumulatorsPair(accumulators, record)
      last = record
    }

    // set group keys value to final output.
    function.setForwardedFields(last, output)

    // get final aggregate value and set to output.
    function.setAggregationResults(accumulators, output)

    // get window start timestamp
    val startTs: Long = last.getField(keysAndAggregatesArity).asInstanceOf[Long]

    // set collector and window
    collector.wrappedCollector = out
    collector.windowStart = startTs
    collector.windowEnd = startTs + windowSize

    collector.collect(output)
  }

}
