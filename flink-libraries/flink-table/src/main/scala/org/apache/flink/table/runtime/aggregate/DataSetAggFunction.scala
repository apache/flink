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
import org.apache.flink.types.Row
import org.apache.flink.util.{Collector, Preconditions}
import org.slf4j.LoggerFactory

/**
  * [[RichGroupReduceFunction]] to compute aggregates that do not support pre-aggregation for batch
  * (DataSet) queries.
  *
  * @param genAggregations Code-generated [[GeneratedAggregations]]
  * @param gkeyOutMapping The mapping of group keys between input and output positions.
  * @param groupingSetsMapping The mapping of grouping set keys between input and output positions.
  */
class DataSetAggFunction(
    private val genAggregations: GeneratedAggregationsFunction,
    private val gkeyOutMapping: Array[(Int, Int)],
    private val groupingSetsMapping: Array[(Int, Int)])
  extends RichGroupReduceFunction[Row, Row]
    with Compiler[GeneratedAggregations] {

  Preconditions.checkNotNull(gkeyOutMapping)
  Preconditions.checkNotNull(groupingSetsMapping)

  private var intermediateGKeys: Option[Array[Int]] = None

  private var output: Row = _
  private var accumulators: Row = _

  val LOG = LoggerFactory.getLogger(this.getClass)
  private var function: GeneratedAggregations = _

  override def open(config: Configuration) {
    LOG.debug(s"Compiling AggregateHelper: $genAggregations.name \n\n " +
                s"Code:\n$genAggregations.code")
    val clazz = compile(
      getClass.getClassLoader,
      genAggregations.name,
      genAggregations.code)
    LOG.debug("Instantiating AggregateHelper.")
    function = clazz.newInstance()

    output = function.createOutputRow()
    accumulators = function.createAccumulators()

    if (!groupingSetsMapping.isEmpty) {
      intermediateGKeys = Some(gkeyOutMapping.map(_._1))
    }
  }

  override def reduce(records: Iterable[Row], out: Collector[Row]): Unit = {

    // reset accumulators
    function.resetAccumulator(accumulators)

    val iterator = records.iterator()

    var record: Row = null
    while (iterator.hasNext) {
      record = iterator.next()

      // accumulate
      function.accumulate(accumulators, record)
    }

    // set group keys value to final output
    function.setForwardedFields(record, null, output)

    // set agg results to output
    function.setAggregationResults(accumulators, output)

    // set grouping set flags to output
    if (intermediateGKeys.isDefined) {
      var i = 0
      while (i < groupingSetsMapping.length) {
        val (in, out) = groupingSetsMapping(i)
        output.setField(out, !intermediateGKeys.get.contains(in))
        i += 1
      }
    }

    out.collect(output)
  }
}
