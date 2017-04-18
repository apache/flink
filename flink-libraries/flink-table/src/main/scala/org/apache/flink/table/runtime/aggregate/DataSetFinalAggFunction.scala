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
  * [[RichGroupReduceFunction]] to compute the final result of a pre-aggregated aggregation
  * for batch (DataSet) queries.
  *
  * @param genAggregations Code-generated [[GeneratedAggregations]]
  * @param gkeyOutFields The positions of the grouping keys in the output
  * @param groupingSetsMapping The mapping of grouping set keys between input and output positions.
  */
class DataSetFinalAggFunction(
    private val genAggregations: GeneratedAggregationsFunction,
    private val gkeyOutFields: Array[Int],
    private val groupingSetsMapping: Array[(Int, Int)])
  extends RichGroupReduceFunction[Row, Row]
    with Compiler[GeneratedAggregations] {

  Preconditions.checkNotNull(gkeyOutFields)
  Preconditions.checkNotNull(groupingSetsMapping)

  private var output: Row = _
  private var accumulators: Row = _

  val LOG = LoggerFactory.getLogger(this.getClass)
  private var function: GeneratedAggregations = _

  private val intermediateGKeys: Option[Array[Int]] = if (!groupingSetsMapping.isEmpty) {
    Some(gkeyOutFields)
  } else {
    None
  }

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
  }

  override def reduce(records: Iterable[Row], out: Collector[Row]): Unit = {

    val iterator = records.iterator()

    // reset first accumulator
    function.resetAccumulator(accumulators)

    var i = 0
    while (iterator.hasNext) {
      val record = iterator.next()

      // accumulate
      function.mergeAccumulatorsPairWithKeyOffset(accumulators, record)

      // check if this record is the last record
      if (!iterator.hasNext) {
        // set group keys value to final output
        i = 0
        while (i < gkeyOutFields.length) {
          output.setField(gkeyOutFields(i), record.getField(i))
          i += 1
        }

        // get final aggregate value and set to output.
        function.setAggregationResults(accumulators, output)

        // set grouping set flags to output
        if (intermediateGKeys.isDefined) {
          i = 0
          while (i < groupingSetsMapping.length) {
            val (in, out) = groupingSetsMapping(i)
            output.setField(out, !intermediateGKeys.get.contains(in))
            i += 1
          }
        }

        out.collect(output)
      }
    }
  }
}
