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

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.table.codegen.{Compiler, GeneratedAggregationsFunction}
import org.apache.flink.table.runtime.types.CRow
import org.apache.flink.table.util.Logging
import org.apache.flink.types.Row

/**
  * Aggregate Function used for the aggregate or table aggregate operator in
  * [[org.apache.flink.streaming.api.datastream.WindowedStream]].
  *
  * @param genAggregations Generated aggregate or table aggregate helper function
  * @param isTableAggregate Whether it is table aggregate.
  */
class AggregateAggFunction[F <: AggregationsFunction](
    genAggregations: GeneratedAggregationsFunction,
    isTableAggregate: Boolean)
  extends AggregateFunction[CRow, Row, Row]
    with Compiler[F] with Logging {

  private var function: F = _

  override def createAccumulator(): Row = {
    if (function == null) {
      initFunction()
    }
    function.createAccumulators()
  }

  override def add(value: CRow, accumulatorRow: Row): Row = {
    if (function == null) {
      initFunction()
    }
    function.accumulate(accumulatorRow, value.row)
    accumulatorRow
  }

  override def getResult(accumulatorRow: Row): Row = {
    if (function == null) {
      initFunction()
    }

    if (isTableAggregate) {
      // pass both accumulator and function to the window function and emit value in it.
      Row.of(accumulatorRow, function)
    } else {
      val output = function.createOutputRow()
      function.asInstanceOf[GeneratedAggregations].setAggregationResults(accumulatorRow, output)
      output
    }
  }

  override def merge(aAccumulatorRow: Row, bAccumulatorRow: Row): Row = {
    if (function == null) {
      initFunction()
    }
    function.mergeAccumulatorsPair(aAccumulatorRow, bAccumulatorRow)
  }

  def initFunction(): Unit = {
    LOG.debug(s"Compiling AggregateHelper: $genAggregations.name \n\n " +
      s"Code:\n$genAggregations.code")
    val clazz = compile(
      Thread.currentThread().getContextClassLoader,
      genAggregations.name,
      genAggregations.code)
    LOG.debug("Instantiating AggregateHelper.")
    function = clazz.newInstance()
  }
}
