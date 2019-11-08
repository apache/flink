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

import org.apache.flink.api.common.functions.{AbstractRichFunction, GroupCombineFunction, MapPartitionFunction}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.ResultTypeQueryable
import org.apache.flink.configuration.Configuration
import org.apache.flink.table.codegen.{Compiler, GeneratedAggregationsFunction}
import org.apache.flink.table.util.Logging
import org.apache.flink.types.Row
import org.apache.flink.util.Collector

/**
  * This wraps the aggregate logic inside of
  * [[org.apache.flink.api.java.operators.GroupCombineOperator]].
  *
  * @param genAggregations Code-generated [[GeneratedAggregations]]
  * @param keysAndAggregatesArity    The total arity of keys and aggregates
  * @param gap                 Session time window gap.
  * @param intermediateRowType Intermediate row data type.
  */
class DataSetSessionWindowAggregatePreProcessor(
    genAggregations: GeneratedAggregationsFunction,
    keysAndAggregatesArity: Int,
    gap: Long,
    @transient var intermediateRowType: TypeInformation[Row])
  extends AbstractRichFunction
  with MapPartitionFunction[Row,Row]
  with GroupCombineFunction[Row,Row]
  with ResultTypeQueryable[Row]
  with Compiler[GeneratedAggregations]
  with Logging {

  private var output: Row = _
  private val rowTimeFieldPos = keysAndAggregatesArity
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

    accumulators = function.createAccumulators()
    output = function.createOutputRow()
  }

  /**
    * For sub-grouped intermediate aggregate Rows, divide window based on the rowtime
    * (current'rowtime - previous’rowtime > gap), and then merge data (within a unified window)
    * into an aggregate buffer.
    *
    * @param records  Sub-grouped intermediate aggregate Rows.
    * @return Combined intermediate aggregate Row.
    *
    */
  override def combine(records: Iterable[Row], out: Collector[Row]): Unit = {

    var windowStart: java.lang.Long = null
    var windowEnd: java.lang.Long = null
    var currentRowTime: java.lang.Long = null

    // reset accumulator
    function.resetAccumulator(accumulators)

    val iterator = records.iterator()

    while (iterator.hasNext) {
      val record = iterator.next()
      currentRowTime = record.getField(rowTimeFieldPos).asInstanceOf[Long]
      // initial traversal or opening a new window
      if (windowEnd == null || (windowEnd != null && (currentRowTime > windowEnd))) {

        // calculate the current window and open a new window.
        if (windowEnd != null) {
          // emit the current window's merged data
          doCollect(out, windowStart, windowEnd)

          // reset accumulator
          function.resetAccumulator(accumulators)
        } else {
          // set group keys to aggregateBuffer.
          function.setForwardedFields(record, output)
        }

        windowStart = record.getField(rowTimeFieldPos).asInstanceOf[Long]
      }

      function.mergeAccumulatorsPair(accumulators, record)

      // the current rowtime is the last rowtime of the next calculation.
      windowEnd = currentRowTime + gap
    }
    // emit the merged data of the current window.
    doCollect(out, windowStart, windowEnd)
  }

  /**
    * Divide window based on the rowtime
    * (current'rowtime - previous’rowtime > gap), and then merge data (within a unified window)
    * into an aggregate buffer.
    *
    * @param records  Intermediate aggregate Rows.
    * @return Pre partition intermediate aggregate Row.
    *
    */
  override def mapPartition(records: Iterable[Row], out: Collector[Row]): Unit = {
    combine(records, out)
  }

  /**
    * Emit the merged data of the current window.
    *
    * @param out             the collection of the aggregate results
    * @param windowStart     the window's start attribute value is the min (rowtime)
    *                        of all rows in the window.
    * @param windowEnd       the window's end property value is max (rowtime) + gap
    *                        for all rows in the window.
    */
  def doCollect(
      out: Collector[Row],
      windowStart: Long,
      windowEnd: Long): Unit = {

    function.setAggregationResults(accumulators, output)

    // intermediate Row WindowStartPos is rowtime pos.
    output.setField(rowTimeFieldPos, windowStart)

    // intermediate Row WindowEndPos is rowtime pos + 1.
    output.setField(rowTimeFieldPos + 1, windowEnd)

    out.collect(output)
  }

  override def getProducedType: TypeInformation[Row] = {
    intermediateRowType
  }
}
