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

import org.apache.flink.types.Row
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.windowing.RichAllWindowFunction
import org.apache.flink.streaming.api.windowing.windows.Window
import org.apache.flink.table.runtime.TableAggregateCollector
import org.apache.flink.table.runtime.types.CRow
import org.apache.flink.util.Collector

/**
  * Computes the final (table)aggregate value from incrementally computed aggregates.
  *
  * @param finalRowArity The arity of the final output row.
  * @param isTableAggregate Whether it is table aggregate.
  */
class IncrementalAggregateAllWindowFunction[W <: Window](
    private val finalRowArity: Int,
    private val isTableAggregate: Boolean)
  extends RichAllWindowFunction[Row, CRow, W] {

  private var output: CRow = _
  private var concatCollector: TableAggregateCollector = _

  override def open(parameters: Configuration): Unit = {
    output = new CRow(new Row(finalRowArity), true)
    if (isTableAggregate) {
      concatCollector = new TableAggregateCollector(0)
      concatCollector.setResultRow(output.row)
    }
  }

  /**
    * Calculate aggregated values output by aggregate buffer, and set them into output
    * Row based on the mapping relation between intermediate aggregate data and output data.
    */
  override def apply(
      window: W,
      records: Iterable[Row],
      out: Collector[CRow]): Unit = {

    val iterator = records.iterator

    if (iterator.hasNext) {
      val record = iterator.next()
      if (isTableAggregate) {
        concatCollector.out = out
        val accumulator = record.getField(0).asInstanceOf[Row]
        val func = record.getField(1).asInstanceOf[GeneratedTableAggregations]
        func.emit(accumulator, concatCollector)
      } else {
        var i = 0
        while (i < record.getArity) {
          output.row.setField(i, record.getField(i))
          i += 1
        }
        out.collect(output)
      }
    }
  }
}
