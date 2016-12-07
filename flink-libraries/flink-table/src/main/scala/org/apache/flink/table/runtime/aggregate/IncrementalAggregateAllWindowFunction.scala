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
import org.apache.flink.util.{Collector, Preconditions}

/**
  * Computes the final aggregate value from incrementally computed aggreagtes.
  *
  * @param aggregates   The aggregate functions.
  * @param groupKeysMapping The index mapping of group keys between intermediate aggregate Row
  *                         and output Row.
  * @param aggregateMapping The index mapping between aggregate function list and aggregated value
  *                         index in output Row.
  * @param finalRowArity  The arity of the final output row.
  */
class IncrementalAggregateAllWindowFunction[W <: Window](
    private val aggregates: Array[Aggregate[_ <: Any]],
    private val groupKeysMapping: Array[(Int, Int)],
    private val aggregateMapping: Array[(Int, Int)],
    private val finalRowArity: Int)
  extends RichAllWindowFunction[Row, Row, W] {

  private var output: Row = _

  override def open(parameters: Configuration): Unit = {
    Preconditions.checkNotNull(aggregates)
    Preconditions.checkNotNull(groupKeysMapping)
    output = new Row(finalRowArity)
  }

  /**
    * Calculate aggregated values output by aggregate buffer, and set them into output
    * Row based on the mapping relation between intermediate aggregate data and output data.
    */
  override def apply(
    window: W,
    records: Iterable[Row],
    out: Collector[Row]): Unit = {

    val iterator = records.iterator

    if (iterator.hasNext) {
      val record = iterator.next()
      // Set group keys value to final output.
      groupKeysMapping.foreach {
        case (after, previous) =>
          output.setField(after, record.getField(previous))
      }
      // Evaluate final aggregate value and set to output.
      aggregateMapping.foreach {
        case (after, previous) =>
          output.setField(after, aggregates(previous).evaluate(record))
      }
      out.collect(output)
    }
  }
}
