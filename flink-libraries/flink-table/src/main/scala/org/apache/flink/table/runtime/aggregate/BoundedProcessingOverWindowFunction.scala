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

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.types.Row
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction
import org.apache.flink.streaming.api.windowing.windows.Window
import org.apache.flink.util.Collector
import org.apache.flink.table.functions.AggregateFunction
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.util.Preconditions
import org.apache.flink.table.functions.Accumulator
import java.lang.Iterable

class BoundedProcessingOverWindowFunction[W <: Window](
  private val aggregates: Array[AggregateFunction[_]],
  private val aggFields: Array[Int],
  private val forwardedFieldCount: Int)
    extends RichWindowFunction[Row, Row, Tuple, W] {

  Preconditions.checkNotNull(aggregates)
  Preconditions.checkNotNull(aggFields)
  Preconditions.checkArgument(aggregates.length == aggFields.length)

  private var output: Row = _
  private var accumulators: Row = _
  private var reuse: Row = _

  output = new Row(forwardedFieldCount + aggregates.length)
  if (null == accumulators) {
    accumulators = new Row(aggregates.length)
  }

  override def apply(
    key: Tuple,
    window: W,
    records: Iterable[Row],
    out: Collector[Row]): Unit = {

    var i = 0
    // setting the accumulators for each aggregation
    while (i < aggregates.length) {
      accumulators.setField(i, aggregates(i).createAccumulator())
      i += 1
    }
    
    // iterate over window elements, and aggregate values
    val iter = records.iterator
    while (iter.hasNext) {
      reuse = iter.next
      i = 0
      // for each aggregation, accumulate
      while (i < aggregates.length) {
        val accumulator = accumulators.getField(i).asInstanceOf[Accumulator]
        aggregates(i).accumulate(accumulator, reuse.getField(aggFields(i)))
        i += 1
      }

    }

    i = 0
    // set the output value of forward fields
    while (i < forwardedFieldCount) {
      output.setField(i, reuse.getField(i))
      i += 1
    }

    i = 0
    // set aggregated values in the output
    while (i < aggregates.length) {
      val index = forwardedFieldCount + i
      val accumulator = accumulators.getField(i).asInstanceOf[Accumulator]
      output.setField(index, aggregates(i).getValue(accumulator))
      i += 1
    }

    out.collect(output)
  }

}
