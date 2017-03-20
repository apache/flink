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

class IncrementalAggregateOverWindowFunction[W <: Window](
  private val numGroupingKey: Int,
  private val numAggregates: Int,
  private val forwardedFieldCount: Int)
    extends RichWindowFunction[Row, Row, Tuple, W] {

  private var output: Row = _
  private var reuse: Row = _

  override def open(parameters: Configuration): Unit = {
    output = new Row(forwardedFieldCount + numAggregates)
  }
  override def apply(
    key: Tuple,
    window: W,
    records: Iterable[Row],
    out: Collector[Row]): Unit = {

    var i = 0
    val iter = records.iterator
    while (iter.hasNext) {
      reuse = iter.next
      i = 0
      // set the output value of forward fields
      while (i < forwardedFieldCount) {
        output.setField(i, reuse.getField(i))
        i += 1
      }

      i = 0
      // set aggregated values in the output
      while (i < numAggregates) {
        val index = forwardedFieldCount + i
        output.setField(index, reuse.getField(index))
        i += 1
      }

      out.collect(output)
    }
  }

}
