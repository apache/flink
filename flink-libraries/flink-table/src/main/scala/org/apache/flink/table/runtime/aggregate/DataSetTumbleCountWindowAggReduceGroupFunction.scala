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
import org.apache.flink.types.Row
import org.apache.flink.util.{Collector, Preconditions}

/**
  * It wraps the aggregate logic inside of
  * [[org.apache.flink.api.java.operators.GroupReduceOperator]].
  * It is only used for tumbling count-window on batch.
  *
  * @param windowSize Tumble count window size
  * @param aggregates The aggregate functions.
  * @param groupKeysMapping The index mapping of group keys between intermediate aggregate Row
  *                         and output Row.
  * @param aggregateMapping The index mapping between aggregate function list and aggregated value
  *                         index in output Row.
  * @param intermediateRowArity The intermediate row field count
  * @param finalRowArity The output row field count
  */
class DataSetTumbleCountWindowAggReduceGroupFunction(
    private val windowSize: Long,
    private val aggregates: Array[Aggregate[_ <: Any]],
    private val groupKeysMapping: Array[(Int, Int)],
    private val aggregateMapping: Array[(Int, Int)],
    private val intermediateRowArity: Int,
    private val finalRowArity: Int)
  extends RichGroupReduceFunction[Row, Row] {

  private var aggregateBuffer: Row = _
  private var output: Row = _

  override def open(config: Configuration) {
    Preconditions.checkNotNull(aggregates)
    Preconditions.checkNotNull(groupKeysMapping)
    aggregateBuffer = new Row(intermediateRowArity)
    output = new Row(finalRowArity)
  }

  override def reduce(records: Iterable[Row], out: Collector[Row]): Unit = {

    var count: Long = 0

    val iterator = records.iterator()

    while (iterator.hasNext) {
      val record = iterator.next()
      if (count == 0) {
        // initiate intermediate aggregate value.
        aggregates.foreach(_.initiate(aggregateBuffer))
      }
      // merge intermediate aggregate value to buffer.
      aggregates.foreach(_.merge(record, aggregateBuffer))

      count += 1
      if (windowSize == count) {
        // set group keys value to final output.
        groupKeysMapping.foreach {
          case (after, previous) =>
            output.setField(after, record.getField(previous))
        }
        // evaluate final aggregate value and set to output.
        aggregateMapping.foreach {
          case (after, previous) =>
            output.setField(after, aggregates(previous).evaluate(aggregateBuffer))
        }
        // emit the output
        out.collect(output)
        count = 0
      }
    }
  }
}
