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
import java.util.{ArrayList => JArrayList}

import org.apache.flink.api.common.functions.RichGroupReduceFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.table.functions.{Accumulator, AggregateFunction}
import org.apache.flink.types.Row
import org.apache.flink.util.{Collector, Preconditions}

/**
  * It wraps the aggregate logic inside of
  * [[org.apache.flink.api.java.operators.GroupReduceOperator]].
  * It is only used for tumbling count-window on batch.
  *
  * @param windowSize       Tumble count window size
  * @param aggregates       The aggregate functions.
  * @param groupKeysMapping The index mapping of group keys between intermediate aggregate Row
  *                         and output Row.
  * @param aggregateMapping The index mapping between aggregate function list and aggregated value
  *                         index in output Row.
  * @param finalRowArity    The output row field count
  */
class DataSetTumbleCountWindowAggReduceGroupFunction(
    private val windowSize: Long,
    private val aggregates: Array[AggregateFunction[_ <: Any]],
    private val groupKeysMapping: Array[(Int, Int)],
    private val aggregateMapping: Array[(Int, Int)],
    private val finalRowArity: Int)
  extends RichGroupReduceFunction[Row, Row] {

  private var aggregateBuffer: Row = _
  private var output: Row = _
  private val accumStartPos: Int = groupKeysMapping.length
  private val intermediateRowArity: Int = accumStartPos + aggregates.length + 1
  private val maxMergeLen = 16
  val accumulatorList = Array.fill(aggregates.length) {
    new JArrayList[Accumulator]()
  }

  override def open(config: Configuration) {
    Preconditions.checkNotNull(aggregates)
    Preconditions.checkNotNull(groupKeysMapping)
    aggregateBuffer = new Row(intermediateRowArity)
    output = new Row(finalRowArity)
  }

  override def reduce(records: Iterable[Row], out: Collector[Row]): Unit = {

    var count: Long = 0
    accumulatorList.foreach(_.clear())

    val iterator = records.iterator()

    while (iterator.hasNext) {
      val record = iterator.next()

      if (count == 0) {
        // clear the accumulator list for all aggregate
        accumulatorList.foreach(_.clear())
      }

      // collect the accumulators for each aggregate
      for (i <- aggregates.indices) {
        accumulatorList(i).add(record.getField(accumStartPos + i).asInstanceOf[Accumulator])
      }
      count += 1

      // for every maxMergeLen accumulators, we merge them into one
      if (count % maxMergeLen == 0) {
        for (i <- aggregates.indices) {
          val agg = aggregates(i)
          val accumulator = agg.merge(accumulatorList(i))
          accumulatorList(i).clear()
          accumulatorList(i).add(accumulator)
        }
      }

      if (windowSize == count) {
        // set group keys value to final output.
        groupKeysMapping.foreach {
          case (after, previous) =>
            output.setField(after, record.getField(previous))
        }

        // merge the accumulators and then get value for the final output
        aggregateMapping.foreach {
          case (after, previous) =>
            val agg = aggregates(previous)
            val accumulator = agg.merge(accumulatorList(previous))
            output.setField(after, agg.getValue(accumulator))
        }

        // emit the output
        out.collect(output)
        count = 0
      }
    }
  }
}
