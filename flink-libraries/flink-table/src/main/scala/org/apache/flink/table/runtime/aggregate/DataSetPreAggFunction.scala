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

import org.apache.flink.api.common.functions._
import org.apache.flink.configuration.Configuration
import org.apache.flink.table.functions.{Accumulator, AggregateFunction}
import org.apache.flink.types.Row
import org.apache.flink.util.{Collector, Preconditions}

/**
  * [[GroupCombineFunction]] and [[MapPartitionFunction]] to compute pre-aggregates for batch
  * (DataSet) queries.
  *
  * @param aggregates The aggregate functions.
  * @param aggInFields The positions of the aggregation input fields.
  * @param groupingKeys The positions of the grouping keys in the input.
  */
class DataSetPreAggFunction(
    private val aggregates: Array[AggregateFunction[_ <: Any]],
    private val aggInFields: Array[Int],
    private val groupingKeys: Array[Int])
  extends AbstractRichFunction
    with GroupCombineFunction[Row, Row]
    with MapPartitionFunction[Row, Row] {

  private var output: Row = _
  private val aggsWithIdx: Array[(AggregateFunction[_], Int)] = aggregates.zipWithIndex
  private var accumulators: Array[Accumulator] = _

  override def open(config: Configuration) {
    Preconditions.checkNotNull(aggregates)
    Preconditions.checkNotNull(aggInFields)
    Preconditions.checkNotNull(groupingKeys)
    accumulators = new Array(aggregates.length)
    output = new Row(groupingKeys.length + aggregates.length)
  }

  override def combine(values: Iterable[Row], out: Collector[Row]): Unit = {
    preaggregate(values, out)
  }

  override def mapPartition(values: Iterable[Row], out: Collector[Row]): Unit = {
    preaggregate(values, out)
  }

  def preaggregate(records: Iterable[Row], out: Collector[Row]): Unit = {

    aggsWithIdx.foreach { case (agg, i) => accumulators(i) = agg.createAccumulator() }

    var last: Row = null
    val iterator = records.iterator()

    while (iterator.hasNext) {
      val record = iterator.next()
      aggsWithIdx.foreach { case (agg, i) =>
        agg.accumulate(accumulators(i), record.getField(aggInFields(i)))
      }
      last = record
    }

    // set grouping keys to output
    groupingKeys.zipWithIndex.foreach( g => output.setField(g._2, last.getField(g._1)))
    // set agg results to output
    aggsWithIdx.foreach( a => output.setField(a._2 + groupingKeys.length, accumulators(a._2)))

    out.collect(output)
  }

}
