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
import org.apache.flink.table.functions.{Accumulator, AggregateFunction}
import org.apache.flink.types.Row
import org.apache.flink.util.{Collector, Preconditions}

/**
  * [[RichGroupReduceFunction]] to compute aggregates that do not support preaggregation for batch
  * (DataSet) queries.
  *
  * @param aggregates The aggregate functions.
  * @param aggInFields The positions of the aggregation input fields.
  * @param gkeyOutMapping The mapping of group keys between input and output positions.
  * @param aggOutMapping  The mapping of aggregates to output positions.
  * @param groupingSetsMapping The mapping of grouping set keys between input and output positions.
  * @param finalRowArity The arity of the final resulting row.
  */
class DataSetAggFunction(
    private val aggregates: Array[AggregateFunction[_ <: Any]],
    private val aggInFields: Array[Int],
    private val aggOutMapping: Array[(Int, Int)],
    private val gkeyOutMapping: Array[(Int, Int)],
    private val groupingSetsMapping: Array[(Int, Int)],
    private val finalRowArity: Int)
  extends RichGroupReduceFunction[Row, Row] {

  private var output: Row = _

  private var intermediateGKeys: Option[Array[Int]] = None
  private val aggsWithIdx: Array[(AggregateFunction[_], Int)] = aggregates.zipWithIndex
  private var accumulators: Array[Accumulator] = _

  override def open(config: Configuration) {
    Preconditions.checkNotNull(aggregates)
    Preconditions.checkNotNull(aggInFields)
    Preconditions.checkNotNull(aggOutMapping)
    Preconditions.checkNotNull(gkeyOutMapping)
    accumulators = new Array(aggregates.length)
    output = new Row(finalRowArity)

    if (!groupingSetsMapping.isEmpty) {
      intermediateGKeys = Some(gkeyOutMapping.map(_._1))
    }
  }


  override def reduce(records: Iterable[Row], out: Collector[Row]): Unit = {

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
    gkeyOutMapping.foreach { case (out, in) =>
      output.setField(out, last.getField(in))
    }

    // set agg results to output
    aggOutMapping.foreach { case (out, in) =>
      output.setField(out, aggregates(in).getValue(accumulators(in)))
    }

    // set grouping set flags to output
    if (intermediateGKeys.isDefined) {
      groupingSetsMapping.foreach {
        case (inputIndex, outputIndex) =>
          output.setField(outputIndex, !intermediateGKeys.get.contains(inputIndex))
      }
    }

    out.collect(output)
  }
}
