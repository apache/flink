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
  * [[RichGroupReduceFunction]] to compute the final result of a pre-aggragregated aggregation
  * for batch (DataSet) queries.
  *
  * @param aggregates The aggregate functions.
  * @param aggOutFields The positions of the aggregation results in the output
  * @param gkeyOutFields The positions of the grouping keys in the output
  * @param groupingSetsMapping The mapping of grouping set keys between input and output positions.
  * @param finalRowArity The arity of the final resulting row
  */
class DataSetFinalAggFunction(
    private val aggregates: Array[AggregateFunction[_ <: Any]],
    private val aggOutFields: Array[Int],
    private val gkeyOutFields: Array[Int],
    private val groupingSetsMapping: Array[(Int, Int)],
    private val finalRowArity: Int)
  extends RichGroupReduceFunction[Row, Row] {

  private var output: Row = _

  private var intermediateGKeys: Option[Array[Int]] = None
  private val numAggs = aggregates.length
  private val numGKeys = gkeyOutFields.length

  private val accumulators: Array[JArrayList[Accumulator]] =
    Array.fill(numAggs)(new JArrayList[Accumulator](2))

  override def open(config: Configuration) {
    Preconditions.checkNotNull(aggregates)
    Preconditions.checkNotNull(aggOutFields)
    Preconditions.checkNotNull(gkeyOutFields)
    output = new Row(finalRowArity)

    if (!groupingSetsMapping.isEmpty) {
      intermediateGKeys = Some(gkeyOutFields)
    }

    // init lists with two empty accumulators
    for (i <- aggregates.indices) {
      val accumulator = aggregates(i).createAccumulator()
      accumulators(i).add(accumulator)
      accumulators(i).add(accumulator)
    }
  }

  override def reduce(records: Iterable[Row], out: Collector[Row]): Unit = {

    var last: Row = null
    val iterator = records.iterator()

    // reset first accumulator
    for (i <- aggregates.indices) {
      val accumulator = aggregates(i).createAccumulator()
      accumulators(i).set(0, accumulator)
    }

    while (iterator.hasNext) {
      val record = iterator.next()

      for (i <- aggregates.indices) {
        // insert received accumulator into acc list
        val newAcc = record.getField(numGKeys + i).asInstanceOf[Accumulator]
        accumulators(i).set(1, newAcc)
        // merge acc list
        val retAcc = aggregates(i).merge(accumulators(i))
        // insert result into acc list
        accumulators(i).set(0, retAcc)
      }
      last = record
    }

    // set grouping keys to output
    gkeyOutFields.zipWithIndex.foreach(g => output.setField(g._1, last.getField(g._2)))
    // set aggregation results to output
    aggOutFields.zipWithIndex
      .foreach(a => output.setField(a._1, aggregates(a._2).getValue(accumulators(a._2).get(0))))

    // set grouping set flags to output
    if (intermediateGKeys.isDefined) {
      groupingSetsMapping.foreach {
        case (in, out) =>
          output.setField(out, !intermediateGKeys.get.contains(in))
      }
    }

    out.collect(output)
  }
}
