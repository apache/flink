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
  * [[RichGroupReduceFunction]] to compute the final result of a pre-aggregated aggregation
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

  Preconditions.checkNotNull(aggregates)
  Preconditions.checkNotNull(aggOutFields)
  Preconditions.checkNotNull(gkeyOutFields)
  Preconditions.checkNotNull(groupingSetsMapping)

  private var output: Row = _

  private val intermediateGKeys: Option[Array[Int]] = if (!groupingSetsMapping.isEmpty) {
    Some(gkeyOutFields)
  } else {
    None
  }

  private val numAggs = aggregates.length
  private val numGKeys = gkeyOutFields.length

  private val accumulators: Array[JArrayList[Accumulator]] =
    Array.fill(numAggs)(new JArrayList[Accumulator](2))

  override def open(config: Configuration) {
    output = new Row(finalRowArity)

    // init lists with two empty accumulators
    for (i <- aggregates.indices) {
      val accumulator = aggregates(i).createAccumulator()
      accumulators(i).add(accumulator)
      accumulators(i).add(accumulator)
    }
  }

  override def reduce(records: Iterable[Row], out: Collector[Row]): Unit = {

    val iterator = records.iterator()

    // reset first accumulator
    var i = 0
    while (i < aggregates.length) {
      aggregates(i).resetAccumulator(accumulators(i).get(0))
      i += 1
    }

    while (iterator.hasNext) {
      val record = iterator.next()

      // accumulate
      i = 0
      while (i < aggregates.length) {
        // insert received accumulator into acc list
        val newAcc = record.getField(numGKeys + i).asInstanceOf[Accumulator]
        accumulators(i).set(1, newAcc)
        // merge acc list
        val retAcc = aggregates(i).merge(accumulators(i))
        // insert result into acc list
        accumulators(i).set(0, retAcc)
        i += 1
      }

      // check if this record is the last record
      if (!iterator.hasNext) {
        // set group keys value to final output
        i = 0
        while (i < gkeyOutFields.length) {
          output.setField(gkeyOutFields(i), record.getField(i))
          i += 1
        }

        // get final aggregate value and set to output.
        i = 0
        while (i < aggOutFields.length) {
          output.setField(aggOutFields(i), aggregates(i).getValue(accumulators(i).get(0)))
          i += 1
        }

        // set grouping set flags to output
        if (intermediateGKeys.isDefined) {
          i = 0
          while (i < groupingSetsMapping.length) {
            val (in, out) = groupingSetsMapping(i)
            output.setField(out, !intermediateGKeys.get.contains(in))
            i += 1
          }
        }

        out.collect(output)
      }
    }
  }
}
