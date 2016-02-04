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
package org.apache.flink.api.table.runtime

import java.lang.Iterable
import com.google.common.base.Preconditions
import org.apache.flink.api.common.functions.RichGroupReduceFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector
import scala.collection.JavaConversions._
import org.apache.flink.api.table.Row
import org.apache.flink.api.table.runtime.aggregate.Aggregate

/**
 * A wrapper Flink GroupReduceOperator UDF of aggregates. It takes the grouped data as input,
 * feed to the aggregates, and collect the record with aggregated value.
 *
 * @param aggregates SQL aggregate functions.
 * @param fields The grouped keys' indices in the input.
 * @param groupingKeys The grouping keys' positions.
 */
class AggregateFunction(
    private val aggregates: Array[Aggregate[_ <: Any]],
    private val fields: Array[Int],
    private val groupingKeys: Array[Int]) extends RichGroupReduceFunction[Row, Row] {

  override def open(config: Configuration) {
    Preconditions.checkNotNull(aggregates)
    Preconditions.checkNotNull(fields)
    Preconditions.checkNotNull(groupingKeys)
    Preconditions.checkArgument(aggregates.size == fields.size)
  }

  override def reduce(records: Iterable[Row], out: Collector[Row]): Unit = {
    aggregates.foreach(_.initiateAggregate)

    var currentRecord: Row = null

    // iterate all input records, feed to each aggregate.
    val recordIterator = records.iterator
    while (recordIterator.hasNext) {
      currentRecord = recordIterator.next()
      for (i <- 0 until aggregates.length) {
        aggregates(i).aggregate(currentRecord.productElement(fields(i)))
      }
    }

    // output a new Row type that contains the grouping keys and aggregates
    var outValue: Row = new Row(groupingKeys.length + aggregates.length)

    // copy the grouping fields from the last input row to the output row
    for (i <- 0 until groupingKeys.length) {
      outValue.setField(i, currentRecord.productElement(groupingKeys(i)))
    }
    // copy the results of the aggregate functions to the output row
    for (i <- groupingKeys.length until groupingKeys.length + aggregates.length) {
      outValue.setField(i, aggregates(i - groupingKeys.length).getAggregated)
    }
    out.collect(outValue)
  }
}
