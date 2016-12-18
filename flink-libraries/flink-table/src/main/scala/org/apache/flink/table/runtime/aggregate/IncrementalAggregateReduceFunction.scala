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

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.types.Row
import org.apache.flink.util.Preconditions

/**
  * Incrementally computes group window aggregates.
  *
  * @param aggregates   The aggregate functions.
  * @param groupKeysMapping The index mapping of group keys between intermediate aggregate Row
  *                         and output Row.
  */
class IncrementalAggregateReduceFunction(
    private val aggregates: Array[Aggregate[_]],
    private val groupKeysMapping: Array[(Int, Int)],
    private val intermediateRowArity: Int)
  extends ReduceFunction[Row] {

  Preconditions.checkNotNull(aggregates)
  Preconditions.checkNotNull(groupKeysMapping)

  /**
    * For Incremental intermediate aggregate Rows, merge value1 and value2
    * into aggregate buffer, return aggregate buffer.
    *
    * @param value1 The first value to combined.
    * @param value2 The second value to combined.
    * @return  accumulatorRow A resulting row that combines two input values.
    *
    */
  override def reduce(value1: Row, value2: Row): Row = {

    // TODO: once FLINK-5105 is solved, we can avoid creating a new row for each invocation
    //   and directly merge value1 and value2.
    val accumulatorRow = new Row(intermediateRowArity)

    // copy all fields of value1 into accumulatorRow
    (0 until intermediateRowArity)
    .foreach(i => accumulatorRow.setField(i, value1.getField(i)))
    // merge value2 to accumulatorRow
    aggregates.foreach(_.merge(value2, accumulatorRow))

    accumulatorRow
  }
}
