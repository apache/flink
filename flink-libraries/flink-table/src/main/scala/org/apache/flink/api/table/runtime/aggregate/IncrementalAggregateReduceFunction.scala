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
package org.apache.flink.api.table.runtime.aggregate

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.table.Row
import org.apache.flink.util.Preconditions

/**
  * For Incremental intermediate aggregate Rows, merge every row into aggregate buffer.
  *
  * @param aggregates   The aggregate functions.
  * @param groupKeysMapping The index mapping of group keys between intermediate aggregate Row
  *                         and output Row.
  */
class IncrementalAggregateReduceFunction(
    private val aggregates: Array[Aggregate[_]],
    private val groupKeysMapping: Array[(Int, Int)],
    private val intermediateRowArity: Int)extends ReduceFunction[Row] {

  Preconditions.checkNotNull(aggregates)
  Preconditions.checkNotNull(groupKeysMapping)
  @transient var accumulatorRow:Row = _

  /**
    * For Incremental intermediate aggregate Rows, merge value1 and value2
    * into aggregate buffer, return aggregate buffer.
    *
    * @param value1 The first value to combined.
    * @param value2 The second value to combined.
    * @return The combined value of both input values.
    *
    */
  override def reduce(value1: Row, value2: Row): Row = {

    if(null == accumulatorRow){
      accumulatorRow = new Row(intermediateRowArity)
    }

    // Initiate intermediate aggregate value.
    aggregates.foreach(_.initiate(accumulatorRow))

    //merge value1 to accumulatorRow
    aggregates.foreach(_.merge(value1, accumulatorRow))
    //merge value2 to accumulatorRow
    aggregates.foreach(_.merge(value2, accumulatorRow))

    // Set group keys value to accumulator
    for (i <- groupKeysMapping.indices) {
      accumulatorRow.setField(i, value2.productElement(i))
    }
    accumulatorRow
  }
}
