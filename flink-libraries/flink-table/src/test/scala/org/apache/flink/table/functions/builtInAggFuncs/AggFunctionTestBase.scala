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
package org.apache.flink.table.functions.builtInAggFuncs

import org.apache.flink.table.functions.{Accumulator, AggregateFunction}
import java.math.BigDecimal

import org.apache.flink.table.functions.utils.UserDefinedFunctionUtils._
import org.apache.flink.types.Row
import org.junit.Test
import org.junit.Assert.assertEquals

/**
  * Base class for aggregate function test
  *
  * @tparam T the type for the aggregation result
  */
abstract class AggFunctionTestBase[T] {
  private val offset = 2
  private val numOfAggregates = 1
  private val rowArity = offset + numOfAggregates
  def inputValueSets: Seq[Seq[_]]
  def expectedResults: Seq[T]
  def aggregator: AggregateFunction[T]

  @Test
  // test aggregate functions without partial merge
  def testAggregateWithoutMerge(): Unit = {
    // iterate over input sets
    for((vals, expected) <- inputValueSets.zip(expectedResults)) {
      val resultRow = aggregateVals(vals)
      val result = getResult(resultRow)
      validateResult(expected, result)
    }
  }

  @Test
  // test aggregate functions with partial merge
  def testAggregateWithMerge(): Unit = {

    if (ifMethodExitInFunction("merge", aggregator)) {
      // iterate over input sets
      for((vals, expected) <- inputValueSets.zip(expectedResults)) {
        val (firstVals, secondVals) = vals.splitAt(vals.length / 2)
        val combined = aggregateVals(firstVals) :: aggregateVals(secondVals) :: Nil
        val resultRow = mergeRows(combined)
        val result = getResult(resultRow)
        validateResult(expected, result)
      }
    }
  }

  private def validateResult(expected: T, result: T): Unit = {
    (expected, result) match {
      case (e: BigDecimal, r: BigDecimal) =>
        // BigDecimal.equals() value and scale but we are only interested in value.
        assert(e.compareTo(r) == 0)
      case _ =>
        assertEquals(expected, result)
    }
  }

  private def getResult(resultRow: Row): T = {
    val accumulator = resultRow.getField(offset).asInstanceOf[Accumulator]
    aggregator.getValue(accumulator)
  }

  private def aggregateVals(vals: Seq[_]): Row = {
    val accumulator = aggregator.createAccumulator()
    vals.foreach(v => aggregator.accumulate(accumulator, v))

    val row = new Row(rowArity)
    row.setField(offset, accumulator)
    row
  }

  private def mergeRows(rows: Seq[Row]): Row = {
    var accumulator = aggregator.createAccumulator()
    rows.foreach(
      row => {
        val acc = row.getField(offset).asInstanceOf[Accumulator]
        accumulator = aggregator.merge(accumulator, acc)
      }
    )
    val resultRow = new Row(rowArity)
    resultRow.setField(offset, accumulator)
    resultRow
  }
}
