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
package org.apache.flink.table.functions.aggfunctions

import java.math.BigDecimal
import java.util.{ArrayList => JArrayList, List => JList}
import org.apache.flink.table.functions.{Accumulator, AggregateFunction}
import org.apache.flink.table.functions.utils.UserDefinedFunctionUtils._
import org.junit.Assert.assertEquals
import org.junit.Test

/**
  * Base class for aggregate function test
  *
  * @tparam T the type for the aggregation result
  */
abstract class AggFunctionTestBase[T] {
  def inputValueSets: Seq[Seq[_]]

  def expectedResults: Seq[T]

  def aggregator: AggregateFunction[T]

  def supportRetraction: Boolean = true

  @Test
  // test aggregate and retract functions without partial merge
  def testAccumulateAndRetractWithoutMerge(): Unit = {
    // iterate over input sets
    for ((vals, expected) <- inputValueSets.zip(expectedResults)) {
      val accumulator = accumulateVals(vals)
      var result = aggregator.getValue(accumulator)
      validateResult[T](expected, result)

      if (supportRetraction) {
        retractVals(accumulator, vals)
        val expectedAccum = aggregator.createAccumulator()
        //The two accumulators should be exactly same
        validateResult[Accumulator](expectedAccum, accumulator)
      }
    }
  }

  @Test
  // test aggregate functions with partial merge
  def testAggregateWithMerge(): Unit = {

    if (ifMethodExistInFunction("merge", aggregator)) {
      // iterate over input sets
      for ((vals, expected) <- inputValueSets.zip(expectedResults)) {
        //equally split the vals sequence into two sequences
        val (firstVals, secondVals) = vals.splitAt(vals.length / 2)

        //1. verify merge with accumulate
        val accumulators: JList[Accumulator] = new JArrayList[Accumulator]()
        accumulators.add(accumulateVals(firstVals))
        accumulators.add(accumulateVals(secondVals))

        var accumulator = aggregator.merge(accumulators)
        val result = aggregator.getValue(accumulator)
        validateResult[T](expected, result)

        //2. verify merge with accumulate & retract
        if (supportRetraction) {
          retractVals(accumulator, vals)
          val expectedAccum = aggregator.createAccumulator()
          //The two accumulators should be exactly same
          validateResult[Accumulator](expectedAccum, accumulator)
        }
      }

      // iterate over input sets
      for ((vals, expected) <- inputValueSets.zip(expectedResults)) {
        //3. test partial merge with an empty accumulator
        val accumulators: JList[Accumulator] = new JArrayList[Accumulator]()
        accumulators.add(accumulateVals(vals))
        accumulators.add(aggregator.createAccumulator())

        val accumulator = aggregator.merge(accumulators)
        val result = aggregator.getValue(accumulator)
        validateResult[T](expected, result)
      }
    }
  }

  @Test
  // test aggregate functions with resetAccumulator
  def testResetAccumulator(): Unit = {

    if (ifMethodExistInFunction("resetAccumulator", aggregator)) {
      // iterate over input sets
      for ((vals, expected) <- inputValueSets.zip(expectedResults)) {
        val accumulator = accumulateVals(vals)
        aggregator.resetAccumulator(accumulator)
        val expectedAccum = aggregator.createAccumulator()
        //The accumulator after reset should be exactly same as the new accumulator
        validateResult[Accumulator](expectedAccum, accumulator)
      }
    }
  }

  private def validateResult[T](expected: T, result: T): Unit = {
    (expected, result) match {
      case (e: DecimalSumWithRetractAccumulator, r: DecimalSumWithRetractAccumulator) =>
        // BigDecimal.equals() value and scale but we are only interested in value.
        assert(e.f0.compareTo(r.f0) == 0 && e.f1 == r.f1)
      case (e: DecimalAvgAccumulator, r: DecimalAvgAccumulator) =>
        // BigDecimal.equals() value and scale but we are only interested in value.
        assert(e.f0.compareTo(r.f0) == 0 && e.f1 == r.f1)
      case (e: BigDecimal, r: BigDecimal) =>
        // BigDecimal.equals() value and scale but we are only interested in value.
        assert(e.compareTo(r) == 0)
      case _ =>
        assertEquals(expected, result)
    }
  }

  private def accumulateVals(vals: Seq[_]): Accumulator = {
    val accumulator = aggregator.createAccumulator()
    vals.foreach(v => aggregator.accumulate(accumulator, v))
    accumulator
  }

  private def retractVals(accumulator:Accumulator, vals: Seq[_]) = {
    vals.foreach(v => aggregator.retract(accumulator, v))
  }
}
