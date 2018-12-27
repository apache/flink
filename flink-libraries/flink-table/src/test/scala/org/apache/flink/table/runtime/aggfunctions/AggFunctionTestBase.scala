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

package org.apache.flink.table.runtime.aggfunctions

import java.lang.reflect.Method
import java.math.BigDecimal
import java.util.{ArrayList => JArrayList, List => JList}

import org.apache.flink.table.functions.AggregateFunction
import org.apache.flink.table.functions.aggfunctions.{DecimalAvgAccumulator, DecimalSumWithRetractAccumulator, MaxWithRetractAccumulator, MinWithRetractAccumulator}
import org.apache.flink.table.functions.utils.UserDefinedFunctionUtils._
import org.junit.Assert.assertEquals
import org.junit.Test

/**
  * Base class for aggregate function test
  *
  * @tparam T the type for the aggregation result
  */
abstract class AggFunctionTestBase[T, ACC] {
  def inputValueSets: Seq[Seq[_]]

  def expectedResults: Seq[T]

  def aggregator: AggregateFunction[T, ACC]

  val accType = aggregator.getClass.getMethod("createAccumulator").getReturnType

  def accumulateFunc: Method = aggregator.getClass.getMethod("accumulate", accType, classOf[Any])

  def retractFunc: Method = null

  @Test
  // test aggregate and retract functions without partial merge
  def testAccumulateAndRetractWithoutMerge(): Unit = {
    // iterate over input sets
    for ((vals, expected) <- inputValueSets.zip(expectedResults)) {
      val accumulator = accumulateVals(vals)
      val result = aggregator.getValue(accumulator)
      validateResult[T](expected, result)

      if (ifMethodExistInFunction("retract", aggregator)) {
        retractVals(accumulator, vals)
        val expectedAccum = aggregator.createAccumulator()
        //The two accumulators should be exactly same
        validateResult[ACC](expectedAccum, accumulator)
      }
    }
  }

  @Test
  def testAggregateWithMerge(): Unit = {

    if (ifMethodExistInFunction("merge", aggregator)) {
      val mergeFunc =
        aggregator.getClass.getMethod("merge", accType, classOf[java.lang.Iterable[ACC]])
      // iterate over input sets
      for ((vals, expected) <- inputValueSets.zip(expectedResults)) {
        //equally split the vals sequence into two sequences
        val (firstVals, secondVals) = vals.splitAt(vals.length / 2)

        //1. verify merge with accumulate
        val accumulators: JList[ACC] = new JArrayList[ACC]()
        accumulators.add(accumulateVals(secondVals))

        val acc = accumulateVals(firstVals)

        mergeFunc.invoke(aggregator, acc.asInstanceOf[Object], accumulators)
        val result = aggregator.getValue(acc)
        validateResult[T](expected, result)

        //2. verify merge with accumulate & retract
        if (ifMethodExistInFunction("retract", aggregator)) {
          retractVals(acc, vals)
          val expectedAccum = aggregator.createAccumulator()
          //The two accumulators should be exactly same
          validateResult[ACC](expectedAccum, acc)
        }
      }

      // iterate over input sets
      for ((vals, expected) <- inputValueSets.zip(expectedResults)) {
        //3. test partial merge with an empty accumulator
        val accumulators: JList[ACC] = new JArrayList[ACC]()
        accumulators.add(aggregator.createAccumulator())

        val acc = accumulateVals(vals)

        mergeFunc.invoke(aggregator, acc.asInstanceOf[Object], accumulators)
        val result = aggregator.getValue(acc)
        validateResult[T](expected, result)
      }
    }
  }

  @Test
  // test aggregate functions with resetAccumulator
  def testResetAccumulator(): Unit = {

    if (ifMethodExistInFunction("resetAccumulator", aggregator)) {
      val resetAccFunc = aggregator.getClass.getMethod("resetAccumulator", accType)
      // iterate over input sets
      for ((vals, expected) <- inputValueSets.zip(expectedResults)) {
        val accumulator = accumulateVals(vals)
        resetAccFunc.invoke(aggregator, accumulator.asInstanceOf[Object])
        val expectedAccum = aggregator.createAccumulator()
        //The accumulator after reset should be exactly same as the new accumulator
        validateResult[ACC](expectedAccum, accumulator)
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
      case (e: MinWithRetractAccumulator[_], r: MinWithRetractAccumulator[_]) =>
        assertEquals(e.min, r.min)
        assertEquals(e.distinctCount, r.distinctCount)
      case (e: MaxWithRetractAccumulator[_], r: MaxWithRetractAccumulator[_]) =>
        assertEquals(e.max, r.max)
        assertEquals(e.distinctCount, r.distinctCount)
      case _ =>
        assertEquals(expected, result)
    }
  }

  private def accumulateVals(vals: Seq[_]): ACC = {
    val accumulator = aggregator.createAccumulator()
    vals.foreach(
      v =>
        if (accumulateFunc.getParameterCount == 1) {
          this.accumulateFunc.invoke(aggregator, accumulator.asInstanceOf[Object])
        } else {
          this.accumulateFunc.invoke(
            aggregator,
            accumulator.asInstanceOf[Object],
            v.asInstanceOf[Object])
        }
    )
    accumulator
  }

  private def retractVals(accumulator: ACC, vals: Seq[_]) = {
    vals.foreach(
      v =>
        if (retractFunc.getParameterCount == 1) {
          this.retractFunc.invoke(
            aggregator,
            accumulator.asInstanceOf[Object])
        } else {
          this.retractFunc.invoke(
            aggregator,
            accumulator.asInstanceOf[Object],
            v.asInstanceOf[Object])
        }
    )
  }
}
