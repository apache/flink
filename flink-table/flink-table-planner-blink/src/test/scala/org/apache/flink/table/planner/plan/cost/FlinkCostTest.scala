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

package org.apache.flink.table.planner.plan.cost

import org.apache.calcite.plan.RelOptCostImpl
import org.junit.Assert._
import org.junit.Test

class FlinkCostTest {

  @Test
  def testIsInfinite(): Unit = {
    assertFalse(FlinkCost.Zero.isInfinite)
    assertFalse(FlinkCost.Tiny.isInfinite)
    assertFalse(FlinkCost.Huge.isInfinite)
    assertTrue(FlinkCost.Infinity.isInfinite)
    assertTrue(new FlinkCost(100.0, 1000.0, Double.PositiveInfinity, 0.0, 0.0).isInfinite)
  }

  @Test
  def testEquals(): Unit = {
    assertTrue(FlinkCost.Tiny.equals(FlinkCost.Tiny))
    assertFalse(FlinkCost.Tiny.equals(RelOptCostImpl.FACTORY.makeTinyCost()))

    val cost1 = FlinkCost.FACTORY.makeCost(100.0, 1000.0, 0.0, 500.0, 0.0)
    val cost2 = FlinkCost.FACTORY.makeCost(100.0, 1000.0, 0.0, 500.0, 0.0)
    assertTrue(cost1.equals(cost2))

    val cost3 = FlinkCost.FACTORY.makeCost(100.0, 1000.0, Double.PositiveInfinity, 0.0, 0.0)
    val cost4 = FlinkCost.FACTORY.makeCost(100.0, 1000.0, Double.PositiveInfinity, 0.0, 0.0)
    assertTrue(cost3.equals(cost4))

    val cost5 = new FlinkCost(Double.PositiveInfinity, Double.PositiveInfinity,
      Double.PositiveInfinity, Double.PositiveInfinity, Double.PositiveInfinity)
    assertTrue(FlinkCost.Infinity.equals(cost5))

    val cost6 = FlinkCost.FACTORY.makeCost(100.0, 1000.0, 0.0, 500.0, 1.0E-6)
    assertFalse(cost1.equals(cost6))
  }

  @Test
  def testIsEqWithEpsilon(): Unit = {
    assertTrue(FlinkCost.Tiny.isEqWithEpsilon(FlinkCost.Tiny))
    assertFalse(FlinkCost.Tiny.isEqWithEpsilon(RelOptCostImpl.FACTORY.makeTinyCost()))

    val cost1 = FlinkCost.FACTORY.makeCost(100.123456, 1000.12345, 1.0E-6, 500.123, 1.0E-7)
    val cost2 = FlinkCost.FACTORY.makeCost(100.123457, 1000.12346, 1.0E-7, 500.123, 1.0E-6)
    assertTrue(cost1.isEqWithEpsilon(cost2))

    val cost3 = FlinkCost.FACTORY.makeCost(100.123456, 1000.12347, 1.0E-6, 500.123, 1.0E-7)
    assertFalse(cost1.isEqWithEpsilon(cost3))
  }

  @Test
  def testIsLe(): Unit = {
    assertTrue(FlinkCost.Tiny.isLe(FlinkCost.Tiny))

    val cost1 = FlinkCost.FACTORY.makeCost(100.0, 1000.0, 0.0, 500.0, 0.0)
    val cost2 = FlinkCost.FACTORY.makeCost(100.0, 1000.0, 0.0, 500.0, 0.0)
    assertTrue(cost1.isLe(cost2))
    assertTrue(cost2.isLe(cost1))

    val cost3 = FlinkCost.FACTORY.makeCost(100.0, 1200.0, 0.0, 500.0, 0.0)
    assertTrue(cost1.isLe(cost3))
    assertFalse(cost3.isLe(cost1))

    val cost4 = FlinkCost.FACTORY.makeCost(100.0, 1000.0, 50.0, 300.0, 200.0)
    assertFalse(cost1.isLe(cost4))
    assertTrue(cost4.isLe(cost1))
  }

  @Test(expected = classOf[ClassCastException])
  def testIsLe_WithDiffCost(): Unit = {
    FlinkCost.Tiny.isLe(RelOptCostImpl.FACTORY.makeTinyCost())
  }

  @Test
  def testIsLt(): Unit = {
    assertFalse(FlinkCost.Tiny.isLt(FlinkCost.Tiny))

    val cost1 = FlinkCost.FACTORY.makeCost(100.0, 1000.0, 0.0, 500.0, 0.0)
    val cost2 = FlinkCost.FACTORY.makeCost(100.0, 1000.0, 0.0, 500.0, 0.0)
    assertFalse(cost1.isLt(cost2))
    assertFalse(cost2.isLt(cost1))

    val cost3 = FlinkCost.FACTORY.makeCost(100.0, 1200.0, 0.0, 500.0, 0.0)
    assertTrue(cost1.isLt(cost3))
    assertFalse(cost3.isLt(cost1))

    val cost4 = FlinkCost.FACTORY.makeCost(100.0, 1000.0, 50.0, 300.0, 200.0)
    assertFalse(cost1.isLt(cost4))
    assertTrue(cost4.isLt(cost1))
  }

  @Test(expected = classOf[ClassCastException])
  def testIsLt_WithDiffCost(): Unit = {
    FlinkCost.Tiny.isLt(RelOptCostImpl.FACTORY.makeTinyCost())
  }

  @Test
  def testPlus(): Unit = {
    val cost1 = FlinkCost.FACTORY.makeCost(100.0, 1000.0, 0.0, 500.0, 0.0)
    assertTrue(FlinkCost.Infinity.equals(cost1.plus(FlinkCost.Infinity)))
    assertTrue(FlinkCost.Infinity.equals(FlinkCost.Infinity.plus(cost1)))

    val cost2 = FlinkCost.FACTORY.makeCost(500.0, 3000.0, 27.0, 100.1234567, 0.0)
    val expectedCost1 = FlinkCost.FACTORY.makeCost(600.0, 4000.0, 27.0, 600.1234567, 0.0)
    assertTrue(expectedCost1.equals(cost1.plus(cost2)))

    val cost3 = FlinkCost.FACTORY.makeCost(
      500.0, 3000.0, 27.0, 100.1234567, Double.PositiveInfinity)
    val expectedCost2 = FlinkCost.FACTORY.makeCost(
      600.0, 4000.0, 27.0, 600.1234567, Double.PositiveInfinity)
    assertTrue(expectedCost2.equals(cost1.plus(cost3)))
  }

  @Test(expected = classOf[ClassCastException])
  def testPlus_WithDiffCost(): Unit = {
    FlinkCost.Tiny.plus(RelOptCostImpl.FACTORY.makeTinyCost())
  }

  @Test
  def testMinus(): Unit = {
    val cost1 = FlinkCost.FACTORY.makeCost(100.0, 1000.0, 0.0, 500.0, 0.0)
    assertTrue(FlinkCost.Infinity.equals(FlinkCost.Infinity.minus(cost1)))

    val expectedCost1 = FlinkCost.FACTORY.makeCost(
      Double.NegativeInfinity, Double.NegativeInfinity, Double.NegativeInfinity,
      Double.NegativeInfinity, Double.NegativeInfinity)
    assertTrue(expectedCost1.equals(cost1.minus(FlinkCost.Infinity)))

    val cost2 = FlinkCost.FACTORY.makeCost(500.0, 3000.0, 27.0, 100.1234567, 0.0)
    val expectedCost2 = FlinkCost.FACTORY.makeCost(-400.0, -2000.0, -27.0, 399.8765433, 0.0)
    assertTrue(expectedCost2.equals(cost1.minus(cost2)))

    val expectedCost3 = FlinkCost.FACTORY.makeCost(400.0, 2000.0, 27.0, -399.8765433, 0.0)
    assertTrue(expectedCost3.equals(cost2.minus(cost1)))
  }

  @Test(expected = classOf[ClassCastException])
  def testMinus_WithDiffCost(): Unit = {
    FlinkCost.Tiny.minus(RelOptCostImpl.FACTORY.makeTinyCost())
  }

  @Test
  def testMultiplyBy(): Unit = {
    assertTrue(FlinkCost.Infinity.equals(FlinkCost.Infinity.multiplyBy(10.0)))

    val cost1 = FlinkCost.FACTORY.makeCost(100.0, 1000.0, 0.0, 500.0, 0.0)
    val expectedCost1 = FlinkCost.FACTORY.makeCost(1000.0, 10000.0, 0.0, 5000.0, 0.0)
    assertTrue(expectedCost1.equals(cost1.multiplyBy(10.0)))

    val cost2 = FlinkCost.FACTORY.makeCost(100.0, 1000.0, 0.0, 500.0, Double.PositiveInfinity)
    val expectedCost2 = FlinkCost.FACTORY.makeCost(
      1000.0, 10000.0, 0.0, 5000.0, Double.PositiveInfinity)
    assertTrue(expectedCost2.equals(cost2.multiplyBy(10.0)))
  }

  @Test
  def testDivideBy(): Unit = {
    assertEquals(1.0, FlinkCost.Infinity.divideBy(FlinkCost.Infinity), 1e-5)
    assertEquals(1.0, FlinkCost.Tiny.divideBy(FlinkCost.Infinity), 1e-5)
    assertEquals(1.0, FlinkCost.Tiny.divideBy(FlinkCost.Zero), 1e-5)

    val cost1 = FlinkCost.FACTORY.makeCost(100.0, 1000.0, 200.0, 500.0, 600.0)
    val cost2 = FlinkCost.FACTORY.makeCost(50.0, 100.0, 40.0, 25.0, 200.0)
    assertEquals(Math.pow(6000.0, 1 / 5.0), cost1.divideBy(cost2), 1e-5)
    assertEquals(Math.pow(1 / 6000.0, 1 / 5.0), cost2.divideBy(cost1), 1e-5)
  }

}
