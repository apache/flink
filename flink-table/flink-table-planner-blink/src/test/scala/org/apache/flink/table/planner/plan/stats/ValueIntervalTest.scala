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

package org.apache.flink.table.planner.plan.stats

import org.apache.flink.table.planner.plan.stats.ValueInterval._
import org.apache.flink.table.planner.plan.utils.ColumnIntervalUtil.toBigDecimalInterval

import org.junit.Assert._
import org.junit.Test

class ValueIntervalTest {

  @Test
  def testUnion(): Unit = {
    // empty union empty = empty
    assertEquals(empty, union(empty, empty))
    // infinity union infinity = infinity
    assertEquals(infinite, union(infinite, infinite))
    // empty union [1, 3] = [1, 3]
    val interval1 = ValueInterval(1, 3)
    assertEquals(interval1, union(empty, interval1))
    assertEquals(interval1, union(interval1, empty))
    // infinity union [1,3] = infinity
    assertEquals(infinite, union(infinite, interval1))
    assertEquals(infinite, union(interval1, infinite))
    // [1, 3] union (-1, 2) = (-1, 3]
    assertEquals(
      ValueInterval(-1, 3, includeLower = false, includeUpper = true),
      union(interval1, ValueInterval(-1, 2, includeLower = false, includeUpper = true))
    )
    // [1, 3] union [-1, 4] = [-1, 4]
    assertEquals(ValueInterval(-1, 4), union(interval1, ValueInterval(-1, 4)))
    // [1, 3] union [-3, -2) = [-3, 3]
    assertEquals(
      ValueInterval(-3, 3),
      union(interval1, ValueInterval(-3, -2, includeLower = true, includeUpper = false)))
    // [1, 3] union (0, 3) = (0, 3]
    assertEquals(
      ValueInterval(0, 3, includeLower = false, includeUpper = true),
      union(interval1, ValueInterval(0, 3, includeLower = false, includeUpper = false)))
    // [1, 3] union (0, 4] = (0, 4]
    assertEquals(
      ValueInterval(0, 4, includeLower = false, includeUpper = true),
      union(interval1, ValueInterval(0, 4, includeLower = false, includeUpper = true)))
    // [1, 3] union [4, 7] = [1,7]
    assertEquals(ValueInterval(1, 7), union(interval1, ValueInterval(4, 7)))
    // [1, 3] union (-Inf, -2) = (-Inf, 3]
    assertEquals(
      ValueInterval(null, 3),
      union(interval1, ValueInterval(null, -2, includeUpper = false)))
    assertEquals(
      ValueInterval(null, 3),
      union(ValueInterval(null, -2, includeUpper = false), interval1))
    // [1, 3] union (-Inf, 3) = (-Inf, 3]
    assertEquals(
      ValueInterval(null, 3),
      union(interval1, ValueInterval(null, 3, includeUpper = false)))
    assertEquals(
      ValueInterval(null, 3),
      union(ValueInterval(null, 3, includeUpper = false), interval1))
    // [1, 3] union (-Inf, 4] = (-Inf, 4]
    assertEquals(ValueInterval(null, 4), union(interval1, ValueInterval(null, 4)))
    assertEquals(ValueInterval(null, 4), union(ValueInterval(null, 4), interval1))
    // [1, 3] union [-1, Inf) = [-1, Inf)
    assertEquals(ValueInterval(-1, null), union(interval1, ValueInterval(-1, null)))
    assertEquals(ValueInterval(-1, null), union(ValueInterval(-1, null), interval1))
    // [1, 3] union [0, Inf) = [0, Inf)
    assertEquals(ValueInterval(0, null), union(interval1, ValueInterval(0, null)))
    assertEquals(ValueInterval(0, null), union(ValueInterval(0, null), interval1))
    // [1, 3] union [4, Inf) = [1, Inf)
    assertEquals(ValueInterval(1, null), union(interval1, ValueInterval(4, null)))
    assertEquals(ValueInterval(1, null), union(ValueInterval(4, null), interval1))
    val interval2 = ValueInterval(null, 2)
    // (-Inf, 2] union [3, Inf) = infinity
    assertEquals(infinite, union(interval2, ValueInterval(3, null)))
    assertEquals(infinite, union(ValueInterval(3, null), interval2))
    // (-Inf, 2] union [-1, Inf) = infinity
    assertEquals(infinite, union(interval2, ValueInterval(-1, null)))
    assertEquals(infinite, union(ValueInterval(-1, null), interval2))
    // (-Inf, 2] union (-Inf, 1) = (-Inf, 2]
    assertEquals(interval2, union(interval2, ValueInterval(null, 1, includeUpper = false)))
    // (-Inf, 2] union (-Inf, 3) = (-Inf, 3)
    assertEquals(
      ValueInterval(null, 3, includeUpper = false),
      union(interval2, ValueInterval(null, 3, includeUpper = false)))
    val interval3 = ValueInterval(3, null)
    // [3, Inf) union (4, Inf) = [3, Inf)
    assertEquals(interval3, union(interval3, ValueInterval(4, null, includeLower = false)))
    // [3, Inf) union (-1, Inf) = (-1, Inf)
    assertEquals(
      ValueInterval(-1, null, includeLower = false),
      union(interval3, ValueInterval(-1, null, includeLower = false))
    )
  }

  @Test
  def testUnionUncompatibility(): Unit = {
    assertEquals(
      toBigDecimalInterval(ValueInterval(1, 2)),
      toBigDecimalInterval(
        union(ValueInterval(1L, 2L),
          ValueInterval(1.2D, 2.0D)
        )
      )
    )
  }

  @Test
  def testIsIntersected(): Unit = {
    // empty not intersect empty
    assertFalse(isIntersected(empty, empty))
    // infinity intersect infinity
    assertTrue(isIntersected(infinite, infinite))
    val interval1 = ValueInterval(1, 3)
    // empty not intersect [1, 3]
    assertFalse(isIntersected(empty, interval1))
    assertFalse(isIntersected(interval1, empty))
    // infinity intersect [1,3]
    assertTrue(isIntersected(infinite, interval1))
    assertTrue(isIntersected(interval1, infinite))
    // [1, 3] intersect (-1, 2)
    assertTrue(
      isIntersected(interval1, ValueInterval(-1, 2, includeLower = false, includeUpper = false)))
    // [1, 3] intersect [-1, 4]
    assertTrue(isIntersected(interval1, ValueInterval(-1, 4)))
    // [1, 3] not intersect [-3, -2)
    assertFalse(
      isIntersected(interval1, ValueInterval(-3, -2, includeLower = true, includeUpper = false)))
    // [1, 3] intersect (0, 3)
    assertTrue(isIntersected(interval1, ValueInterval(0, 3, includeLower = false, includeUpper =
      false)))
    // [1, 3] intersect (0, 4]
    assertTrue(
      isIntersected(interval1, ValueInterval(0, 4, includeLower = false, includeUpper = true)))
    // [1, 3] not intersect [4, 7]
    assertFalse(isIntersected(interval1, ValueInterval(4, 7)))
    // [1, 3) not intersect [3, 3]
    assertFalse(isIntersected(
      ValueInterval(1, 3, includeLower = true, includeUpper = false), ValueInterval(3, 3)))
    // [1, 3] not intersect (-Inf, -2]
    assertFalse(isIntersected(interval1, ValueInterval(null, -2)))
    assertFalse(isIntersected(ValueInterval(null, -2), interval1))
    // [1, 3] intersect (-Inf, 3)
    assertTrue(isIntersected(interval1, ValueInterval(null, 3, includeUpper = false)))
    assertTrue(isIntersected(ValueInterval(null, 3, includeUpper = false), interval1))
    // [1, 3] intersect (-Inf, 4]
    assertTrue(isIntersected(interval1, ValueInterval(null, 4)))
    assertTrue(isIntersected(ValueInterval(null, 4), interval1))
    // [1, 3] intersect [-1, Inf)
    assertTrue(isIntersected(interval1, ValueInterval(-1, null)))
    assertTrue(isIntersected(ValueInterval(-1, null), interval1))
    // [1, 3] intersect [0, Inf)
    assertTrue(isIntersected(interval1, ValueInterval(0, null)))
    assertTrue(isIntersected(ValueInterval(0, null), interval1))
    // [1, 3] not intersect [4, Inf)
    assertFalse(isIntersected(interval1, ValueInterval(4, null)))
    assertFalse(isIntersected(ValueInterval(4, null), interval1))
    // [1, 3] not intersect (3, Inf)
    assertFalse(isIntersected(interval1, ValueInterval(3, null, includeLower = false)))
    assertFalse(isIntersected(ValueInterval(3, null, includeLower = false), interval1))
    val interval2 = ValueInterval(null, 2)
    // (-Inf, 2] not intersect [3, Inf)
    assertFalse(isIntersected(interval2, ValueInterval(3, null)))
    assertFalse(isIntersected(ValueInterval(3, null), interval2))
    // (-Inf, 2] intersect [-1, Inf)
    assertTrue(isIntersected(interval2, ValueInterval(-1, null)))
    assertTrue(isIntersected(ValueInterval(-1, null), interval2))
    // (-Inf, 2] not intersect (2, Inf)
    assertFalse(isIntersected(interval2, ValueInterval(2, null, includeLower = false)))
    assertFalse(isIntersected(ValueInterval(2, null, includeLower = false), interval2))
    // (-Inf, 2] intersect [2, Inf)
    assertTrue(isIntersected(interval2, ValueInterval(2, null)))
    assertTrue(isIntersected(ValueInterval(2, null), interval2))
    // (-Inf, 2] intersect (-Inf, 1)
    assertTrue(isIntersected(interval2, ValueInterval(null, 1, includeUpper = false)))
    // (-Inf, 2] intersect (-Inf, 3)
    assertTrue(isIntersected(interval2, ValueInterval(null, 3, includeUpper = false)))
    // [3, Inf) intersect [4, Inf)
    assertTrue(isIntersected(ValueInterval(3, null), ValueInterval(4, null)))
    // [3, Inf) intersect (-1, Inf)
    assertTrue(isIntersected(ValueInterval(3, null), ValueInterval(-1, null, includeLower = false)))
    // [1, 5] intersect [2.0, 3.0]
    assertTrue(isIntersected(ValueInterval(1, 5), ValueInterval(2.0D, 3.0D)))
  }

  @Test
  def testIntersect(): Unit = {
    // empty intersect empty = empty
    assertEquals(empty, intersect(empty, empty))
    // infinity intersect infinity = infinity
    assertEquals(infinite, intersect(infinite, infinite))
    val interval1 = ValueInterval(1, 3)
    // empty intersect [1, 3] = empty
    assertEquals(empty, intersect(empty, interval1))
    assertEquals(empty, intersect(interval1, empty))
    // infinity intersect [1,3] = [1,3]
    assertEquals(interval1, intersect(infinite, interval1))
    assertEquals(interval1, intersect(interval1, infinite))
    // [1, 3] intersect (-1, 2) = [1, 2)
    assertEquals(
      ValueInterval(1, 2, includeLower = true, includeUpper = false),
      intersect(interval1, ValueInterval(-1, 2, includeLower = false, includeUpper = false)))
    // [1, 3] intersect [-1, 4] = [1, 3]
    assertEquals(interval1, intersect(interval1, ValueInterval(-1, 4)))
    // [1, 3] intersect [-3, -2) = empty
    assertEquals(
      empty,
      intersect(interval1, ValueInterval(-3, -2, includeLower = true, includeUpper = false)))
    // [1, 3] intersect (0, 3) = [1, 3)
    assertEquals(
      ValueInterval(1, 3, includeLower = true, includeUpper = false),
      intersect(interval1, ValueInterval(0, 3, includeLower = false, includeUpper = false)))
    // [1, 3] intersect (0, 4] = [1, 3]
    assertEquals(
      interval1,
      intersect(interval1, ValueInterval(0, 4, includeLower = false, includeUpper = true)))
    // [1, 3] intersect [4, 7] = empty
    assertEquals(empty, intersect(interval1, ValueInterval(4, 7)))
    // [1, 3) intersect [3, 3] = empty
    assertEquals(
      empty,
      intersect(ValueInterval(1, 3, includeLower = true, includeUpper = false),
        ValueInterval(3, 3)))
    // [1, 3] intersect (-Inf, -2] = empty
    assertEquals(empty, intersect(interval1, ValueInterval(null, -2)))
    assertEquals(empty, intersect(ValueInterval(null, -2), interval1))
    // [1, 3] intersect (-Inf, 3) = [1, 3)
    assertEquals(
      ValueInterval(1, 3, includeLower = true, includeUpper = false),
      intersect(interval1, ValueInterval(null, 3, includeUpper = false)))
    assertEquals(
      ValueInterval(1, 3, includeLower = true, includeUpper = false),
      intersect(ValueInterval(null, 3, includeUpper = false), interval1))
    // [1, 3] intersect (-Inf, 4] = [1, 3]
    assertEquals(interval1, intersect(interval1, ValueInterval(null, 4)))
    assertEquals(interval1, intersect(ValueInterval(null, 4), interval1))
    // [1, 3] intersect [-1, Inf) = [1, 3]
    assertEquals(interval1, intersect(interval1, ValueInterval(-1, null)))
    assertEquals(interval1, intersect(ValueInterval(-1, null), interval1))
    // [1, 3] intersect [0, Inf) = [1, 3]
    assertEquals(interval1, intersect(interval1, ValueInterval(0, null)))
    assertEquals(interval1, intersect(ValueInterval(0, null), interval1))
    // [1, 3] intersect [4, Inf) = empty
    assertEquals(empty, intersect(interval1, ValueInterval(4, null)))
    assertEquals(empty, intersect(ValueInterval(4, null), interval1))
    // [1, 3] intersect (3, Inf) = empty
    assertEquals(empty, intersect(interval1, ValueInterval(3, null, includeLower = false)))
    assertEquals(empty, intersect(ValueInterval(3, null, includeLower = false), interval1))
    val interval2 = ValueInterval(null, 2)
    // (-Inf, 2] intersect [3, Inf) = empty
    assertEquals(empty, intersect(interval2, ValueInterval(3, null)))
    assertEquals(empty, intersect(ValueInterval(3, null), interval2))
    // (-Inf, 2] intersect [-1, Inf) = [-1, 2]
    assertEquals(ValueInterval(-1, 2), intersect(interval2, ValueInterval(-1, null)))
    assertEquals(ValueInterval(-1, 2), intersect(ValueInterval(-1, null), interval2))
    // (-Inf, 2] intersect (2, Inf) = empty
    assertEquals(empty, intersect(interval2, ValueInterval(2, null, includeLower = false)))
    assertEquals(empty, intersect(ValueInterval(2, null, includeLower = false), interval2))
    // (-Inf, 2] intersect [2, Inf) = [2, 2]
    assertEquals(ValueInterval(2, 2), intersect(interval2, ValueInterval(2, null)))
    assertEquals(ValueInterval(2, 2), intersect(ValueInterval(2, null), interval2))
    // (-Inf, 2] intersect (-Inf, 1) = (-Inf, 1)
    assertEquals(
      ValueInterval(null, 1, includeUpper = false),
      intersect(interval2, ValueInterval(null, 1, includeUpper = false)))
    // (-Inf, 2] intersect (-Inf, 3) = (-Inf, 2]
    assertEquals(interval2, intersect(interval2, ValueInterval(null, 3, includeUpper = false)))
    // [3, Inf) intersect [4, Inf) = [4, Inf)
    assertEquals(
      ValueInterval(4, null),
      intersect(ValueInterval(3, null), ValueInterval(4, null)))
    // [3, Inf) intersect (-1, Inf) = [3, Inf)
    assertEquals(
      ValueInterval(3, null),
      intersect(ValueInterval(3, null), ValueInterval(-1, null, includeLower = false)))
  }

  @Test
  def testContains(): Unit = {
    // EmptyValueInterval
    assertFalse(contains(empty, null))
    assertFalse(contains(empty, 0))
    assertFalse(contains(empty, 1.0))
    assertFalse(contains(empty, "abc"))

    // InfiniteValueInterval
    assertFalse(contains(infinite, null))
    assertTrue(contains(infinite, 0))
    assertTrue(contains(infinite, 1.0))
    assertTrue(contains(infinite, "abc"))

    // LeftSemiInfiniteValueInterval
    // int type
    val intInterval1 = ValueInterval(null, 100, includeLower = false, includeUpper = true)
    assertFalse(contains(intInterval1, null))
    assertTrue(contains(intInterval1, 99))
    assertTrue(contains(intInterval1, 100))
    assertFalse(contains(intInterval1, 101))
    val intInterval2 = ValueInterval(null, 100, includeLower = false, includeUpper = false)
    assertFalse(contains(intInterval2, null))
    assertTrue(contains(intInterval2, 99))
    assertFalse(contains(intInterval2, 100))
    assertFalse(contains(intInterval2, 101))
    // string type
    val strInterval1 = ValueInterval(null, "m", includeLower = false, includeUpper = true)
    assertFalse(contains(strInterval1, null))
    assertTrue(contains(strInterval1, "l"))
    assertTrue(contains(strInterval1, "m"))
    assertFalse(contains(strInterval1, "n"))
    val strInterval2 = ValueInterval(null, "m", includeLower = false, includeUpper = false)
    assertFalse(contains(strInterval2, null))
    assertTrue(contains(strInterval2, "l"))
    assertFalse(contains(strInterval2, "m"))
    assertFalse(contains(strInterval2, "n"))

    // RightSemiInfiniteValueInterval
    // int type
    val intInterval3 = ValueInterval(100, null, includeLower = true, includeUpper = false)
    assertFalse(contains(intInterval3, null))
    assertFalse(contains(intInterval3, 99))
    assertTrue(contains(intInterval3, 100))
    assertTrue(contains(intInterval3, 101))
    val intInterval4 = ValueInterval(100, null, includeLower = false, includeUpper = false)
    assertFalse(contains(intInterval4, null))
    assertFalse(contains(intInterval4, 99))
    assertFalse(contains(intInterval4, 100))
    assertTrue(contains(intInterval4, 101))
    // string type
    val strInterval3 = ValueInterval("m", null, includeLower = true, includeUpper = false)
    assertFalse(contains(strInterval3, null))
    assertFalse(contains(strInterval3, "l"))
    assertTrue(contains(strInterval3, "m"))
    assertTrue(contains(strInterval3, "n"))
    val strInterval4 = ValueInterval("m", null, includeLower = false, includeUpper = false)
    assertFalse(contains(strInterval4, null))
    assertFalse(contains(strInterval4, "l"))
    assertFalse(contains(strInterval4, "m"))
    assertTrue(contains(strInterval4, "n"))

    // FiniteValueInterval
    // int type
    val intInterval5 = ValueInterval(100, 200, includeLower = true, includeUpper = true)
    assertFalse(contains(intInterval5, null))
    assertFalse(contains(intInterval5, 99))
    assertTrue(contains(intInterval5, 100))
    assertTrue(contains(intInterval5, 101))
    assertTrue(contains(intInterval5, 199))
    assertTrue(contains(intInterval5, 200))
    assertFalse(contains(intInterval5, 201))
    val intInterval6 = ValueInterval(100, 200, includeLower = false, includeUpper = true)
    assertFalse(contains(intInterval6, null))
    assertFalse(contains(intInterval6, 99))
    assertFalse(contains(intInterval6, 100))
    assertTrue(contains(intInterval6, 101))
    assertTrue(contains(intInterval6, 199))
    assertTrue(contains(intInterval6, 200))
    assertFalse(contains(intInterval6, 201))
    val intInterval7 = ValueInterval(100, 200, includeLower = true, includeUpper = false)
    assertFalse(contains(intInterval7, null))
    assertFalse(contains(intInterval7, 99))
    assertTrue(contains(intInterval7, 100))
    assertTrue(contains(intInterval7, 101))
    assertTrue(contains(intInterval7, 199))
    assertFalse(contains(intInterval7, 200))
    assertFalse(contains(intInterval7, 201))
    val intInterval8 = ValueInterval(100, 200, includeLower = false, includeUpper = false)
    assertFalse(contains(intInterval8, null))
    assertFalse(contains(intInterval8, 99))
    assertFalse(contains(intInterval8, 100))
    assertTrue(contains(intInterval8, 101))
    assertTrue(contains(intInterval8, 199))
    assertFalse(contains(intInterval8, 200))
    assertFalse(contains(intInterval8, 201))
    // string type
    val strInterval5 = ValueInterval("m", "s", includeLower = true, includeUpper = true)
    assertFalse(contains(strInterval5, null))
    assertFalse(contains(strInterval5, "l"))
    assertTrue(contains(strInterval5, "m"))
    assertTrue(contains(strInterval5, "n"))
    assertTrue(contains(strInterval5, "r"))
    assertTrue(contains(strInterval5, "s"))
    assertFalse(contains(strInterval5, "t"))
    val strInterval6 = ValueInterval("m", "s", includeLower = false, includeUpper = true)
    assertFalse(contains(strInterval6, null))
    assertFalse(contains(strInterval6, "l"))
    assertFalse(contains(strInterval6, "m"))
    assertTrue(contains(strInterval6, "n"))
    assertTrue(contains(strInterval6, "r"))
    assertTrue(contains(strInterval6, "s"))
    assertFalse(contains(strInterval6, "t"))
    val strInterval7 = ValueInterval("m", "s", includeLower = true, includeUpper = false)
    assertFalse(contains(strInterval7, null))
    assertFalse(contains(strInterval7, "l"))
    assertTrue(contains(strInterval7, "m"))
    assertTrue(contains(strInterval7, "n"))
    assertTrue(contains(strInterval7, "r"))
    assertFalse(contains(strInterval7, "s"))
    assertFalse(contains(strInterval7, "t"))
    val strInterval8 = ValueInterval("m", "s", includeLower = false, includeUpper = false)
    assertFalse(contains(strInterval8, null))
    assertFalse(contains(strInterval8, "l"))
    assertFalse(contains(strInterval8, "m"))
    assertTrue(contains(strInterval8, "n"))
    assertTrue(contains(strInterval8, "r"))
    assertFalse(contains(strInterval8, "s"))
    assertFalse(contains(strInterval8, "t"))
  }

  @Test
  def testContainsTypeNotMatch(): Unit = {
    // LeftSemiInfiniteValueInterval
    val intInterval1 = ValueInterval(null, 100, includeLower = false, includeUpper = true)
    assertTrue(contains(intInterval1, 1.0))
    testContainsTypeNotMatch(intInterval1, "a")
    val strInterval1 = ValueInterval(null, "m", includeLower = false, includeUpper = true)
    testContainsTypeNotMatch(strInterval1, 1.0)
    testContainsTypeNotMatch(strInterval1, 1)
    // RightSemiInfiniteValueInterval
    val intInterval2 = ValueInterval(100, null, includeLower = true, includeUpper = false)
    assertFalse(contains(intInterval2, 1.0))
    testContainsTypeNotMatch(intInterval2, "a")
    val strInterval2 = ValueInterval("m", null, includeLower = true, includeUpper = false)
    testContainsTypeNotMatch(strInterval2, 1.0)
    testContainsTypeNotMatch(strInterval2, 1)
    // FiniteValueInterval
    val intInterval3 = ValueInterval(100, 200, includeLower = true, includeUpper = true)
    assertFalse(contains(intInterval3, 1.0))
    testContainsTypeNotMatch(intInterval3, "a")
    val strInterval3 = ValueInterval("m", "s", includeLower = true, includeUpper = true)
    testContainsTypeNotMatch(strInterval3, 1.0)
    testContainsTypeNotMatch(strInterval3, 1)
  }

  private def testContainsTypeNotMatch(interval: ValueInterval, value: Comparable[_]): Unit = {
    try {
      contains(interval, value)
      fail()
    } catch {
      case _: IllegalArgumentException =>
      case _ => fail()
    }
  }
}
