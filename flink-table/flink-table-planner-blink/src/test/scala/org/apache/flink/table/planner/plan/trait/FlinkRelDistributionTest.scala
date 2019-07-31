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
package org.apache.flink.table.planner.plan.`trait`

import org.apache.calcite.rel.RelFieldCollation
import org.apache.calcite.rel.RelFieldCollation.Direction
import org.apache.calcite.util.ImmutableIntList
import org.apache.calcite.util.mapping.{MappingType, Mappings}
import org.junit.Assert.{assertEquals, assertFalse, assertTrue}
import org.junit.Test

class FlinkRelDistributionTest {

  @Test
  def testSatisfy(): Unit = {
    val hash1 = FlinkRelDistribution.hash(ImmutableIntList.of(1), requireStrict = false)
    val hash2 = FlinkRelDistribution.hash(ImmutableIntList.of(2), requireStrict = false)
    val hash12 = FlinkRelDistribution.hash(ImmutableIntList.of(1, 2), requireStrict = false)
    val hash21 = FlinkRelDistribution.hash(ImmutableIntList.of(2, 1), requireStrict = false)
    val strictHash21 = FlinkRelDistribution.hash(ImmutableIntList.of(2, 1))
    assertTrue(hash1.satisfies(hash1))
    assertFalse(hash1.satisfies(hash2))
    assertTrue(hash1.satisfies(hash12))
    assertFalse(hash12.satisfies(hash1))
    assertFalse(hash12.satisfies(hash2))
    assertTrue(hash12.satisfies(hash12))
    assertTrue(hash12.satisfies(hash21))
    assertTrue(hash21.satisfies(hash12))
    assertTrue(strictHash21.satisfies(hash21))
    assertTrue(strictHash21.satisfies(hash12))
    assertFalse(hash21.satisfies(strictHash21))
    assertFalse(hash12.satisfies(strictHash21))

    val rangeAsc1 = FlinkRelDistribution.range(new RelFieldCollation(1, Direction.ASCENDING))
    val rangeDesc1 = FlinkRelDistribution.range(new RelFieldCollation(1, Direction.DESCENDING))
    val range12 = FlinkRelDistribution.range(
      new RelFieldCollation(1), new RelFieldCollation(2))
    val range21 = FlinkRelDistribution.range(
      new RelFieldCollation(2), new RelFieldCollation(1))
    val range123 = FlinkRelDistribution.range(
      new RelFieldCollation(1), new RelFieldCollation(2), new RelFieldCollation(3))
    assertFalse(rangeAsc1.satisfies(rangeDesc1))
    assertTrue(rangeAsc1.satisfies(rangeAsc1))
    assertTrue(rangeDesc1.satisfies(rangeDesc1))
    assertFalse(range12.satisfies(range21))
    assertFalse(range21.satisfies(range12))
    assertTrue(range12.satisfies(range123))
    assertFalse(range12.satisfies(rangeAsc1))

    val hash = FlinkRelDistribution.hash(ImmutableIntList.of(0), requireStrict = false)
    val range = FlinkRelDistribution.range(new RelFieldCollation(0))
    val random = FlinkRelDistribution.RANDOM_DISTRIBUTED
    val roundrobin = FlinkRelDistribution.ROUND_ROBIN_DISTRIBUTED
    val broadcast = FlinkRelDistribution.BROADCAST_DISTRIBUTED
    val singleton = FlinkRelDistribution.SINGLETON
    val any = FlinkRelDistribution.ANY

    assertTrue(hash.satisfies(hash))
    assertFalse(hash.satisfies(range))
    assertTrue(hash.satisfies(random))
    assertFalse(hash.satisfies(roundrobin))
    assertFalse(hash.satisfies(broadcast))
    assertFalse(hash.satisfies(singleton))
    assertTrue(hash.satisfies(any))

    assertFalse(range.satisfies(hash))
    assertTrue(range.satisfies(range))
    assertTrue(range.satisfies(random))
    assertFalse(range.satisfies(roundrobin))
    assertFalse(range.satisfies(broadcast))
    assertFalse(range.satisfies(singleton))
    assertTrue(range.satisfies(any))

    assertFalse(random.satisfies(hash))
    assertFalse(random.satisfies(range))
    assertTrue(random.satisfies(random))
    assertFalse(random.satisfies(roundrobin))
    assertFalse(random.satisfies(broadcast))
    assertFalse(random.satisfies(singleton))
    assertTrue(random.satisfies(any))

    assertFalse(roundrobin.satisfies(hash))
    assertFalse(roundrobin.satisfies(range))
    assertTrue(roundrobin.satisfies(random))
    assertTrue(roundrobin.satisfies(roundrobin))
    assertFalse(roundrobin.satisfies(broadcast))
    assertFalse(roundrobin.satisfies(singleton))
    assertTrue(roundrobin.satisfies(any))

    assertFalse(broadcast.satisfies(hash))
    assertFalse(broadcast.satisfies(range))
    assertFalse(broadcast.satisfies(random))
    assertFalse(broadcast.satisfies(roundrobin))
    assertTrue(broadcast.satisfies(broadcast))
    assertFalse(broadcast.satisfies(singleton))
    assertTrue(broadcast.satisfies(any))

    assertFalse(singleton.satisfies(hash))
    assertFalse(singleton.satisfies(range))
    assertFalse(singleton.satisfies(random))
    assertFalse(singleton.satisfies(roundrobin))
    assertFalse(singleton.satisfies(broadcast))
    assertTrue(singleton.satisfies(singleton))
    assertTrue(singleton.satisfies(any))

    assertFalse(any.satisfies(hash))
    assertFalse(any.satisfies(range))
    assertFalse(any.satisfies(random))
    assertFalse(any.satisfies(roundrobin))
    assertFalse(any.satisfies(broadcast))
    assertFalse(any.satisfies(singleton))
    assertTrue(any.satisfies(any))
  }

  @Test
  def testApply(): Unit = {
    val mapping = Mappings.create(MappingType.INVERSE_FUNCTION, 7, 2)
    mapping.set(6, 0)
    mapping.set(2, 1)
    val finalMapping = mapping.inverse()
    val hash1 = FlinkRelDistribution.hash(ImmutableIntList.of(1), requireStrict = false)
    assertEquals(FlinkRelDistribution.hash(ImmutableIntList.of(2), requireStrict = false),
      hash1.apply(finalMapping))
    val hash2 = FlinkRelDistribution.hash(ImmutableIntList.of(2), requireStrict = false)
    assertEquals(FlinkRelDistribution.ANY, hash2.apply(finalMapping))
    val hash12 = FlinkRelDistribution.hash(ImmutableIntList.of(1, 2), requireStrict = false)
    assertEquals(FlinkRelDistribution.ANY, hash12.apply(finalMapping))
    val hash01 = FlinkRelDistribution.hash(ImmutableIntList.of(0, 1), requireStrict = false)
    assertEquals(FlinkRelDistribution.hash(ImmutableIntList.of(6, 2), requireStrict = false),
      hash01.apply(finalMapping))
    val strictHash01 = FlinkRelDistribution.hash(ImmutableIntList.of(0, 1))
    assertEquals(FlinkRelDistribution.hash(ImmutableIntList.of(6, 2)),
      strictHash01.apply(finalMapping))

    val rangeAsc1 = FlinkRelDistribution.range(new RelFieldCollation(1, Direction.ASCENDING))
    assertEquals(FlinkRelDistribution.range(new RelFieldCollation(2)),
      rangeAsc1.apply(finalMapping))
    val rangeDesc1 = FlinkRelDistribution.range(new RelFieldCollation(1, Direction.DESCENDING))
    assertEquals(FlinkRelDistribution.range(new RelFieldCollation(2, Direction.DESCENDING)),
      rangeDesc1.apply(finalMapping))
    val range12 = FlinkRelDistribution.range(
      new RelFieldCollation(1), new RelFieldCollation(2))
    assertEquals(FlinkRelDistribution.ANY, range12.apply(finalMapping))
    val range01 = FlinkRelDistribution.range(
      new RelFieldCollation(0), new RelFieldCollation(1))
    assertEquals(FlinkRelDistribution.range(new RelFieldCollation(6), new RelFieldCollation(2)),
      range01.apply(finalMapping))
  }

}
