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

package org.apache.flink.table.plan.util

import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.types.DataTypes
import org.apache.flink.table.expressions.{Expression, ResolvedFieldReference, UnresolvedFieldReference}
import org.apache.flink.table.plan.util.PartitionPredicateExtractor.extractPartitionPredicates
import org.junit.Assert._
import org.junit.Test

class PartitionPredicateExtractorTest {

  def resolveFields(
    predicates: Array[Expression],
    allFieldNames: Array[String]): Array[Expression] = {
    val fieldTypes = allFieldNames.zip(allFieldNames.map(_ => DataTypes.STRING)).toMap

    val rule: PartialFunction[Expression, Expression] = {
      case u@UnresolvedFieldReference(name) =>
        ResolvedFieldReference(name, fieldTypes(name))
    }
    predicates.map(_.postOrderTransform(rule))
  }

  @Test
  def testNonePartitionField(): Unit = {
    val partitionFieldNames = Array("part")
    val predicate = resolveFields(Array[Expression]('name === "abc"), Array("part", "name"))
    val (partitionPredicate, remaining) = extractPartitionPredicates(predicate, partitionFieldNames)
    assertTrue(partitionPredicate.isEmpty)
    assertEquals(1, remaining.length)
    assertEquals(predicate.head, remaining.head)
  }

  @Test
  def testOnePartitionField(): Unit = {
    val partitionFieldNames = Array("part")
    val predicate = resolveFields(Array[Expression]('part === "test"), Array("part"))
    val (partitionPredicate, remaining) = extractPartitionPredicates(predicate, partitionFieldNames)
    assertEquals(1, partitionPredicate.length)
    assertEquals(predicate.head, partitionPredicate.head)
    assertTrue(remaining.isEmpty)
  }

  @Test
  def testOnePartitionFieldWithOr(): Unit = {
    val partitionFieldNames = Array("part")
    val predicate = resolveFields(
      Array[Expression]('part === "test1" || 'part === "test2"), Array("part"))
    val (partitionPredicate, remaining) = extractPartitionPredicates(predicate, partitionFieldNames)
    assertEquals(1, partitionPredicate.length)
    assertEquals(predicate.head, partitionPredicate.head)
    assertTrue(remaining.isEmpty)
  }

  @Test
  def testOnePartitionOrNonPartitionField(): Unit = {
    val partitionFieldNames = Array("part")
    val predicate = resolveFields(
      Array[Expression]('name === "abc" || 'part === "test"), Array("name", "part"))
    val (partitionPredicate, remaining) = extractPartitionPredicates(predicate, partitionFieldNames)
    assertTrue(partitionPredicate.isEmpty)
    assertEquals(1, remaining.length)
    assertEquals(predicate.head, remaining.head)
  }

  @Test
  def testTwoPartitionFieldsWithOr(): Unit = {
    val partitionFieldNames = Array("part1", "part2")
    val predicate = resolveFields(
      Array[Expression]('part1 === "test1" || 'part2 === "test2"), Array("part1", "part2"))
    val (partitionPredicate, remaining) = extractPartitionPredicates(predicate, partitionFieldNames)
    assertEquals(1, partitionPredicate.length)
    assertEquals(predicate.head, partitionPredicate.head)
    assertTrue(remaining.isEmpty)
  }

  @Test
  def testTwoPartitionFieldsOrNonPartitionField(): Unit = {
    val partitionFieldNames = Array("part1", "part2")
    val predicate = resolveFields(
      Array[Expression]('part1 === "test1" && 'part2 === "test2" || 'name === "abc"),
      Array("name", "part1", "part2"))
    val (partitionPredicate, remaining) = extractPartitionPredicates(predicate, partitionFieldNames)
    assertTrue(partitionPredicate.isEmpty)
    assertEquals(1, remaining.length)
    assertEquals(predicate.head, remaining.head)
  }

  @Test
  def testLike(): Unit = {
    val partitionFieldNames = Array("part")
    val predicate = resolveFields(Array[Expression]('part.like("%est")), Array("part"))
    val (partitionPredicate, remaining) = extractPartitionPredicates(predicate, partitionFieldNames)
    assertEquals(1, partitionPredicate.length)
    assertEquals(predicate.head, partitionPredicate.head)
    assertTrue(remaining.isEmpty)
  }

  @Test
  def testIsNull(): Unit = {
    val partitionFieldNames = Array("part")
    val predicate = resolveFields(Array[Expression]('part.isNull), Array("part"))
    val (partitionPredicate, remaining) = extractPartitionPredicates(predicate, partitionFieldNames)
    assertEquals(1, partitionPredicate.length)
    assertEquals(predicate.head, partitionPredicate.head)
    assertTrue(remaining.isEmpty)
  }

  @Test
  def testIsNotNull(): Unit = {
    val partitionFieldNames = Array("part")
    val predicate = resolveFields(Array[Expression]('part.isNotNull), Array("part"))
    val (partitionPredicate, remaining) = extractPartitionPredicates(predicate, partitionFieldNames)
    assertEquals(1, partitionPredicate.length)
    assertEquals(predicate.head, partitionPredicate.head)
    assertTrue(remaining.isEmpty)
  }

  @Test
  def testCast(): Unit = {
    val partitionFieldNames = Array("part")
    val predicate = resolveFields(
      Array[Expression]('part.cast(DataTypes.INT)), Array("part"))
    val (partitionPredicate, remaining) = extractPartitionPredicates(predicate, partitionFieldNames)
    assertEquals(1, partitionPredicate.length)
    assertEquals(predicate.head, partitionPredicate.head)
    assertTrue(remaining.isEmpty)
  }

  @Test
  def testIf(): Unit = {
    val partitionFieldNames = Array("part")
    val predicate = resolveFields(
      Array[Expression]('part.isNull.?("test1", "test2")), Array("part"))
    val (partitionPredicate, remaining) = extractPartitionPredicates(predicate, partitionFieldNames)
    assertEquals(1, partitionPredicate.length)
    assertEquals(predicate.head, partitionPredicate.head)
    assertTrue(remaining.isEmpty)
  }

  @Test
  def testMultipleCompositeExpressions(): Unit = {
    val partitionFieldNames = Array("part1", "part2")
    val predicate = resolveFields(Array[Expression](
      'part1 > "test1",
      'name >= "abc",
      'name === "def" || 'part1 === "test",
      'part1 >= "test1" || 'part2 < "test2",
      'part1 === "test1" && 'part2 === "test2" || 'name === "abc",
      'part1.isNotNull || 'part2.like("test") || 'part2.cast(DataTypes.INT) === 1,
      'part2.toTimestamp === 123456789,
      'part1.isNull.?("test1", "test2") || 'part2.isNotNull.upperCase === "TEST"
    ), Array("part1", "part2", "name"))
    val (partitionPredicate, remaining) = extractPartitionPredicates(predicate, partitionFieldNames)
    assertEquals(5, partitionPredicate.length)
    assertEquals(predicate.head, partitionPredicate.head)
    assertEquals(predicate(3), partitionPredicate(1))
    assertEquals(predicate(5), partitionPredicate(2))
    assertEquals(predicate(6), partitionPredicate(3))
    assertEquals(predicate(7), partitionPredicate(4))

    assertEquals(3, remaining.length)
    assertEquals(predicate(1), remaining.head)
    assertEquals(predicate(2), remaining(1))
    assertEquals(predicate(4), remaining(2))
  }

}
