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

package org.apache.flink.table.planner.plan.utils

import org.apache.flink.table.planner.calcite.FlinkRexBuilder

import org.junit.Assert.{assertTrue, assertEquals}
import org.junit.Test

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

/**
 *  Tests for [[NestedSchema]].
 */
class NestedProjectionUtilTest extends RexNodeTestBase{

  private def assertArray(actual: Array[Array[Int]], expected: Array[Array[Int]]): Unit = {
    assertEquals(expected.length, actual.length)
    actual.zip(expected).foreach{
      case (result, expect) =>
        assert(result.sameElements(expect))
    }
  }

  @Test
  def testExtractRefInputFields(): Unit = {
    val (exprs, rowType) = buildExprs()
    val nestedFields = NestedProjectionUtil.build(exprs, rowType)
    val actual = NestedProjectionUtil.convertToIndexArray(nestedFields)
    val expected = Array(Array(2), Array(3), Array(1))

    assertArray(actual, expected)
  }

  @Test
  def testExtractRefNestedInputFields(): Unit = {
    val (rexProgram, rowType) = buildExprsWithNesting()

    val nestedFields = NestedProjectionUtil.build(rexProgram, rowType)
    val actual = NestedProjectionUtil.convertToIndexArray(nestedFields)
    val expected = Array(Array(1, 1), Array(0))

    assertArray(actual, expected)
  }

  @Test
  def testExtractDeepRefNestedInputFieldsUsingNestedField(): Unit = {
    val (rexProgram, rowType) = buildExprsWithDeepNesting()

    val nestedFields = NestedProjectionUtil.build(rexProgram, rowType)
    val actual = NestedProjectionUtil.convertToIndexArray(nestedFields)
    val expected = Array(
      Array(1, 1),
      Array(0),
      Array(2, 0, 0, 0),
      Array(2, 0, 1, 0)
    )

    assertArray(actual, expected)
  }

  @Test
  def testRewriteRexProgram(): Unit = {
    val rexProgram = buildSimpleRexProgram()
    val exprs = rexProgram.getExprList
    assertTrue(exprs.asScala.map(_.toString) == wrapRefArray(Array(
      "$0",
      "$1",
      "$2",
      "$3",
      "$4",
      "*($t2, $t3)",
      "100",
      "<($t5, $t6)",
      "6",
      ">($t1, $t8)",
      "AND($t7, $t9)")))

    val nestedField = NestedProjectionUtil.build(exprs, rexProgram.getInputRowType)
    val paths = NestedProjectionUtil.convertToIndexArray(nestedField)
    val orderedPaths = Array(
      Array(0),
      Array(1),
      Array(2),
      Array(3),
      Array(4)
    )
    assertArray(paths, orderedPaths)
    val builder = new FlinkRexBuilder(typeFactory)
    val projectExprs = rexProgram.getProjectList.map(expr => rexProgram.expandLocalRef(expr))
    val newProjectExprs =
      NestedProjectionUtil.rewrite(
        projectExprs, nestedField, builder)
    val conditionExprs = rexProgram.expandLocalRef(rexProgram.getCondition)
    val newConditionExprs =
      NestedProjectionUtil.rewrite(Seq(conditionExprs), nestedField, builder)
    assertTrue(newProjectExprs.asScala.map(_.toString) == wrapRefArray(Array(
      "$2",
      "*($2, $3)")))
    assertTrue(newConditionExprs.asScala.map(_.toString) == wrapRefArray(Array(
      "AND(<(*($2, $3), 100), >($1, 6))"
    )))
  }

  @Test
  def testRewriteRExProgramWithNestedProject(): Unit = {
    // origin schema: $0 = RAW<name INT, age varchar>, $1 = RAW<id BIGINT, amount int>.amount
    // new schema: $1 = ROW<name INT, age varchar>, $0 = ROW<id BIGINT, amount int>.amount
    val (exprs, rowType) = buildExprsWithNesting()
    assertTrue(exprs.asScala.map(_.toString) == wrapRefArray(Array(
      "$1.amount",
      "$0",
      "100"
    )))

    val nestedField = NestedProjectionUtil.build(exprs, rowType)
    val paths = NestedProjectionUtil.convertToIndexArray(nestedField)
    val orderedPaths = Array(
      Array(1, 1),
      Array(0)
    )
    assertArray(paths, orderedPaths)
    val newExprs =
      NestedProjectionUtil.rewrite(exprs, nestedField, new FlinkRexBuilder(typeFactory))

    assertTrue(newExprs.asScala.map(_.toString) == wrapRefArray(Array(
      "$0",
      "$1",
      "100")))
  }

  @Test
  def testRewriteRexProgramWithDeepNestedProject(): Unit = {
    // origin schema:
    // $0 = persons ROW<name VARCHAR, age INT, passport ROW<id VARCHAR, status VARCHAR>>
    // $1 = payment ROW<id BIGINT, amount INT>
    // $2 = field ROW<with ROW<deeper ROW<entry ROW<inside ROW<entry VARCHAR>>>,
    //                         deep ROW<entry VARCHAR>>>

    // new schema:
    // $0 = payment.amount INT
    // $1 = persons ROW<name VARCHAR, age INT, passport ROW<id VARCHAR, status VARCHAR>>
    // $2 = field.with.deep.entry VARCHAR
    // $3 = field.with.deeper.entry ROW<inside ROW<entry VARCHAR>>

    // mapping
    // $1.amount -> $0
    // $0.passport.status -> $1.passport.status
    // $2.with.deep.entry -> $2
    // $2.with.deeper.entry.inside.entry -> $3.inside.entry
    // $2.with.deeper.entry -> $3
    // $0 -> $1

    val (exprs, rowType) = buildExprsWithDeepNesting()
    assertTrue(exprs.asScala.map(_.toString) == wrapRefArray(Array(
      "*($1.amount, 10)",
      "$0.passport.status",
      "$2.with.deep.entry",
      "$2.with.deeper.entry.inside.entry",
      "$2.with.deeper.entry",
      "$0"
    )))
    val nestedFields = NestedProjectionUtil.build(exprs, rowType)
    val paths = NestedProjectionUtil.convertToIndexArray(nestedFields)
    val orderedPaths = Array(
      Array(1, 1),
      Array(0),
      Array(2, 0, 0, 0),
      Array(2, 0, 1, 0)
    )
    assertArray(paths, orderedPaths)
    val newExprs =
      NestedProjectionUtil.rewrite(exprs, nestedFields, new FlinkRexBuilder(typeFactory))

    assertTrue(newExprs.asScala.map(_.toString) == wrapRefArray(Array(
      "*($0, 10)",
      "$1.passport.status",
      "$2",
      "$3.inside.entry",
      "$3",
      "$1")))
  }
}
