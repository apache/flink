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

package org.apache.flink.table.api

import org.apache.flink.table.expressions.ApiExpressionUtils.unwrapFromApi
import org.apache.flink.table.expressions.Expression
import org.apache.flink.types.Row

import org.hamcrest.CoreMatchers.equalTo
import org.junit.Assert.assertThat
import org.junit.Test

/**
 * Tests for conversion between objects and [[Expression]]s used in Expression DSL.
 */
class ImplicitConversionsTest extends ImplicitExpressionConversions {
  @Test
  def testSeqConversion(): Unit = {
    val expr = Seq(1, 2).toExpr

    assertThatEquals(expr, array(1, 2))
  }

  @Test
  def testSeqOfExpressionsConversion(): Unit = {
    val expr = Seq(row(1, "ABC"), row(3, "DEF")).toExpr

    assertThatEquals(expr, array(row(1, "ABC"), row(3, "DEF")))
  }

  @Test
  def testListConversion(): Unit = {
    val expr = List(1, 2).toExpr

    assertThatEquals(expr, array(1, 2))
  }

  @Test
  def testMapConversion(): Unit = {
    val expr = Map("key1" -> List(2), "key2" -> List(1, 2)).toExpr

    assertThatEquals(
      expr,
        map(
          "key1", array(2),
          "key2", array(1, 2)
        )
    )
  }

  @Test
  def testNestedListConversion(): Unit = {
    val expr = List(List(1), List(2)).toExpr

    assertThatEquals(expr, array(array(1), array(2)))
  }

  @Test
  def testRowConversion(): Unit = {
    val expr = Row.of(Int.box(1), "ABC").toExpr

    assertThatEquals(expr, row(1, "ABC"))
  }

  @Test
  def testRowConversionWithScalaTypes(): Unit = {
    val expr = Row.of(Int.box(1), Seq("ABC", "DEF"), BigDecimal(1234)).toExpr

    assertThatEquals(expr, row(1, array("ABC", "DEF"), BigDecimal(1234)))
  }

  private def assertThatEquals(actual: Expression, expected: Expression): Unit = {
    assertThat(unwrapFromApi(actual), equalTo(unwrapFromApi(expected)))
  }
}
