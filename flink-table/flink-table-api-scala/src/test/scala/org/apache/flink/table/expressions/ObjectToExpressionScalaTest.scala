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

package org.apache.flink.table.expressions

import org.apache.flink.table.api.ImplicitExpressionConversions
import org.apache.flink.table.expressions.ApiExpressionUtils.unwrapFromApi

import org.hamcrest.CoreMatchers.equalTo
import org.junit.Assert.assertThat
import org.junit.Test

import java.math.{BigDecimal => JBigDecimal}

class ObjectToExpressionScalaTest extends ImplicitExpressionConversions {

  @Test
  def testSeqConversion(): Unit = {
      val expr = ApiExpressionUtils.objectToExpression(Seq(1, 2));

      assertThatEquals(expr, array(1, 2));
  }

  @Test
  def testMapConversion(): Unit = {
    val expr = ApiExpressionUtils.objectToExpression(Map(1 -> "ABCDE", 2 -> "EFG"));

    assertThatEquals(expr, map(lit(1), lit("ABCDE"), lit(2), lit("EFG")));
  }

  @Test
  def testDecimalConversion(): Unit = {
    val expr = ApiExpressionUtils.objectToExpression(BigDecimal("1234.456"));

    assertThatEquals(expr, lit(new JBigDecimal("1234.456")));
  }

  private def assertThatEquals(actual: Expression, expected: Expression): Unit = {
    assertThat(unwrapFromApi(actual), equalTo(unwrapFromApi(expected)))
  }
}
