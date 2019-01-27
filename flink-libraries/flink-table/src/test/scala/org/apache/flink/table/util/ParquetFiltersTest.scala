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

package org.apache.flink.table.util

import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.types.{DataTypes, InternalType}
import org.apache.flink.table.expressions._
import org.apache.flink.table.sources.parquet.ParquetFilters
import org.junit.Assert._
import org.junit.Test

class ParquetFiltersTest {

  private def checkFilterPredicate(
      expert: String,
      predicates: Array[Expression],
      fieldNames: Array[String],
      types: Array[InternalType]): Unit = {
    assertEquals(expert, ParquetFilters.applyPredicate(types, fieldNames, predicates).toString)
  }

  private def createExpressions(expression : String*): Array[Expression] = {
    expression.map(ExpressionParser.parseExpression(_)).toArray
  }

  @Test
  def testBooleanConvertParquetFilter(): Unit = {
    val types: Array[InternalType] = Array(DataTypes.STRING, DataTypes.BOOLEAN)
    val names = Array("name", "value")

    checkFilterPredicate("noteq(name, null)", Array[Expression]('name.isNotNull), names, types)

    checkFilterPredicate("eq(name, null)", Array[Expression]('name.isNull), names, types)

    checkFilterPredicate("eq(value, false)", Array[Expression]('value.isFalse), names, types)

    checkFilterPredicate("eq(value, true)", Array[Expression]('value.isTrue), names, types)

    checkFilterPredicate("noteq(value, false)", Array[Expression]('value.isNotFalse), names, types)

    checkFilterPredicate("noteq(value, true)", Array[Expression]('value.isNotTrue), names, types)

  }

  @Test
  def testIntegerConvertParquetFilter(): Unit = {
    val types: Array[InternalType] = Array(DataTypes.STRING, DataTypes.INT)
    val names = Array("name", "value")

    //right expression is literal
    checkFilterPredicate("eq(value, 1)", Array[Expression]('value === 1), names, types)

    checkFilterPredicate("gt(value, 1)", Array[Expression]('value > 1), names, types)

    checkFilterPredicate("lt(value, 1)", Array[Expression]('value < 1), names, types)

    checkFilterPredicate("gteq(value, 1)", Array[Expression]('value >= 1), names, types)

    checkFilterPredicate("lteq(value, 1)", Array[Expression]('value <= 1), names, types)

    checkFilterPredicate("noteq(value, 1)", Array[Expression]('value !== 1), names, types)

    //left expression is literal
    checkFilterPredicate("eq(value, 1)", createExpressions("1 = value"), names, types)

    checkFilterPredicate("gt(value, 1)", createExpressions("1 < value"), names, types)

    checkFilterPredicate("lt(value, 1)", createExpressions("1 > value"), names, types)

    checkFilterPredicate("gteq(value, 1)", createExpressions("1 <= value"), names, types)

    checkFilterPredicate("lteq(value, 1)", createExpressions("1 >= value"), names, types)

    checkFilterPredicate("noteq(value, 1)", createExpressions("1 != value"), names, types)

    // or & and
    checkFilterPredicate(
      "and(noteq(value, 1), gt(value, 1))",
      Array[Expression]('value !== 1, 'value > 1),
      names,
      types)

    checkFilterPredicate(
      "or(eq(value, 1), eq(name, null))",
      Array[Expression]('value === 1 || 'name.isNull),
      names,
      types)

    checkFilterPredicate(
      "and(eq(value, 1), eq(name, null))",
      Array[Expression]('value === 1 && 'name.isNull),
      names,
      types)

    checkFilterPredicate(
      "and(eq(value, 1), eq(name, null))",
      Array[Expression](1 === 'value && 'name.isNull),
      names,
      types)
  }

  @Test
  def testLongConvertParquetFilter(): Unit = {
    val types: Array[InternalType] = Array(DataTypes.STRING, DataTypes.LONG)
    val names = Array("name", "value")

    //right expression is literal
    checkFilterPredicate("eq(value, 1)", Array[Expression]('value === 1L), names, types)

    checkFilterPredicate("gt(value, 1)", Array[Expression]('value > 1L), names, types)

    checkFilterPredicate("lt(value, 1)", Array[Expression]('value < 1L), names, types)

    checkFilterPredicate("gteq(value, 1)", Array[Expression]('value >= 1L), names, types)

    checkFilterPredicate("lteq(value, 1)", Array[Expression]('value <= 1L), names, types)

    checkFilterPredicate("noteq(value, 1)", Array[Expression]('value !== 1L), names, types)

    //left expression is literal
    checkFilterPredicate("eq(value, 1)", createExpressions("1L = value"), names, types)

    checkFilterPredicate("gt(value, 1)", createExpressions("1L < value"), names, types)

    checkFilterPredicate("lt(value, 1)", createExpressions("1L > value"), names, types)

    checkFilterPredicate("gteq(value, 1)", createExpressions("1L <= value"), names, types)

    checkFilterPredicate("lteq(value, 1)", createExpressions("1L >= value"), names, types)

    checkFilterPredicate("noteq(value, 1)", createExpressions("1L != value"), names, types)

    //or & and
    checkFilterPredicate(
      "and(noteq(value, 1), gt(value, 1))",
      Array[Expression]('value !== 1L, 'value > 1L),
      names,
      types)

    checkFilterPredicate(
      "or(eq(value, 1), eq(name, null))",
      Array[Expression]('value === 1L || 'name.isNull),
      names,
      types)

    checkFilterPredicate(
      "and(eq(value, 1), eq(name, null))",
      Array[Expression]('value === 1L && 'name.isNull),
      names,
      types)

    checkFilterPredicate(
      "and(eq(value, 1), eq(name, null))",
      Array[Expression](1L === 'value && 'name.isNull),
      names,
      types)
  }

  @Test
  def testFloatConvertParquetFilter(): Unit = {
    val types: Array[InternalType] = Array(DataTypes.STRING, DataTypes.FLOAT)
    val names = Array("name", "value")

    //right expression is literal
    checkFilterPredicate("eq(value, 1.0)", Array[Expression]('value === 1f), names, types)

    checkFilterPredicate("gt(value, 1.0)", Array[Expression]('value > 1f), names, types)

    checkFilterPredicate("lt(value, 1.0)", Array[Expression]('value < 1f), names, types)

    checkFilterPredicate("gteq(value, 1.0)", Array[Expression]('value >= 1f), names, types)

    checkFilterPredicate("lteq(value, 1.0)", Array[Expression]('value <= 1f), names, types)

    checkFilterPredicate("noteq(value, 1.0)", Array[Expression]('value !== 1f), names, types)

    //left expression is literal
    checkFilterPredicate("eq(value, 1.0)", createExpressions("1f = value"), names, types)

    checkFilterPredicate("gt(value, 1.0)", createExpressions("1f < value"), names, types)

    checkFilterPredicate("lt(value, 1.0)", createExpressions("1f > value"), names, types)

    checkFilterPredicate("gteq(value, 1.0)", createExpressions("1f <= value"), names, types)

    checkFilterPredicate("lteq(value, 1.0)", createExpressions("1f >= value"), names, types)

    checkFilterPredicate("noteq(value, 1.0)", createExpressions("1f != value"), names, types)

    checkFilterPredicate(
      "and(noteq(value, 1.0), gt(value, 1.0))",
      Array[Expression]('value !== 1f, 'value > 1f),
      names,
      types)

    checkFilterPredicate(
      "or(eq(value, 1.0), eq(name, null))",
      Array[Expression]('value === 1f || 'name.isNull),
      names,
      types)

    checkFilterPredicate(
      "and(eq(value, 1.0), eq(name, null))",
      Array[Expression]('value === 1f && 'name.isNull),
      names,
      types)

    checkFilterPredicate(
      "and(eq(value, 1.0), eq(name, null))",
      Array[Expression](1f === 'value && 'name.isNull),
      names,
      types)
  }

  @Test
  def testDoubleConvertParquetFilter(): Unit = {
    val types: Array[InternalType] = Array(DataTypes.STRING, DataTypes.DOUBLE)
    val names = Array("name", "value")
    val constantValue = 1.asInstanceOf[Double]

    //right expression is literal
    checkFilterPredicate(
      "eq(value, 1.0)",
      Array[Expression]('value === constantValue),
      names,
      types)

    checkFilterPredicate("gt(value, 1.0)", Array[Expression]('value > constantValue), names, types)

    checkFilterPredicate("lt(value, 1.0)", Array[Expression]('value < constantValue), names, types)

    checkFilterPredicate(
      "gteq(value, 1.0)",
      Array[Expression]('value >= constantValue),
      names,
      types)

    checkFilterPredicate(
      "lteq(value, 1.0)",
      Array[Expression]('value <= constantValue),
      names,
      types)

    checkFilterPredicate(
      "noteq(value, 1.0)",
      Array[Expression]('value !== constantValue),
      names,
      types)

    //left expression is literal
    checkFilterPredicate(
      "eq(value, 1.0)",
      createExpressions("1d = value"),
      names,
      types)

    checkFilterPredicate("gt(value, 1.0)", createExpressions("1d < value"), names, types)

    checkFilterPredicate("lt(value, 1.0)", createExpressions("1d > value"), names, types)

    checkFilterPredicate(
      "gteq(value, 1.0)",
      createExpressions("1d <= value"),
      names,
      types)

    checkFilterPredicate(
      "lteq(value, 1.0)",
      createExpressions("1d >= value"),
      names,
      types)

    checkFilterPredicate(
      "noteq(value, 1.0)",
      createExpressions("1d != value"),
      names,
      types)

    //or & and
    checkFilterPredicate(
      "and(noteq(value, 1.0), gt(value, 1.0))",
      Array[Expression]('value !== constantValue, 'value > constantValue),
      names,
      types)

    checkFilterPredicate(
      "or(eq(value, 1.0), eq(name, null))",
      Array[Expression]('value === constantValue || 'name.isNull),
      names,
      types)

    checkFilterPredicate(
      "and(eq(value, 1.0), eq(name, null))",
      Array[Expression]('value === constantValue && 'name.isNull),
      names,
      types)
  }

  @Test
  def testStringConvertParquetFilter(): Unit = {
    val types: Array[InternalType] = Array(DataTypes.STRING, DataTypes.STRING)
    val names = Array("name", "value")
    val constantValue = "1"

    //right expression is literal
    checkFilterPredicate(
      "eq(value, Binary{\"1\"})", Array[Expression]('value === constantValue),
      names, types)

    checkFilterPredicate(
      "gt(value, Binary{\"1\"})",
      Array[Expression]('value > constantValue),
      names,
      types)

    checkFilterPredicate(
      "lt(value, Binary{\"1\"})",
      Array[Expression]('value < constantValue),
      names,
      types)

    checkFilterPredicate(
      "gteq(value, Binary{\"1\"})",
      Array[Expression]('value >= constantValue),
      names,
      types)

    checkFilterPredicate(
      "lteq(value, Binary{\"1\"})",
      Array[Expression]('value <= constantValue),
      names,
      types)

    checkFilterPredicate(
      "noteq(value, Binary{\"1\"})",
      Array[Expression]('value !== constantValue),
      names,
      types)

    //left expression is literal
    checkFilterPredicate(
      "eq(value, Binary{\"1\"})", createExpressions("\"1\" = value"),
      names, types)

    checkFilterPredicate(
      "gt(value, Binary{\"1\"})",
      createExpressions("\"1\" < value"),
      names,
      types)

    checkFilterPredicate(
      "lt(value, Binary{\"1\"})",
      createExpressions("\"1\" > value"),
      names,
      types)

    checkFilterPredicate(
      "gteq(value, Binary{\"1\"})",
      createExpressions("\"1\" <= value"),
      names,
      types)

    checkFilterPredicate(
      "lteq(value, Binary{\"1\"})",
      createExpressions("\"1\" >= value"),
      names,
      types)

    checkFilterPredicate(
      "noteq(value, Binary{\"1\"})",
      createExpressions("\"1\" != value"),
      names,
      types)

    //or & and
    checkFilterPredicate(
      "and(noteq(value, Binary{\"1\"}), gt(value, Binary{\"1\"}))",
      Array[Expression]('value !== constantValue, 'value > constantValue),
      names,
      types)

    checkFilterPredicate(
      "or(eq(value, Binary{\"1\"}), eq(name, null))",
      Array[Expression]('value === constantValue || 'name.isNull),
      names,
      types)

    checkFilterPredicate(
      "and(eq(value, Binary{\"1\"}), eq(name, null))",
      Array[Expression]('value === constantValue && 'name.isNull),
      names,
      types)

    checkFilterPredicate(
      "and(eq(value, Binary{\"1\"}), eq(name, null))",
      Array[Expression](constantValue === 'value && 'name.isNull),
      names,
      types)
  }

  @Test
  def testBinaryConvertParquetFilter(): Unit = {
    val types: Array[InternalType] = Array(DataTypes.STRING, DataTypes.BYTE_ARRAY)
    val names = Array("name", "value")
    val constantValue = new Array[Byte](2)

    //right expression is literal
    checkFilterPredicate(
      "eq(value, Binary{2 reused bytes, [0, 0]})",
      Array[Expression]('value === constantValue),
      names, types)

    checkFilterPredicate(
      "gt(value, Binary{2 reused bytes, [0, 0]})", Array[Expression](
        'value > constantValue), names, types)

    checkFilterPredicate(
      "lt(value, Binary{2 reused bytes, [0, 0]})", Array[Expression](
        'value < constantValue), names, types)

    checkFilterPredicate(
      "gteq(value, Binary{2 reused bytes, [0, 0]})",
      Array[Expression]('value >= constantValue),
      names,
      types)

    checkFilterPredicate(
      "lteq(value, Binary{2 reused bytes, [0, 0]})",
      Array[Expression]('value <= constantValue),
      names,
      types)

    checkFilterPredicate(
      "noteq(value, Binary{2 reused bytes, [0, 0]})",
      Array[Expression]('value !== constantValue),
      names,
      types)

    //left expression is literal

    checkFilterPredicate(
      "eq(value, Binary{2 reused bytes, [0, 0]})",
      Array[Expression](EqualTo(ExpressionUtils.convertArray(constantValue), ExpressionParser
        .parseExpression("value"))),
      names, types)

    checkFilterPredicate(
      "lt(value, Binary{2 reused bytes, [0, 0]})",
      Array[Expression](GreaterThan(ExpressionUtils.convertArray(constantValue), ExpressionParser
        .parseExpression("value"))),
      names, types)

    checkFilterPredicate(
      "gt(value, Binary{2 reused bytes, [0, 0]})",
      Array[Expression](LessThan(ExpressionUtils.convertArray(constantValue), ExpressionParser
        .parseExpression("value"))),
      names, types)

    checkFilterPredicate(
      "noteq(value, Binary{2 reused bytes, [0, 0]})",
      Array[Expression](NotEqualTo(ExpressionUtils.convertArray(constantValue), ExpressionParser
        .parseExpression("value"))),
      names, types)

    //or & and
    checkFilterPredicate(
      "and(noteq(value, Binary{2 reused bytes, [0, 0]}), gt(value, Binary{2 reused bytes, [0, " +
        "0]}))",
      Array[Expression]('value !== constantValue, 'value > constantValue),
      names,
      types)

    checkFilterPredicate(
      "or(eq(value, Binary{2 reused bytes, [0, 0]}), eq(name, null))",
      Array[Expression]('value === constantValue || 'name.isNull),
      names,
      types)

    checkFilterPredicate(
      "and(eq(value, Binary{2 reused bytes, [0, 0]}), eq(name, null))",
      Array[Expression]('value === constantValue && 'name.isNull),
      names,
      types)
  }
}
