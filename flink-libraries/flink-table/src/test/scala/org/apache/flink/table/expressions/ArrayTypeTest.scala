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

import java.sql.Date

import org.apache.flink.api.common.typeinfo.{PrimitiveArrayTypeInfo, TypeInformation}
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.types.Row
import org.apache.flink.table.api.scala._
import org.apache.flink.table.expressions.utils.ExpressionTestBase
import org.apache.flink.table.api.{Types, ValidationException}
import org.junit.Test

class ArrayTypeTest extends ExpressionTestBase {

  @Test(expected = classOf[ValidationException])
  def testObviousInvalidIndexTableApi(): Unit = {
    testTableApi('f2.at(0), "FAIL", "FAIL")
  }

  @Test(expected = classOf[ValidationException])
  def testEmptyArraySql(): Unit = {
    testSqlApi("ARRAY[]", "FAIL")
  }

  @Test(expected = classOf[ValidationException])
  def testEmptyArrayTableApi(): Unit = {
    testTableApi("FAIL", "array()", "FAIL")
  }

  @Test(expected = classOf[ValidationException])
  def testNullArraySql(): Unit = {
    testSqlApi("ARRAY[NULL]", "FAIL")
  }

  @Test(expected = classOf[ValidationException])
  def testDifferentTypesArraySql(): Unit = {
    testSqlApi("ARRAY[1, TRUE]", "FAIL")
  }

  @Test(expected = classOf[ValidationException])
  def testDifferentTypesArrayTableApi(): Unit = {
    testTableApi("FAIL", "array(1, true)", "FAIL")
  }

  @Test(expected = classOf[ValidationException])
  def testUnsupportedComparison(): Unit = {
    testAllApis(
      'f2 <= 'f5.at(1),
      "f2 <= f5.at(1)",
      "f2 <= f5[1]",
      "FAIL")
  }

  @Test(expected = classOf[ValidationException])
  def testElementNonArray(): Unit = {
    testTableApi(
      'f0.element(),
      "FAIL",
      "FAIL")
  }

  @Test(expected = classOf[ValidationException])
  def testElementNonArraySql(): Unit = {
    testSqlApi(
      "ELEMENT(f0)",
      "FAIL")
  }

  @Test(expected = classOf[ValidationException])
  def testCardinalityOnNonArray(): Unit = {
    testTableApi('f0.cardinality(), "FAIL", "FAIL")
  }

  @Test(expected = classOf[ValidationException])
  def testCardinalityOnNonArraySql(): Unit = {
    testSqlApi("CARDINALITY(f0)", "FAIL")
  }

  @Test
  def testArrayLiterals(): Unit = {
    // primitive literals
    testAllApis(array(1, 2, 3), "array(1, 2, 3)", "ARRAY[1, 2, 3]", "[1, 2, 3]")

    testAllApis(
      array(true, true, true),
      "array(true, true, true)",
      "ARRAY[TRUE, TRUE, TRUE]",
      "[true, true, true]")

    // object literals
    testTableApi(array(BigDecimal(1), BigDecimal(1)), "array(1p, 1p)", "[1, 1]")

    testAllApis(
      array(array(array(1), array(1))),
      "array(array(array(1), array(1)))",
      "ARRAY[ARRAY[ARRAY[1], ARRAY[1]]]",
      "[[[1], [1]]]")

    testAllApis(
      array(1 + 1, 3 * 3),
      "array(1 + 1, 3 * 3)",
      "ARRAY[1 + 1, 3 * 3]",
      "[2, 9]")

    testAllApis(
      array(Null(Types.INT), 1),
      "array(Null(INT), 1)",
      "ARRAY[NULLIF(1,1), 1]",
      "[null, 1]")

    testAllApis(
      array(array(Null(Types.INT), 1)),
      "array(array(Null(INT), 1))",
      "ARRAY[ARRAY[NULLIF(1,1), 1]]",
      "[[null, 1]]")

    // implicit conversion
    testTableApi(
      Array(1, 2, 3),
      "array(1, 2, 3)",
      "[1, 2, 3]")

    testTableApi(
      Array[Integer](1, 2, 3),
      "array(1, 2, 3)",
      "[1, 2, 3]")

    testAllApis(
      Array(Date.valueOf("1985-04-11")),
      "array('1985-04-11'.toDate)",
      "ARRAY[DATE '1985-04-11']",
      "[1985-04-11]")

    testAllApis(
      Array(BigDecimal(2.0002), BigDecimal(2.0003)),
      "Array(2.0002p, 2.0003p)",
      "ARRAY[CAST(2.0002 AS DECIMAL), CAST(2.0003 AS DECIMAL)]",
      "[2.0002, 2.0003]")

    testAllApis(
      Array(Array(x = true)),
      "Array(Array(true))",
      "ARRAY[ARRAY[TRUE]]",
      "[[true]]")

    testAllApis(
      Array(Array(1, 2, 3), Array(3, 2, 1)),
      "Array(Array(1, 2, 3), Array(3, 2, 1))",
      "ARRAY[ARRAY[1, 2, 3], ARRAY[3, 2, 1]]",
      "[[1, 2, 3], [3, 2, 1]]")
  }

  @Test
  def testArrayField(): Unit = {
    testAllApis(
      array('f0, 'f1),
      "array(f0, f1)",
      "ARRAY[f0, f1]",
      "[null, 42]")

    testAllApis(
      array('f0, 'f1),
      "array(f0, f1)",
      "ARRAY[f0, f1]",
      "[null, 42]")

    testAllApis(
      'f2,
      "f2",
      "f2",
      "[1, 2, 3]")

    testAllApis(
      'f3,
      "f3",
      "f3",
      "[1984-03-12, 1984-02-10]")

    testAllApis(
      'f5,
      "f5",
      "f5",
      "[[1, 2, 3], null]")

    testAllApis(
      'f6,
      "f6",
      "f6",
      "[1, null, null, 4]")

    testAllApis(
      'f2,
      "f2",
      "f2",
      "[1, 2, 3]")

    testAllApis(
      'f2.at(1),
      "f2.at(1)",
      "f2[1]",
      "1")

    testAllApis(
      'f3.at(1),
      "f3.at(1)",
      "f3[1]",
      "1984-03-12")

    testAllApis(
      'f3.at(2),
      "f3.at(2)",
      "f3[2]",
      "1984-02-10")

    testAllApis(
      'f5.at(1).at(2),
      "f5.at(1).at(2)",
      "f5[1][2]",
      "2")

    testAllApis(
      'f5.at(2).at(2),
      "f5.at(2).at(2)",
      "f5[2][2]",
      "null")

    testAllApis(
      'f4.at(2).at(2),
      "f4.at(2).at(2)",
      "f4[2][2]",
      "null")
  }

  @Test
  def testArrayOperations(): Unit = {
    // cardinality
    testAllApis(
      'f2.cardinality(),
      "f2.cardinality()",
      "CARDINALITY(f2)",
      "3")

    testAllApis(
      'f4.cardinality(),
      "f4.cardinality()",
      "CARDINALITY(f4)",
      "null")

    // element
    testAllApis(
      'f9.element(),
      "f9.element()",
      "ELEMENT(f9)",
      "1")

    testAllApis(
      'f8.element(),
      "f8.element()",
      "ELEMENT(f8)",
      "4.0")

    testAllApis(
      'f10.element(),
      "f10.element()",
      "ELEMENT(f10)",
      "null")

    testAllApis(
      'f4.element(),
      "f4.element()",
      "ELEMENT(f4)",
      "null")

    // comparison
    testAllApis(
      'f2 === 'f5.at(1),
      "f2 === f5.at(1)",
      "f2 = f5[1]",
      "true")

    testAllApis(
      'f6 === array(1, 2, 3),
      "f6 === array(1, 2, 3)",
      "f6 = ARRAY[1, 2, 3]",
      "false")

    testAllApis(
      'f2 !== 'f5.at(1),
      "f2 !== f5.at(1)",
      "f2 <> f5[1]",
      "false")

    testAllApis(
      'f2 === 'f7,
      "f2 === f7",
      "f2 = f7",
      "false")

    testAllApis(
      'f2 !== 'f7,
      "f2 !== f7",
      "f2 <> f7",
      "true")
  }

  // ----------------------------------------------------------------------------------------------

  case class MyCaseClass(string: String, int: Int)

  override def testData: Any = {
    val testData = new Row(11)
    testData.setField(0, null)
    testData.setField(1, 42)
    testData.setField(2, Array(1, 2, 3))
    testData.setField(3, Array(Date.valueOf("1984-03-12"), Date.valueOf("1984-02-10")))
    testData.setField(4, null)
    testData.setField(5, Array(Array(1, 2, 3), null))
    testData.setField(6, Array[Integer](1, null, null, 4))
    testData.setField(7, Array(1, 2, 3, 4))
    testData.setField(8, Array(4.0))
    testData.setField(9, Array[Integer](1))
    testData.setField(10, Array[Integer]())
    testData
  }

  override def typeInfo: TypeInformation[Any] = {
    new RowTypeInfo(
      Types.INT,
      Types.INT,
      PrimitiveArrayTypeInfo.INT_PRIMITIVE_ARRAY_TYPE_INFO,
      ObjectArrayTypeInfo.getInfoFor(Types.DATE),
      ObjectArrayTypeInfo.getInfoFor(ObjectArrayTypeInfo.getInfoFor(Types.INT)),
      ObjectArrayTypeInfo.getInfoFor(PrimitiveArrayTypeInfo.INT_PRIMITIVE_ARRAY_TYPE_INFO),
      ObjectArrayTypeInfo.getInfoFor(Types.INT),
      PrimitiveArrayTypeInfo.INT_PRIMITIVE_ARRAY_TYPE_INFO,
      PrimitiveArrayTypeInfo.DOUBLE_PRIMITIVE_ARRAY_TYPE_INFO,
      ObjectArrayTypeInfo.getInfoFor(Types.INT),
      ObjectArrayTypeInfo.getInfoFor(Types.INT)
    ).asInstanceOf[TypeInformation[Any]]
  }
}
