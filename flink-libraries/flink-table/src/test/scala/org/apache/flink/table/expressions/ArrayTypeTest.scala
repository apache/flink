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

import org.apache.flink.table.api.Types
import org.apache.flink.table.api.scala._
import org.apache.flink.table.expressions.utils.ArrayTypeTestBase
import org.junit.Test

class ArrayTypeTest extends ArrayTypeTestBase {

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

    // implicit type cast only works on SQL APIs.
    testSqlApi("ARRAY[CAST(1 AS DOUBLE), CAST(2 AS FLOAT)]", "[1.0, 2.0]")
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

    testAllApis(
      'f11.at(1),
      "f11.at(1)",
      "f11[1]",
      "1")
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

    testAllApis(
      'f11.cardinality(),
      "f11.cardinality()",
      "CARDINALITY(f11)",
      "1")

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

    testAllApis(
      'f11.element(),
      "f11.element()",
      "ELEMENT(f11)",
      "1")

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

    testAllApis(
      'f11 === 'f11,
      "f11 === f11",
      "f11 = f11",
      "true")

    testAllApis(
      'f11 === 'f9,
      "f11 === f9",
      "f11 = f9",
      "true")

    testAllApis(
      'f11 !== 'f11,
      "f11 !== f11",
      "f11 <> f11",
      "false")

    testAllApis(
      'f11 !== 'f9,
      "f11 !== f9",
      "f11 <> f9",
      "false")
  }

  @Test
  def testArrayTypeCasting(): Unit = {
    testTableApi(
      'f3.cast(Types.OBJECT_ARRAY(Types.SQL_DATE)),
      "f3.cast(OBJECT_ARRAY(SQL_DATE))",
      "[1984-03-12, 1984-02-10]"
    )
  }
}
