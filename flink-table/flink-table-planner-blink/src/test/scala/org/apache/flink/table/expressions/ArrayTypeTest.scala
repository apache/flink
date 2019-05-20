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

import org.apache.flink.table.expressions.utils.ArrayTypeTestBase
import org.junit.Test

class ArrayTypeTest extends ArrayTypeTestBase {

  @Test
  def testArrayLiterals(): Unit = {
    // primitive literals
    testSqlApi(
      "ARRAY[1, 2, 3]",
      "[1, 2, 3]")

    testSqlApi(
      "ARRAY[TRUE, TRUE, TRUE]",
      "[true, true, true]")

    testSqlApi(
      "ARRAY[ARRAY[ARRAY[1], ARRAY[1]]]",
      "[[[1], [1]]]")

    testSqlApi(
      "ARRAY[1 + 1, 3 * 3]",
      "[2, 9]")

    testSqlApi(
      "ARRAY[NULLIF(1,1), 1]",
      "[null, 1]")

    testSqlApi(
      "ARRAY[ARRAY[NULLIF(1,1), 1]]",
      "[[null, 1]]")

    testSqlApi(
      "ARRAY[DATE '1985-04-11', DATE '2018-07-26']",
      "[1985-04-11, 2018-07-26]")

    testSqlApi(
      "ARRAY[TIME '14:15:16', TIME '17:18:19']",
      "[14:15:16, 17:18:19]")

    testSqlApi(
      "ARRAY[TIMESTAMP '1985-04-11 14:15:16', TIMESTAMP '2018-07-26 17:18:19']",
      "[1985-04-11 14:15:16.000, 2018-07-26 17:18:19.000]")

    testSqlApi(
      "ARRAY[CAST(2.0002 AS DECIMAL(10,4)), CAST(2.0003 AS DECIMAL(10,4))]",
      "[2.0002, 2.0003]")

    testSqlApi(
      "ARRAY[ARRAY[TRUE]]",
      "[[true]]")

    testSqlApi(
      "ARRAY[ARRAY[1, 2, 3], ARRAY[3, 2, 1]]",
      "[[1, 2, 3], [3, 2, 1]]")

    // implicit type cast only works on SQL APIs.
    testSqlApi(
      "ARRAY[CAST(1 AS DOUBLE), CAST(2 AS FLOAT)]",
      "[1.0, 2.0]")
  }

  @Test
  def testArrayField(): Unit = {
    testSqlApi(
      "ARRAY[f0, f1]",
      "[null, 42]")

    testSqlApi(
      "ARRAY[f0, f1]",
      "[null, 42]")

    testSqlApi(
      "f2",
      "[1, 2, 3]")

    testSqlApi(
      "f3",
      "[1984-03-12, 1984-02-10]")

    testSqlApi(
      "f5",
      "[[1, 2, 3], null]")

    testSqlApi(
      "f6",
      "[1, null, null, 4]")

    testSqlApi(
      "f2",
      "[1, 2, 3]")

    testSqlApi(
      "f2[1]",
      "1")

    testSqlApi(
      "f3[1]",
      "1984-03-12")

    testSqlApi(
      "f3[2]",
      "1984-02-10")

    testSqlApi(
      "f5[1][2]",
      "2")

    testSqlApi(
      "f5[2][2]",
      "null")

    testSqlApi(
      "f4[2][2]",
      "null")

    testSqlApi(
      "f11[1]",
      "1")
  }

  @Test
  def testArrayOperations(): Unit = {
    // cardinality
    testSqlApi(
      "CARDINALITY(f2)",
      "3")

    testSqlApi(
      "CARDINALITY(f4)",
      "null")

    testSqlApi(
      "CARDINALITY(f11)",
      "1")

    // element
    testSqlApi(
      "ELEMENT(f9)",
      "1")

    testSqlApi(
      "ELEMENT(f8)",
      "4.0")

    testSqlApi(
      "ELEMENT(f10)",
      "null")

    testSqlApi(
      "ELEMENT(f4)",
      "null")

    testSqlApi(
      "ELEMENT(f11)",
      "1")

    // comparison
    testSqlApi(
      "f2 = f5[1]",
      "true")

    testSqlApi(
      "f6 = ARRAY[1, 2, 3]",
      "false")

    testSqlApi(
      "f2 <> f5[1]",
      "false")

    testSqlApi(
      "f2 = f7",
      "false")

    testSqlApi(
      "f2 <> f7",
      "true")

    testSqlApi(
      "f11 = f11",
      "true")

    testSqlApi(
      "f11 = f9",
      "true")

    testSqlApi(
      "f11 <> f11",
      "false")

    testSqlApi(
      "f11 <> f9",
      "false")
  }
}
