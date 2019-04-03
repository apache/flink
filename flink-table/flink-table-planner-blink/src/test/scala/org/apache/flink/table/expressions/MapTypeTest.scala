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

import org.apache.flink.table.expressions.utils.MapTypeTestBase
import org.junit.Test

class MapTypeTest extends MapTypeTestBase {

  @Test
  def testItem(): Unit = {
    testSqlApi("f0['map is null']", "null")
    testSqlApi("f1['map is empty']", "null")
    testSqlApi("f2['b']", "13")
    testSqlApi("f3[1]", "null")
    testSqlApi("f3[12]", "a")
  }

  @Test
  def testMapLiteral(): Unit = {
    // primitive literals
    testSqlApi(
      "MAP[1, 1]",
      "{1=1}")

    testSqlApi(
      "map[TRUE, TRUE]",
      "{true=true}")

    testSqlApi(
      "MAP[MAP[1, 2], MAP[3, 4]]",
      "{{1=2}={3=4}}")

    testSqlApi(
      "map[1 + 2, 3 * 3, 3 - 6, 4 - 2]",
      "{3=9, -3=2}")

    testSqlApi(
      "map[1, NULLIF(1,1)]",
      "{1=null}")

    // explicit conversion
    testSqlApi(
      "MAP[1, CAST(2 AS BIGINT), 3, CAST(4 AS BIGINT)]",
      "{1=2, 3=4}")

    testSqlApi(
      "MAP[DATE '1985-04-11', TIME '14:15:16', DATE '2018-07-26', TIME '17:18:19']",
      "{1985-04-11=14:15:16, 2018-07-26=17:18:19}")

    testSqlApi(
      "MAP[TIME '14:15:16', TIMESTAMP '1985-04-11 14:15:16', " +
        "TIME '17:18:19', TIMESTAMP '2018-07-26 17:18:19']",
      "{14:15:16=1985-04-11 14:15:16.000, 17:18:19=2018-07-26 17:18:19.000}")

    testSqlApi(
      "MAP[CAST(2.0002 AS DECIMAL(5, 4)), CAST(2.0003 AS DECIMAL(5, 4))]",
      "{2.0002=2.0003}")

    // implicit type cast only works on SQL API
    testSqlApi(
      "MAP['k1', CAST(1 AS DOUBLE), 'k2', CAST(2 AS FLOAT)]",
      "{k1=1.0, k2=2.0}")
  }

  @Test
  def testMapField(): Unit = {
    testSqlApi(
      "MAP[f4, f5]",
      "{foo=12}")

    testSqlApi(
      "MAP[f4, f1]",
      "{foo={}}")

    testSqlApi(
      "MAP[f2, f3]",
      "{{a=12, b=13}={12=a, 13=b}}")

    testSqlApi(
      "MAP[f1['a'], f5]",
      "{null=12}")

    testSqlApi(
      "f1",
      "{}")

    testSqlApi(
      "f2",
      "{a=12, b=13}")

    testSqlApi(
      "f2['a']",
      "12")

    testSqlApi(
      "f3[12]",
      "a")

    testSqlApi(
      "MAP[f4, f3]['foo'][13]",
      "b")
  }

  @Test
  def testMapOperations(): Unit = {

    // comparison
    testSqlApi(
      "f1 = f2",
      "false")

    testSqlApi(
      "f3 = f7",
      "true")

    testSqlApi(
      "f5 = f2['a']",
      "true")

    testSqlApi(
      "f8 = f9",
      "true")

    testSqlApi(
      "f10 = f11",
      "true")

    testSqlApi(
      "f8 <> f9",
      "false")

    testSqlApi(
      "f10 <> f11",
      "false")

    testSqlApi(
      "f0['map is null']",
      "null")

    testSqlApi(
      "f1['map is empty']",
      "null")

    testSqlApi(
      "f2['b']",
      "13")

    testSqlApi(
      "f3[1]",
      "null")

    testSqlApi(
      "f3[12]",
      "a")

    testSqlApi(
      "CARDINALITY(f3)",
      "2")

    testSqlApi(
      "f2['a'] IS NOT NULL",
      "true")

    testSqlApi(
      "f2['a'] IS NULL",
      "false")

    testSqlApi(
      "f2['c'] IS NOT NULL",
      "false")

    testSqlApi(
      "f2['c'] IS NULL",
      "true")
  }
}
