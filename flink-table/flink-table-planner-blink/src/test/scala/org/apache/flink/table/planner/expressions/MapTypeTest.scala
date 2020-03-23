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

package org.apache.flink.table.planner.expressions

import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.api.scala._
import org.apache.flink.table.expressions.ApiExpressionUtils.valueLiteral
import org.apache.flink.table.planner.expressions.utils.MapTypeTestBase
import org.apache.flink.table.planner.utils.DateTimeTestUtil.{localDate, localDateTime, localTime => gLocalTime}

import java.time.{LocalDateTime => JLocalTimestamp}

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
    testAllApis(map(1, 1), "map(1, 1)", "MAP[1, 1]", "{1=1}")

    testAllApis(
      map(true, true),
      "map(true, true)",
      "map[TRUE, TRUE]",
      "{true=true}")

    // object literals
    testTableApi(map(BigDecimal(1), BigDecimal(1)), "map(1p, 1p)", "{1=1}")

    testAllApis(
      map(map(1, 2), map(3, 4)),
      "map(map(1, 2), map(3, 4))",
      "MAP[MAP[1, 2], MAP[3, 4]]",
      "{{1=2}={3=4}}")

    testAllApis(
      map(1 + 2, 3 * 3, 3 - 6, 4 - 2),
      "map(1 + 2, 3 * 3, 3 - 6, 4 - 2)",
      "map[1 + 2, 3 * 3, 3 - 6, 4 - 2]",
      "{3=9, -3=2}")

    testAllApis(
      map(1, nullOf(DataTypes.INT)),
      "map(1, Null(INT))",
      "map[1, NULLIF(1,1)]",
      "{1=null}")

    // explicit conversion
    testAllApis(
      map(1, 2L , 3, 4L),
      "map(1, 2L, 3, 4L)",
      "MAP[1, CAST(2 AS BIGINT), 3, CAST(4 AS BIGINT)]",
      "{1=2, 3=4}")

    testAllApis(
      map(valueLiteral(localDate("1985-04-11")), valueLiteral(gLocalTime("14:15:16")),
        valueLiteral(localDate("2018-07-26")), valueLiteral(gLocalTime("17:18:19"))),
      "map('1985-04-11'.toDate, '14:15:16'.toTime, '2018-07-26'.toDate, '17:18:19'.toTime)",
      "MAP[DATE '1985-04-11', TIME '14:15:16', DATE '2018-07-26', TIME '17:18:19']",
      "{1985-04-11=14:15:16, 2018-07-26=17:18:19}")

    // There is no timestamp literal function in Java String Table API,
    // toTimestamp is casting string to TIMESTAMP(3) which is not the same to timestamp literal.
    testTableApi(
      map(valueLiteral(gLocalTime("14:15:16")), valueLiteral(localDateTime("1985-04-11 14:15:16")),
        valueLiteral(gLocalTime("17:18:19")), valueLiteral(localDateTime("2018-07-26 17:18:19"))),
      "{14:15:16=1985-04-11 14:15:16, 17:18:19=2018-07-26 17:18:19}")
    testSqlApi(
      "MAP[TIME '14:15:16', TIMESTAMP '1985-04-11 14:15:16', " +
        "TIME '17:18:19', TIMESTAMP '2018-07-26 17:18:19']",
      "{14:15:16=1985-04-11 14:15:16, 17:18:19=2018-07-26 17:18:19}")

    testAllApis(
      map(valueLiteral(gLocalTime("14:15:16")),
        valueLiteral(localDateTime("1985-04-11 14:15:16.123")),
        valueLiteral(gLocalTime("17:18:19")),
        valueLiteral(localDateTime("2018-07-26 17:18:19.123"))),
      "map('14:15:16'.toTime, '1985-04-11 14:15:16.123'.toTimestamp, " +
        "'17:18:19'.toTime, '2018-07-26 17:18:19.123'.toTimestamp)",
      "MAP[TIME '14:15:16', TIMESTAMP '1985-04-11 14:15:16.123', " +
        "TIME '17:18:19', TIMESTAMP '2018-07-26 17:18:19.123']",
      "{14:15:16=1985-04-11 14:15:16.123, 17:18:19=2018-07-26 17:18:19.123}")

    testTableApi(
      map(valueLiteral(gLocalTime("14:15:16")),
        valueLiteral(JLocalTimestamp.of(1985, 4, 11, 14, 15, 16, 123456000)),
        valueLiteral(gLocalTime("17:18:19")),
        valueLiteral(JLocalTimestamp.of(2018, 7, 26, 17, 18, 19, 123456000))),
      "{14:15:16=1985-04-11 14:15:16.123456, 17:18:19=2018-07-26 17:18:19.123456}")

    testSqlApi(
      "MAP[TIME '14:15:16', TIMESTAMP '1985-04-11 14:15:16.123456', " +
        "TIME '17:18:19', TIMESTAMP '2018-07-26 17:18:19.123456']",
      "{14:15:16=1985-04-11 14:15:16.123456, 17:18:19=2018-07-26 17:18:19.123456}")

    testAllApis(
      map(BigDecimal(2.0002), BigDecimal(2.0003)),
      "map(2.0002p, 2.0003p)",
      "MAP[CAST(2.0002 AS DECIMAL(5, 4)), CAST(2.0003 AS DECIMAL(5, 4))]",
      "{2.0002=2.0003}")

    // implicit type cast only works on SQL API
    testSqlApi("MAP['k1', CAST(1 AS DOUBLE), 'k2', CAST(2 AS FLOAT)]", "{k1=1.0, k2=2.0}")
  }

  @Test
  def testMapField(): Unit = {
    testAllApis(
      map('f4, 'f5),
      "map(f4, f5)",
      "MAP[f4, f5]",
      "{foo=12}")

    testAllApis(
      map('f4, 'f1),
      "map(f4, f1)",
      "MAP[f4, f1]",
      "{foo={}}")

    testAllApis(
      map('f2, 'f3),
      "map(f2, f3)",
      "MAP[f2, f3]",
      "{{a=12, b=13}={12=a, 13=b}}")

    testAllApis(
      map('f1.at("a"), 'f5),
      "map(f1.at('a'), f5)",
      "MAP[f1['a'], f5]",
      "{null=12}")

    testAllApis(
      'f1,
      "f1",
      "f1",
      "{}")

    testAllApis(
      'f2,
      "f2",
      "f2",
      "{a=12, b=13}")

    testAllApis(
      'f2.at("a"),
      "f2.at('a')",
      "f2['a']",
      "12")

    testAllApis(
      'f3.at(12),
      "f3.at(12)",
      "f3[12]",
      "a")

    testAllApis(
      map('f4, 'f3).at("foo").at(13),
      "map(f4, f3).at('foo').at(13)",
      "MAP[f4, f3]['foo'][13]",
      "b")
  }

  @Test
  def testMapOperations(): Unit = {

    // comparison
    testAllApis(
      'f1 === 'f2,
      "f1 === f2",
      "f1 = f2",
      "false")

    testAllApis(
      'f3 === 'f7,
      "f3 === f7",
      "f3 = f7",
      "true")

    testAllApis(
      'f5 === 'f2.at("a"),
      "f5 === f2.at('a')",
      "f5 = f2['a']",
      "true")

    testAllApis(
      'f8 === 'f9,
      "f8 === f9",
      "f8 = f9",
      "true")

    testAllApis(
      'f10 === 'f11,
      "f10 === f11",
      "f10 = f11",
      "true")

    testAllApis(
      'f8 !== 'f9,
      "f8 !== f9",
      "f8 <> f9",
      "false")

    testAllApis(
      'f10 !== 'f11,
      "f10 !== f11",
      "f10 <> f11",
      "false")

    testAllApis(
      'f0.at("map is null"),
      "f0.at('map is null')",
      "f0['map is null']",
      "null")

    testAllApis(
      'f1.at("map is empty"),
      "f1.at('map is empty')",
      "f1['map is empty']",
      "null")

    testAllApis(
      'f2.at("b"),
      "f2.at('b')",
      "f2['b']",
      "13")

    testAllApis(
      'f3.at(1),
      "f3.at(1)",
      "f3[1]",
      "null")

    testAllApis(
      'f3.at(12),
      "f3.at(12)",
      "f3[12]",
      "a")

    testAllApis(
      'f3.cardinality(),
      "f3.cardinality()",
      "CARDINALITY(f3)",
      "2")

    testAllApis(
      'f2.at("a").isNotNull,
      "f2.at('a').isNotNull",
      "f2['a'] IS NOT NULL",
      "true")

    testAllApis(
      'f2.at("a").isNull,
      "f2.at('a').isNull",
      "f2['a'] IS NULL",
      "false")

    testAllApis(
      'f2.at("c").isNotNull,
      "f2.at('c').isNotNull",
      "f2['c'] IS NOT NULL",
      "false")

    testAllApis(
      'f2.at("c").isNull,
      "f2.at('c').isNull",
      "f2['c'] IS NULL",
      "true")
  }

  @Test
  def testMapTypeCasting(): Unit = {
    testTableApi(
      'f2.cast(DataTypes.MAP(DataTypes.STRING, DataTypes.INT)),
      "f2.cast(MAP(STRING, INT))",
      "{a=12, b=13}"
    )
  }
}
