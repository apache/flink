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

import org.apache.flink.table.api._
import org.apache.flink.table.expressions.utils.MapTypeTestBase

import org.junit.Test

import java.sql.Date

class MapTypeTest extends MapTypeTestBase {

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
      map(1 + 2, 3 * 3, 6 / 3, 4 - 2),
      "map(1 + 2, 3 * 3, 6 / 3, 4 - 2)",
      "map[1 + 2, 3 * 3, 6 / 3, 4 - 2]",
      "{2=2, 3=9}")

    testAllApis(
      map(1, nullOf(Types.INT)),
      "map(1, nullOf(INT))",
      "map[1, NULLIF(1,1)]",
      "{1=null}")

    // explicit conversion
    testAllApis(
      map(1, 2L , 3, 4L),
      "map(1, 2L, 3, 4L)",
      "MAP[1, CAST(2 AS BIGINT), 3, CAST(4 AS BIGINT)]",
      "{1=2, 3=4}")

    testAllApis(
      map(Date.valueOf("1985-04-11"), 1),
      "map('1985-04-11'.toDate, 1)",
      "MAP[DATE '1985-04-11', 1]",
      "{1985-04-11=1}")

    testAllApis(
      map(BigDecimal(2.0002), BigDecimal(2.0003)),
      "map(2.0002p, 2.0003p)",
      "MAP[CAST(2.0002 AS DECIMAL), CAST(2.0003 AS DECIMAL)]",
      "{2.0002=2.0003}")

    // implicit conversion
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
      "true"
    )

    testAllApis(
      'f2.at("a").isNull,
      "f2.at('a').isNull",
      "f2['a'] IS NULL",
      "false"
    )

    testAllApis(
      'f2.at("c").isNotNull,
      "f2.at('c').isNotNull",
      "f2['c'] IS NOT NULL",
      "false"
    )

    testAllApis(
      'f2.at("c").isNull,
      "f2.at('c').isNull",
      "f2['c'] IS NULL",
      "true"
    )
  }

  @Test
  def testMapTypeCasting(): Unit = {
    testTableApi(
      'f2.cast(Types.MAP(Types.STRING, Types.INT)),
      "f2.cast(MAP(STRING, INT))",
      "{a=12, b=13}"
    )
  }
}
