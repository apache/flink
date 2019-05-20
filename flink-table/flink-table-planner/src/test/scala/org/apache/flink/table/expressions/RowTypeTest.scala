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
import org.apache.flink.table.expressions.utils.RowTypeTestBase
import org.junit.Test

class RowTypeTest extends RowTypeTestBase {

  @Test
  def testRowLiteral(): Unit = {

    // primitive literal
    testAllApis(
      row(1, "foo", true),
      "row(1, 'foo', true)",
      "ROW(1, 'foo', true)",
      "1,foo,true")

    // special literal
    testAllApis(
      row(Date.valueOf("1985-04-11"),
        BigDecimal("0.1").bigDecimal,
        array(1, 2, 3),
        map("foo", "bar"),
        row(1, true)
      ),
      "row('1985-04-11'.toDate, 0.1p, Array(1, 2, 3), " +
        "Map('foo', 'bar'), row(1, true))",
      "ROW(DATE '1985-04-11', CAST(0.1 AS DECIMAL), ARRAY[1, 2, 3], " +
        "MAP['foo', 'bar'], row(1, true))",
      "1985-04-11,0.1,[1, 2, 3],{foo=bar},1,true") // string flatten

    testAllApis(
      row(1 + 1, 2 * 3, nullOf(Types.STRING)),
      "row(1 + 1, 2 * 3, nullOf(STRING))",
      "ROW(1 + 1, 2 * 3, NULLIF(1,1))",
      "2,6,null"
    )

    testSqlApi("(1, 'foo', true)", "1,foo,true")
  }

  @Test
  def testRowField(): Unit = {
    testAllApis(
      row('f0, 'f1),
      "row(f0, f1)",
      "(f0, f1)",
      "null,1"
    )

    testAllApis(
      'f2,
      "f2",
      "f2",
      "2,foo,true"
    )

    testAllApis(
      row('f2, 'f5),
      "row(f2, f5)",
      "(f2, f5)",
      "2,foo,true,foo,null"
    )

    testAllApis(
      'f4,
      "f4",
      "f4",
      "1984-03-12,0E-8,[1, 2, 3]"
    )

    testAllApis(
      row('f1, "foo", true),
      "row(f1, 'foo', true)",
      "(f1, 'foo',true)",
      "1,foo,true"
    )
  }

  @Test
  def testRowOperations(): Unit = {
    testAllApis(
      'f5.get("f0"),
      "f5.get('f0')",
      "f5.f0",
      "foo"
    )

    testAllApis(
      'f3.get("f1").get("f2"),
      "f3.get('f1').get('f2')",
      "f3.f1.f2",
      "true"
    )

    // SQL API for row value constructor follow by field access is not supported
    testTableApi(
      row('f1, 'f6, 'f2).get("f1").get("f1"),
      "row(f1, f6, f2).get('f1').get('f1')",
      "null"
    )
  }
}
