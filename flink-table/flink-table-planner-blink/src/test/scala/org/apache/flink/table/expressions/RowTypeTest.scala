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

import org.apache.flink.table.expressions.utils.RowTypeTestBase
import org.junit.Test

class RowTypeTest extends RowTypeTestBase {

  @Test
  def testRowLiteral(): Unit = {

    // primitive literal
    testSqlApi(
      "ROW(1, 'foo', true)",
      "(1,foo,true)")

    // special literal
    testSqlApi(
      "ROW(DATE '1985-04-11', TIME '14:15:16', TIMESTAMP '1985-04-11 14:15:16', " +
        "CAST(0.1 AS DECIMAL(2, 1)), ARRAY[1, 2, 3], MAP['foo', 'bar'], row(1, true))",
      "(1985-04-11,14:15:16,1985-04-11 14:15:16.000,0.1,[1, 2, 3],{foo=bar},(1,true))")

    testSqlApi(
      "ROW(1 + 1, 2 * 3, NULLIF(1, 1))",
      "(2,6,null)"
    )

    testSqlApi("(1, 'foo', true)", "(1,foo,true)")
  }

  @Test
  def testRowField(): Unit = {
    testSqlApi(
      "(f0, f1)",
      "(null,1)"
    )

    testSqlApi(
      "f2",
      "(2,foo,true)"
    )

    testSqlApi(
      "(f2, f5)",
      "((2,foo,true),(foo,null))"
    )

    testSqlApi(
      "f4",
      "(1984-03-12,0.00000000,[1, 2, 3])"
    )

    testSqlApi(
      "(f1, 'foo',true)",
      "(1,foo,true)"
    )
  }

  @Test
  def testRowOperations(): Unit = {
    testSqlApi(
      "f5.f0",
      "foo"
    )

    testSqlApi(
      "f3.f1.f2",
      "true"
    )
  }
}
