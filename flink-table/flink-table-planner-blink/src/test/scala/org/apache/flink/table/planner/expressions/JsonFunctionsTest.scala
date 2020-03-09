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

import org.apache.flink.table.planner.expressions.utils.ScalarTypesTestBase
import org.junit.Test

class JsonFunctionsTest extends ScalarTypesTestBase {
  //-------------------------------------------------------------------
  // JSON functions
  //-------------------------------------------------------------------
  @Test
  def testJsonValue(): Unit = {
    // type casting test
    testSqlApi("json_value('{\"foo\":100}', 'strict $.foo')", "100")
    testSqlApi("json_value('{\"foo\":100}', 'strict $.foo' returning integer)", "100")
    testSqlApi("json_value('{\"foo\":100}', 'lax $.foo1' returning integer "
      + "null on empty)", "null")
    testSqlApi("json_value('{\"foo\":\"100\"}', 'strict $.foo1' returning boolean "
      + "null on error)", "null")

    // lax test
    testSqlApi("json_value('{\"foo\":100}', 'lax $.foo' null on empty)", "100")
    testSqlApi("json_value('{\"foo\":100}', 'lax $.foo' error on empty)", "100")
    testSqlApi("json_value('{\"foo\":100}', 'lax $.foo' default 'empty' on empty)", "100")
    testSqlApi("json_value('{\"foo\":100}', 'lax $.foo1' null on empty)", "null")
    testSqlApi("json_value('{\"foo\":100}', 'lax $.foo1' default 'empty' on empty)", "empty")
    testSqlApi("json_value('{\"foo\":{}}', 'lax $.foo' null on empty)", "null")
    testSqlApi("json_value('{\"foo\":{}}', 'lax $.foo' default 'empty' on empty)",
      "empty")
    testSqlApi("json_value('{\"foo\":100}', 'lax $.foo' null on error)", "100")
    testSqlApi("json_value('{\"foo\":100}', 'lax $.foo' error on error)", "100")
    testSqlApi("json_value('{\"foo\":100}', 'lax $.foo' default 'empty' on error)",
      "100")

    // path error test
    testSqlApi("json_value('{\"foo\":100}', 'invalid $.foo' null on error)", "null")
    testSqlApi("json_value('{\"foo\":100}', " + "'invalid $.foo' default 'empty' on error)",
      "empty")

    // strict test
    testSqlApi("json_value('{\"foo\":100}', 'strict $.foo' null on empty)", "100")
    testSqlApi("json_value('{\"foo\":100}', 'strict $.foo' error on empty)", "100")
    testSqlApi("json_value('{\"foo\":100}', " + "'strict $.foo' default 'empty' on empty)",
      "100")
    testSqlApi("json_value('{\"foo\":100}', 'strict $.foo1' null on error)", "null")
    testSqlApi("json_value('{\"foo\":100}', " + "'strict $.foo1' default 'empty' on error)",
      "empty")
    testSqlApi("json_value('{\"foo\":{}}', 'strict $.foo' null on error)", "null")
    testSqlApi("json_value('{\"foo\":{}}', " + "'strict $.foo' default 'empty' on error)",
      "empty")

    // nulls
    testSqlApi("json_value(cast(null as varchar), 'strict $')", "null")
  }
}
