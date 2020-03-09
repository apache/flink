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
  def testJsonQuery(): Unit = {
    testSqlApi("json_array()", "[]")
    testSqlApi("json_array('foo')", "[\"foo\"]")
    testSqlApi("json_array('foo', 'bar')", "[\"foo\",\"bar\"]")
    testSqlApi("json_array(null)", "[]")
    testSqlApi("json_array(null null on null)", "[null]")
    testSqlApi("json_array(null absent on null)", "[]")
    testSqlApi("json_array(100)", "[100]")
    testSqlApi("json_array(json_array('foo'))", "[\"[\\\"foo\\\"]\"]")

    testSqlApi("json_array(f36, f28)", "[\"b\",0.45]")
  }
}
