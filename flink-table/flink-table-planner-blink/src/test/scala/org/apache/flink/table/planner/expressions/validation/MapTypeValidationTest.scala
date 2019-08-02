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

package org.apache.flink.table.planner.expressions.validation

import org.apache.flink.table.api.ValidationException
import org.apache.flink.table.api.scala._
import org.apache.flink.table.planner.expressions.utils.MapTypeTestBase

import org.junit.Test

class MapTypeValidationTest extends MapTypeTestBase {

  @Test(expected = classOf[ValidationException])
  def testWrongKeyType(): Unit = {
    testAllApis('f2.at(12), "f2.at(12)", "f2[12]", "FAIL")
  }

  @Test(expected = classOf[ValidationException])
  def testIncorrectMapTypeComparison(): Unit = {
    testAllApis('f1 === 'f3, "f1 === f3", "f1 = f3", "FAIL")
  }

  @Test(expected = classOf[ValidationException])
  def testUnsupportedComparisonType(): Unit = {
    testAllApis('f6 !== 'f2, "f6 !== f2", "f6 != f2", "FAIL")
    testSqlApi("f6 <> f2", "FAIL")
  }

  @Test(expected = classOf[ValidationException])
  def testEmptyMap(): Unit = {
    testAllApis("FAIL", "map()", "MAP[]", "FAIL")
    testSqlApi("MAP[]", "FAIL")
  }

  @Test(expected = classOf[ValidationException])
  def testUnsupportedMapImplicitTypeCastSql(): Unit = {
    testSqlApi("MAP['k1', 'string', 'k2', 12]", "FAIL")
  }
}
