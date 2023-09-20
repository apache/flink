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

import org.apache.flink.table.api._
import org.apache.flink.table.planner.expressions.utils.MapTypeTestBase

import org.assertj.core.api.Assertions.assertThatExceptionOfType
import org.junit.jupiter.api.Test

class MapTypeValidationTest extends MapTypeTestBase {

  @Test
  def testWrongKeyType(): Unit = {
    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(() => testAllApis('f2.at(12), "f2[12]", "FAIL"))
  }

  @Test
  def testIncorrectMapTypeComparison(): Unit = {
    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(() => testAllApis('f1 === 'f3, "f1 = f3", "FAIL"))
  }

  @Test
  def testUnsupportedComparisonType(): Unit = {
    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(() => testAllApis('f6 !== 'f2, "f6 != f2", "FAIL"))

    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(() => testSqlApi("f6 <> f2", "FAIL"))
  }

  @Test
  def testEmptyMap(): Unit = {
    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(() => testAllApis("FAIL", "MAP[]", "FAIL"))

    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(() => testSqlApi("MAP[]", "FAIL"))
  }

  @Test
  def testUnsupportedMapImplicitTypeCastSql(): Unit = {
    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(() => testSqlApi("MAP['k1', 'string', 'k2', 12]", "FAIL"))
  }
}
