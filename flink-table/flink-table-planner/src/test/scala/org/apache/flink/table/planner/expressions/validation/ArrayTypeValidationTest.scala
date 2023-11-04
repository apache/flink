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
import org.apache.flink.table.planner.expressions.utils.ArrayTypeTestBase

import org.assertj.core.api.Assertions.assertThatExceptionOfType
import org.junit.jupiter.api.Test

class ArrayTypeValidationTest extends ArrayTypeTestBase {

  @Test
  def testImplicitTypeCastArraySql(): Unit = {
    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(() => testSqlApi("ARRAY['string', 12]", "FAIL"))
  }

  @Test
  def testObviousInvalidIndexTableApi(): Unit = {
    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(() => testTableApi('f2.at(0), "FAIL"))
  }

  @Test
  def testEmptyArraySql(): Unit = {
    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(() => testSqlApi("ARRAY[]", "FAIL"))
  }

  @Test
  def testNullArraySql(): Unit = {
    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(() => testSqlApi("ARRAY[NULL]", "FAIL"))
  }

  @Test
  def testDifferentTypesArraySql(): Unit = {
    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(() => testSqlApi("ARRAY[1, TRUE]", "FAIL"))
  }

  @Test
  def testElementNonArray(): Unit = {
    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(() => testTableApi('f0.element(), "FAIL"))
  }

  @Test
  def testElementNonArraySql(): Unit = {
    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(() => testSqlApi("ELEMENT(f0)", "FAIL"))
  }

  @Test
  def testCardinalityOnNonArray(): Unit = {
    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(() => testTableApi('f0.cardinality(), "FAIL"))
  }

  @Test
  def testCardinalityOnNonArraySql(): Unit = {
    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(() => testSqlApi("CARDINALITY(f0)", "FAIL"))
  }
}
