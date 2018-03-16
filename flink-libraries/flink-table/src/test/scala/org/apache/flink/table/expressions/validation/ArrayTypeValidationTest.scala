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

package org.apache.flink.table.expressions.validation

import org.apache.flink.table.api.ValidationException
import org.apache.flink.table.api.scala._
import org.apache.flink.table.expressions.utils.ArrayTypeTestBase
import org.junit.Test

class ArrayTypeValidationTest extends ArrayTypeTestBase {

  @Test(expected = classOf[ValidationException])
  def testImplicitTypeCastTableApi(): Unit = {
    testTableApi(array(1.0, 2.0f), "FAIL", "FAIL")
  }

  @Test(expected = classOf[ValidationException])
  def testImplicitTypeCastArraySql(): Unit = {
    testSqlApi("ARRAY['string', 12]", "FAIL")
  }

  @Test(expected = classOf[ValidationException])
  def testObviousInvalidIndexTableApi(): Unit = {
    testTableApi('f2.at(0), "FAIL", "FAIL")
  }

  @Test(expected = classOf[ValidationException])
  def testEmptyArraySql(): Unit = {
    testSqlApi("ARRAY[]", "FAIL")
  }

  @Test(expected = classOf[ValidationException])
  def testEmptyArrayTableApi(): Unit = {
    testTableApi("FAIL", "array()", "FAIL")
  }

  @Test(expected = classOf[ValidationException])
  def testNullArraySql(): Unit = {
    testSqlApi("ARRAY[NULL]", "FAIL")
  }

  @Test(expected = classOf[ValidationException])
  def testDifferentTypesArraySql(): Unit = {
    testSqlApi("ARRAY[1, TRUE]", "FAIL")
  }

  @Test(expected = classOf[ValidationException])
  def testDifferentTypesArrayTableApi(): Unit = {
    testTableApi("FAIL", "array(1, true)", "FAIL")
  }

  @Test(expected = classOf[ValidationException])
  def testUnsupportedComparison(): Unit = {
    testAllApis(
      'f2 <= 'f5.at(1),
      "f2 <= f5.at(1)",
      "f2 <= f5[1]",
      "FAIL")
  }

  @Test(expected = classOf[ValidationException])
  def testElementNonArray(): Unit = {
    testTableApi(
      'f0.element(),
      "FAIL",
      "FAIL")
  }

  @Test(expected = classOf[ValidationException])
  def testElementNonArraySql(): Unit = {
    testSqlApi(
      "ELEMENT(f0)",
      "FAIL")
  }

  @Test(expected = classOf[ValidationException])
  def testCardinalityOnNonArray(): Unit = {
    testTableApi('f0.cardinality(), "FAIL", "FAIL")
  }

  @Test(expected = classOf[ValidationException])
  def testCardinalityOnNonArraySql(): Unit = {
    testSqlApi("CARDINALITY(f0)", "FAIL")
  }
}
