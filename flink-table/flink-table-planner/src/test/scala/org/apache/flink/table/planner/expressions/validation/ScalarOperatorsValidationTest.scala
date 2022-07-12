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
import org.apache.flink.table.planner.expressions.utils.ScalarOperatorsTestBase

import org.junit.Test

class ScalarOperatorsValidationTest extends ScalarOperatorsTestBase {

  @Test(expected = classOf[ValidationException])
  def testIfInvalidTypesScala(): Unit = {
    testTableApi(('f6 && true).?(5, "false"), "FAIL")
  }

  @Test(expected = classOf[ValidationException])
  def testInvalidStringComparison1(): Unit = {
    testTableApi("w" === 4, "FAIL")
  }

  @Test(expected = classOf[ValidationException])
  def testInvalidStringComparison2(): Unit = {
    testTableApi("w" > 4.toExpr, "FAIL")
  }

  // ----------------------------------------------------------------------------------------------
  // Sub-query functions
  // ----------------------------------------------------------------------------------------------

  @Test(expected = classOf[ValidationException])
  def testInMoreThanOneTypes(): Unit = {
    testTableApi('f2.in('f3, 'f4, 4), "FAIL")
  }

  @Test(expected = classOf[ValidationException])
  def testInDifferentOperands(): Unit = {
    testTableApi('f1.in("Hi", "Hello world", "Comment#1"), "FAIL")
  }

  @Test(expected = classOf[ValidationException])
  def testBetweenWithDifferentOperandTypeScala(): Unit = {
    testTableApi(2.between(1, "a"), "FAIL")
  }

  @Test
  def testTemporalTypeEqualsInvalidStringLiteral(): Unit = {
    testExpectedSqlException(
      "f15 = 'invalid'",
      "Error when casting CHAR(7) NOT NULL to DATE",
      classOf[ValidationException])
    testExpectedSqlException(
      "'invalid' = f15",
      "Error when casting CHAR(7) NOT NULL to DATE",
      classOf[ValidationException])

    testExpectedSqlException(
      "f21 = 'invalid'",
      "Error when casting CHAR(7) NOT NULL to TIME",
      classOf[ValidationException])
    testExpectedSqlException(
      "'invalid' = f21",
      "Error when casting CHAR(7) NOT NULL to TIME",
      classOf[ValidationException])

    testExpectedSqlException(
      "f22 = 'invalid'",
      "Error when casting CHAR(7) NOT NULL to TIMESTAMP(6)",
      classOf[ValidationException])
    testExpectedSqlException(
      "'invalid' = f22",
      "Error when casting CHAR(7) NOT NULL to TIMESTAMP(6)",
      classOf[ValidationException])
  }
}
