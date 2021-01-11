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

import org.junit.jupiter.api.Test

class ScalarOperatorsValidationTest extends ScalarOperatorsTestBase {

  @Test
  def testIfInvalidTypesScala(): Unit = {
        assertThrows[ValidationException] {
                testTableApi(('f6 && true).?(5, "false"), "FAIL", "FAIL")
        }
    }

  @Test
  def testIfInvalidTypesJava(): Unit = {
        assertThrows[ValidationException] {
                testTableApi("FAIL", "(f8 && true).?(5, 'false')", "FAIL")
        }
    }

  @Test
  def testInvalidStringComparison1(): Unit = {
        assertThrows[ValidationException] {
                testTableApi("w" === 4, "FAIL", "FAIL")
        }
    }

  @Test
  def testInvalidStringComparison2(): Unit = {
        assertThrows[ValidationException] {
                testTableApi("w" > 4.toExpr, "FAIL", "FAIL")
        }
    }

  // ----------------------------------------------------------------------------------------------
  // Sub-query functions
  // ----------------------------------------------------------------------------------------------

  @Test
  def testInMoreThanOneTypes(): Unit = {
        assertThrows[ValidationException] {
                testTableApi(
      'f2.in('f3, 'f4, 4),
      "FAIL",
      "FAIL"
    )
        }
    }

  @Test
  def testInDifferentOperands(): Unit = {
        assertThrows[ValidationException] {
                testTableApi(
      'f1.in("Hi", "Hello world", "Comment#1"),
      "FAIL",
      "FAIL"
    )
        }
    }

  @Test
  def testBetweenWithDifferentOperandTypeScala(): Unit = {
        assertThrows[ValidationException] {
                testTableApi(
      2.between(1, "a"),
      "FAIL",
      "FAIL"
    )
        }
    }

  @Test
  def testBetweenWithDifferentOperandTypeJava(): Unit = {
        assertThrows[ValidationException] {
                testTableApi(
      "FAIL",
      "2.between(1, 'a')",
      "FAIL"
    )
        }
    }
}
