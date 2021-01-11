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

import org.apache.flink.table.api._
import org.apache.flink.table.expressions.utils.ArrayTypeTestBase

import org.junit.jupiter.api.Test

class ArrayTypeValidationTest extends ArrayTypeTestBase {

  @Test
  def testImplicitTypeCastArraySql(): Unit = {
        assertThrows[ValidationException] {
                testSqlApi("ARRAY['string', 12]", "FAIL")
        }
    }

  @Test
  def testObviousInvalidIndexTableApi(): Unit = {
        assertThrows[ValidationException] {
                testTableApi('f2.at(0), "FAIL", "FAIL")
        }
    }

  @Test
  def testEmptyArraySql(): Unit = {
        assertThrows[ValidationException] {
                testSqlApi("ARRAY[]", "FAIL")
        }
    }

  @Test
  def testEmptyArrayTableApi(): Unit = {
        assertThrows[ValidationException] {
                testTableApi("FAIL", "array()", "FAIL")
        }
    }

  @Test
  def testNullArraySql(): Unit = {
        assertThrows[ValidationException] {
                testSqlApi("ARRAY[NULL]", "FAIL")
        }
    }

  @Test
  def testDifferentTypesArraySql(): Unit = {
        assertThrows[ValidationException] {
                testSqlApi("ARRAY[1, TRUE]", "FAIL")
        }
    }

  @Test
  def testDifferentTypesArrayTableApi(): Unit = {
        assertThrows[ValidationException] {
                testTableApi("FAIL", "array(1, true)", "FAIL")
        }
    }

  @Test
  def testElementNonArray(): Unit = {
        assertThrows[ValidationException] {
                testTableApi(
      'f0.element(),
      "FAIL",
      "FAIL")
        }
    }

  @Test
  def testElementNonArraySql(): Unit = {
        assertThrows[ValidationException] {
                testSqlApi(
      "ELEMENT(f0)",
      "FAIL")
        }
    }

  @Test
  def testCardinalityOnNonArray(): Unit = {
        assertThrows[ValidationException] {
                testTableApi('f0.cardinality(), "FAIL", "FAIL")
        }
    }

  @Test
  def testCardinalityOnNonArraySql(): Unit = {
        assertThrows[ValidationException] {
                testSqlApi("CARDINALITY(f0)", "FAIL")
        }
    }
}
