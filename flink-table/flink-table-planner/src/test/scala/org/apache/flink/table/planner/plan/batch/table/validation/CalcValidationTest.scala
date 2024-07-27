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
package org.apache.flink.table.planner.plan.batch.table.validation

import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.planner.utils.TableTestBase

import org.assertj.core.api.Assertions.assertThatExceptionOfType
import org.junit.jupiter.api.Test

class CalcValidationTest extends TableTestBase {

  @Test
  def testSelectInvalidFieldFields(): Unit = {
    val util = batchTestUtil()

    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(
        () =>
          util
            .addTableSource[(Int, Long, String)]("Table3", 'a, 'b, 'c)
            // must fail. Field 'foo does not exist
            .select('a, 'foo))
      .withMessageContaining("Cannot resolve field [foo], input field list:[a, b, c].")
  }

  @Test
  def testFilterInvalidFieldName(): Unit = {
    val util = batchTestUtil()
    val t = util.addTableSource[(Int, Long, String)]("Table3", 'a, 'b, 'c)

    // must fail. Field 'foo does not exist
    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(() => t.filter('foo === 2))
  }

  @Test
  def testSelectInvalidField() {
    val util = batchTestUtil()
    val t = util.addTableSource[(Int, Long, String)]("Table3", 'a, 'b, 'c)

    // Must fail. Field foo does not exist
    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(() => t.select($"a" + 1, $"foo" + 2))
  }

  @Test
  def testSelectAmbiguousFieldNames() {
    val util = batchTestUtil()
    val t = util.addTableSource[(Int, Long, String)]("Table3", 'a, 'b, 'c)

    // Must fail. Field foo does not exist
    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(() => t.select(($"a" + 1).as("foo"), ($"b" + 2).as("foo")))
  }

  @Test
  def testFilterInvalidField() {
    val util = batchTestUtil()
    val t = util.addTableSource[(Int, Long, String)]("Table3", 'a, 'b, 'c)

    // Must fail. Field foo does not exist.
    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(() => t.filter($"foo" === 17))
  }

  @Test
  def testAliasStarException(): Unit = {
    val util = batchTestUtil()

    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(() => util.addTableSource[(Int, Long, String)]("Table1", '*, 'b, 'c))

    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(() => util.addTableSource[(Int, Long, String)]("Table3").as("*", "b", "c"))

    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(
        () => util.addTableSource[(Int, Long, String)]("Table4", 'a, 'b, 'c).select('*, 'b))
  }

  @Test
  def testDuplicateFlattening(): Unit = {
    val util = batchTestUtil()
    val table = util.addTableSource[((Int, Long), (String, Boolean), String)]("MyTable", 'a, 'b, 'c)

    assertThatExceptionOfType(classOf[ValidationException])
      .isThrownBy(() => table.select('a.flatten(), 'a.flatten()))
  }
}
