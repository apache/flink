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

package org.apache.flink.table.api.batch.table.validation

import org.apache.flink.api.scala._
import org.apache.flink.table.api.ValidationException
import org.apache.flink.table.api.scala._
import org.apache.flink.table.utils.TableTestBase
import org.apache.flink.types.Row
import org.junit.Assert._
import org.junit._

class CalcValidationTest extends TableTestBase {

  @Test
  def testSelectInvalidFieldFields(): Unit = {
    expectedException.expect(classOf[ValidationException])
    expectedException.expectMessage("Cannot resolve field [foo], input field list:[a, b, c].")
    val util = batchTestUtil()
    val t = util.addTable[(Int, Long, String)]("Table3",'a, 'b, 'c)
      // must fail. Field 'foo does not exist
      .select('a, 'foo)
  }

  @Test(expected = classOf[ValidationException])
  def testSelectAmbiguousRenaming(): Unit = {
    val util = batchTestUtil()
    val t = util.addTable[(Int, Long, String)]("Table3",'a, 'b, 'c)
      // must fail. 'a and 'b are both renamed to 'foo
      .select('a + 1 as 'foo, 'b + 2 as 'foo).toDataSet[Row].print()
  }

  @Test(expected = classOf[ValidationException])
  def testSelectAmbiguousRenaming2(): Unit = {
    val util = batchTestUtil()
    val t = util.addTable[(Int, Long, String)]("Table3",'a, 'b, 'c)
      // must fail. 'a and 'b are both renamed to 'a
      .select('a, 'b as 'a).toDataSet[Row].print()
  }

  @Test(expected = classOf[ValidationException])
  def testFilterInvalidFieldName(): Unit = {
    val util = batchTestUtil()
    val t = util.addTable[(Int, Long, String)]("Table3",'a, 'b, 'c)

    // must fail. Field 'foo does not exist
    t.filter( 'foo === 2 )
  }

  @Test(expected = classOf[ValidationException])
  def testSelectInvalidField() {
    val util = batchTestUtil()
    val t = util.addTable[(Int, Long, String)]("Table3",'a, 'b, 'c)

    // Must fail. Field foo does not exist
    t.select($"a" + 1, $"foo" + 2)
  }

  @Test(expected = classOf[ValidationException])
  def testSelectAmbiguousFieldNames() {
    val util = batchTestUtil()
    val t = util.addTable[(Int, Long, String)]("Table3",'a, 'b, 'c)

    // Must fail. Field foo does not exist
    t.select($"a" + 1 as "foo", $"b" + 2 as "foo")
  }

  @Test(expected = classOf[ValidationException])
  def testFilterInvalidField() {
    val util = batchTestUtil()
    val t = util.addTable[(Int, Long, String)]("Table3",'a, 'b, 'c)

    // Must fail. Field foo does not exist.
    t.filter($"foo" === 17)
  }

  @Test
  def testAliasStarException(): Unit = {
    val util = batchTestUtil()

    try {
      util.addTable[(Int, Long, String)]("Table1", '*, 'b, 'c)
      fail("TableException expected")
    } catch {
      case _: ValidationException => //ignore
    }

    try {
      util.addTable[(Int, Long, String)]("Table2")
      .select('_1 as '*, '_2 as 'b, '_1 as 'c)
      fail("ValidationException expected")
    } catch {
      case _: ValidationException => //ignore
    }

    try {
      util.addTable[(Int, Long, String)]("Table3").as("*", "b", "c")
      fail("ValidationException expected")
    } catch {
      case _: ValidationException => //ignore
    }
    try {
      util.addTable[(Int, Long, String)]("Table4", 'a, 'b, 'c).select('*, 'b)
      fail("ValidationException expected")
    } catch {
      case _: ValidationException => //ignore
    }
  }

  @Test(expected = classOf[ValidationException])
  def testDuplicateFlattening(): Unit = {
    val util = batchTestUtil()
    val table = util.addTable[((Int, Long), (String, Boolean), String)]("MyTable", 'a, 'b, 'c)

    table.select('a.flatten(), 'a.flatten())
  }
}
