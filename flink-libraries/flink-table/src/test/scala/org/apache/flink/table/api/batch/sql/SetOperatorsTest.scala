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

package org.apache.flink.table.api.batch.sql

import org.apache.flink.api.java.typeutils.GenericTypeInfo
import org.apache.flink.api.scala._
import org.apache.flink.table.api.Types
import org.apache.flink.table.api.scala._
import org.apache.flink.table.runtime.utils.CommonTestData.NonPojo
import org.apache.flink.table.util.TableTestBase
import org.apache.flink.types.Row
import org.junit.{Ignore, Test}

class SetOperatorsTest extends TableTestBase {

  @Test
  def testMinusWithNestedTypes(): Unit = {
    val util = batchTestUtil()
    val t = util.addTable[(Long, (Int, String), Array[Boolean])]("MyTable", 'a, 'b, 'c)

    val result = t.minus(t)

    util.verifyPlan(result)
  }

  @Test
  def testExists(): Unit = {
    val util = batchTestUtil()
    util.addTable[(Long, Int, String)]("A", 'a_long, 'a_int, 'a_string)
    util.addTable[(Long, Int, String)]("B", 'b_long, 'b_int, 'b_string)

    util.verifyPlan(
      "SELECT a_int, a_string FROM A WHERE EXISTS(SELECT * FROM B WHERE a_long = b_long)"
    )
  }

  @Test
  def testNotIn(): Unit = {
    val util = batchTestUtil()
    util.addTable[(Int, Long, String)]("A", 'a, 'b, 'c)
    util.verifyPlan(
      "SELECT a, c FROM A WHERE b NOT IN (SELECT b FROM A WHERE b = 6 OR b = 1)"
    )
  }

  @Test
  def testInWithFields(): Unit = {
    val util = batchTestUtil()
    util.addTable[(Int, Long, Int, String, Long)]("A", 'a, 'b, 'c, 'd, 'e)

    util.verifyPlan(
      "SELECT a, b, c, d, e FROM A WHERE a IN (c, b, 5)"
    )
  }

  @Test
  @Ignore // Calcite bug
  def testNotInWithFilter(): Unit = {
    val util = batchTestUtil()
    util.addTable[(Int, Long, String)]("A", 'a, 'b, 'c)
    util.addTable[(Int, Long, Int, String, Long)]("B", 'a, 'b, 'c, 'd, 'e)

    util.verifyPlan(
      "SELECT d FROM B WHERE d NOT IN (SELECT a FROM A) AND d < 5"
    )
  }

  @Test
  def testUnionNullableTypes(): Unit = {
    val util = batchTestUtil()
    util.addTable[((Int, String), (Int, String), Int)]("A", 'a, 'b, 'c)

    util.verifyPlan(
      "SELECT a FROM A UNION ALL SELECT CASE WHEN c > 0 THEN b ELSE NULL END FROM A"
    )
  }

  @Test
  def testUnionAnyType(): Unit = {
    val util = batchTestUtil()
    util.addJavaTable[Row](Types.ROW(
      new GenericTypeInfo(classOf[NonPojo]),
      new GenericTypeInfo(classOf[NonPojo])), "A", 'a, 'b)

    util.verifyPlan("SELECT a FROM A UNION ALL SELECT b FROM A")
  }
}
