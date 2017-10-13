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

package org.apache.flink.table.api.stream.table.validation

import org.apache.flink.api.scala._
import org.apache.flink.table.api.TableException
import org.apache.flink.table.api.scala._
import org.apache.flink.table.utils.TableTestBase
import org.junit.Test

/**
  * Currently only time-windowed inner joins can be processed in a streaming fashion.
  */
class JoinValidationTest extends TableTestBase {

  /**
    * At least one equi-join predicate required.
    */
  @Test(expected = classOf[TableException])
  def testInnerJoinWithoutEquiPredicate(): Unit = {
    val util = streamTestUtil()
    val left = util.addTable[(Long, Int, String)]('a, 'b, 'c, 'ltime.rowtime)
    val right = util.addTable[(Long, Int, String)]('d, 'e, 'f, 'rtime.rowtime)

    val resultTable = left.join(right)
      .where('ltime >= 'rtime - 5.minutes && 'ltime < 'rtime + 3.seconds)
      .select('a, 'e, 'ltime)

    val expected = ""
    util.verifyTable(resultTable, expected)
  }

  /**
    * There must be complete window-bounds.
    */
  @Test(expected = classOf[TableException])
  def testInnerJoinWithIncompleteWindowBounds1(): Unit = {
    val util = streamTestUtil()
    val left = util.addTable[(Long, Int, String)]('a, 'b, 'c, 'ltime.rowtime)
    val right = util.addTable[(Long, Int, String)]('d, 'e, 'f, 'rtime.rowtime)

    val resultTable = left.join(right)
      .where('a ==='d && 'ltime >= 'rtime - 5.minutes && 'ltime < 'ltime + 3.seconds)
      .select('a, 'e, 'ltime)

    util.verifyTable(resultTable, "")
  }

  /**
    * There must be complete window-bounds.
    */
  @Test(expected = classOf[TableException])
  def testInnerJoinWithIncompleteWindowBounds2(): Unit = {
    val util = streamTestUtil()
    val left = util.addTable[(Long, Int, String)]('a, 'b, 'c, 'ltime.rowtime)
    val right = util.addTable[(Long, Int, String)]('d, 'e, 'f, 'rtime.rowtime)

    val resultTable = left.join(right)
      .where('a ==='d && 'ltime >= 'rtime - 5.minutes && 'ltime > 'rtime + 3.seconds)
      .select('a, 'e, 'ltime)

    util.verifyTable(resultTable, "")
  }

  /**
    * Time indicators for the two tables must be identical.
    */
  @Test(expected = classOf[TableException])
  def testInnerJoinWithDifferentTimeIndicators(): Unit = {
    val util = streamTestUtil()
    val left = util.addTable[(Long, Int, String)]('a, 'b, 'c, 'ltime.proctime)
    val right = util.addTable[(Long, Int, String)]('d, 'e, 'f, 'rtime.rowtime)

    val resultTable = left.join(right)
      .where('a ==='d && 'ltime >= 'rtime - 5.minutes && 'ltime < 'rtime + 3.seconds)

    util.verifyTable(resultTable, "")
  }
}
