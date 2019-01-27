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
package org.apache.flink.table.api.stream.sql

import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.util.TableTestBase
import org.junit.Test

class GroupingSetsTest extends TableTestBase {

  @Test
  def testGroupingSets(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('a, 'b, 'c)
    val sql = s"SELECT b, avg(a), grouping(b), grouping_id(b) FROM $table group by grouping sets(b)"
    util.verifyPlan(sql)
  }

  @Test
  def testGroupingSets1(): Unit = {
    val util = streamTestUtil()
    util.addTable[(String, Int, Int)]("courseSales", 'course, 'year0, 'earnings)
    val sqlQuery =
      """
        |SELECT course, SUM(earnings) AS sum0, GROUPING_ID(course, earnings)
        |FROM courseSales
        |GROUP BY GROUPING SETS((), (course), (course, earnings))
        |ORDER BY course, sum0
      """.stripMargin
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testGroupingSets2(): Unit = {
    val util = streamTestUtil()
    util.addTable[(String, Int, Int)]("courseSales", 'course, 'year0, 'earnings)
    val sqlQuery =
      """
        |SELECT course, SUM(earnings) AS sum0
        |FROM courseSales
        |GROUP BY GROUPING SETS((), (course), (course, earnings))
        |ORDER BY course, sum0
      """.stripMargin
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testRollup(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('a, 'b, 'c)
    val sql = s"SELECT b, c, count(*) FROM $table group by rollup(b, c)"
    util.verifyPlan(sql)
  }

  @Test
  def testCube(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Long, Int, String)]('a, 'b, 'c)
    val sql = s"SELECT b, c, count(*) FROM $table group by cube(b, c) having count(*) > 3"
    util.verifyPlan(sql)
  }

}
