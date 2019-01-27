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

package org.apache.flink.table.plan.batch.table

import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.plan.stats.{ColumnStats, TableStats}
import org.apache.flink.table.runtime.utils.CommonTestData
import org.apache.flink.table.util.TableTestBase
import org.junit.Test

import scala.collection.JavaConversions._

class UnionTest extends TableTestBase {

  private val util = batchTestUtil()

  @Test
  def testUnionAll(): Unit = {
    val table1 = util.addTable[(Int, Long, String)]("MyTable1", 'a, 'b, 'c)
    val table2 = util.addTable[(Int, Long, String)]("MyTable2", 'a, 'b, 'c)

    val result = table1.select('a, 'c).unionAll(table2.select('a, 'c)).where('a > 2).select("*")
    util.verifyPlan(result)
  }

  @Test
  def testUnion(): Unit = {
    val table1 = util.addTable[(Int, Long, String)]("MyTable1", 'a, 'b, 'c)
    val table2 = util.addTable[(Int, Long, String)]("MyTable2", 'a, 'b, 'c)

    val result = table1.union(table2).select("a")
    util.verifyPlan(result)
  }

  @Test
  def testUnionWithStats(): Unit = {
    util.addTable("MyTable1", CommonTestData.get3Source(Array("a", "b", "c")))
    util.addTable("MyTable2", CommonTestData.get3Source(Array("a", "b", "c")))
    util.tableEnv.alterTableStats("MyTable1", Some(TableStats(100000000L, Map[String, ColumnStats](
      "a" -> ColumnStats(2L, null, null, null, null, null),
      "b" -> ColumnStats(3L, null, null, null, null, null),
      "c" -> ColumnStats(3L, null, null, null, null, null)
    ))))
    util.tableEnv.alterTableStats("MyTable2", Some(TableStats(100000000L, Map[String, ColumnStats](
      "a" -> ColumnStats(2L, null, null, null, null, null),
      "b" -> ColumnStats(3L, null, null, null, null, null),
      "c" -> ColumnStats(3L, null, null, null, null, null)
    ))))
    val result = util.tableEnv.scan("MyTable1").union(util.tableEnv.scan("MyTable2")).select("a")
    util.verifyPlan(result)
  }

  @Test
  def testTernaryUnion(): Unit = {
    val table0 = util.addTable[(Int, Long, String)]("MyTable1", 'a, 'b, 'c)
    val table1 = util.addTable[(Int, Long, String)]("MyTable2", 'a, 'b, 'c)
    val table2 = util.addTable[(Int, Long, String)]("MyTable3", 'a, 'b, 'c)

    val result = table0.where('b > 2).select('a).union(table1.select('a).union(table2.select('a)))
    util.verifyPlan(result)
  }

}
