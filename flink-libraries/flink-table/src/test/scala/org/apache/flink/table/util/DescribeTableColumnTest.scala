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

package org.apache.flink.table.util

import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.plan.stats.{ColumnStats, TableStats}
import org.apache.flink.table.runtime.utils.CommonTestData
import org.apache.flink.table.util.DescribeTableColumn._
import org.apache.flink.test.util.TestBaseUtils
import org.junit.{Before, Test}

import scala.collection.JavaConversions._

class DescribeTableColumnTest extends TableTestBase {

  private val util = batchTestUtil()

  @Before
  def before(): Unit = {
    util.addTable[(Int, Long, String)]("t", 'a, 'b, 'c)
    util.addTable("x", CommonTestData.get3Source(Array("a", "b", "c")))
    util.tableEnv.alterTableStats("x", Some(TableStats(100L, Map[String, ColumnStats](
      "a" -> ColumnStats(2L, null, null, null, null, null),
      "b" -> ColumnStats(3L, null, null, null, null, null),
      "c" -> ColumnStats(3L, null, null, null, null, null)
    ))))
    util.addTable("y", CommonTestData.get3Source(Array("a", "b", "c")))
    util.tableEnv.alterTableStats("y", Some(TableStats(100L)))
  }

  @Test
  def testDescribeTable1(): Unit = {
    val results = describeTable(util.tableEnv, Array("t"), false)
    val expected = "a,INTEGER,YES\n" +
        "b,BIGINT,YES\n" +
        "c,VARCHAR,YES"
    TestBaseUtils.compareResultAsText(results, expected)
  }

  @Test
  def testDescribeTable2(): Unit = {
    val results = describeTable(util.tableEnv, Array("t"), true)
    val expected = "a,INTEGER,YES\n" +
        "b,BIGINT,YES\n" +
        "c,VARCHAR,YES\n" +
        ",,\n" +
        "# Detailed Table Information,,\n" +
        "row_count,NULL,\n" +
        "table_name,t,"
    TestBaseUtils.compareResultAsText(results, expected)
  }

  @Test
  def testDescribeTable3(): Unit = {
    val results = describeTable(util.tableEnv, Array("x"), false)
    val expected = "a,INTEGER,YES\n" +
        "b,BIGINT,YES\n" +
        "c,VARCHAR,YES"
    TestBaseUtils.compareResultAsText(results, expected)
  }

  @Test
  def testDescribeTable4(): Unit = {
    val results = describeTable(util.tableEnv, Array("x"), true)
    val expected = "a,INTEGER,YES\n" +
        "b,BIGINT,YES\n" +
        "c,VARCHAR,YES\n" +
        ",,\n" +
        "# Detailed Table Information,,\n" +
        "row_count,100,\n" +
        "table_name,x,"
    TestBaseUtils.compareResultAsText(results, expected)
  }

  @Test
  def testDescribeTable5(): Unit = {
    val results = describeTable(util.tableEnv, Array("y"), true)
    val expected = "a,INTEGER,YES\n" +
        "b,BIGINT,YES\n" +
        "c,VARCHAR,YES\n" +
        ",,\n" +
        "# Detailed Table Information,,\n" +
        "row_count,100,\n" +
        "table_name,y,"
    TestBaseUtils.compareResultAsText(results, expected)
  }

  @Test
  def testDescribeTable6(): Unit = {
    val results = describeTable(util.tableEnv, Array("y"), false)
    val expected = "a,INTEGER,YES\n" +
        "b,BIGINT,YES\n" +
        "c,VARCHAR,YES"
    TestBaseUtils.compareResultAsText(results, expected)
  }

  @Test
  def testDescribeColumn1(): Unit = {
    val results = describeColumn(util.tableEnv, Array("t"), "a", true)
    val expected = "column_name,a\n" +
        "column_type,INTEGER\n" +
        "is_nullable,YES\n" +
        "ndv,NULL\n" +
        "null_count,NULL\n" +
        "avg_len,NULL\n" +
        "max_len,NULL\n" +
        "max,NULL\n" +
        "min,NULL"
    TestBaseUtils.compareResultAsText(results, expected)
  }

  @Test
  def testDescribeColumn2(): Unit = {
    val results = describeColumn(util.tableEnv, Array("t"), "a", false)
    val expected = "column_name,a\n" +
        "column_type,INTEGER\n" +
        "is_nullable,YES"
    TestBaseUtils.compareResultAsText(results, expected)
  }

  @Test
  def testDescribeColumn3(): Unit = {
    val results = describeColumn(util.tableEnv, Array("x"), "a", true)
    val expected = "column_name,a\n" +
        "column_type,INTEGER\n" +
        "is_nullable,YES\n" +
        "ndv,2\n" +
        "null_count,NULL\n" +
        "avg_len,NULL\n" +
        "max_len,NULL\n" +
        "max,NULL\n" +
        "min,NULL"
    TestBaseUtils.compareResultAsText(results, expected)
  }

  @Test
  def testDescribeColumn4(): Unit = {
    val results = describeColumn(util.tableEnv, Array("x"), "a", false)
    val expected = "column_name,a\n" +
        "column_type,INTEGER\n" +
        "is_nullable,YES"
    TestBaseUtils.compareResultAsText(results, expected)
  }

  @Test
  def testDescribeColumn5(): Unit = {
    val results = describeColumn(util.tableEnv, Array("y"), "a", true)
    val expected = "column_name,a\n" +
        "column_type,INTEGER\n" +
        "is_nullable,YES\n" +
        "ndv,NULL\n" +
        "null_count,NULL\n" +
        "avg_len,NULL\n" +
        "max_len,NULL\n" +
        "max,NULL\n" +
        "min,NULL"
    TestBaseUtils.compareResultAsText(results, expected)
  }

  @Test
  def testDescribeColumn6(): Unit = {
    val results = describeColumn(util.tableEnv, Array("y"), "a", false)
    val expected = "column_name,a\n" +
        "column_type,INTEGER\n" +
        "is_nullable,YES"
    TestBaseUtils.compareResultAsText(results, expected)
  }
}
