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

package org.apache.flink.api.scala.batch.table


import org.apache.flink.api.scala._
import org.apache.flink.api.scala.table._
import org.apache.flink.api.scala.util.CollectionDataSets
import org.apache.flink.api.table.{Row, Table, TableConfig, TableEnvironment}
import org.apache.flink.test.util.TestBaseUtils
import org.junit._

import scala.collection.JavaConverters._

class GroupingSetsTest {

  private var tableEnv: BatchTableEnvironment = _
  private var table: Table = _
  private var tableWithNulls: Table = _

  @Before
  def setup(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    tableEnv = TableEnvironment.getTableEnvironment(env, new TableConfig())

    val dataSet = CollectionDataSets.get3TupleDataSet(env)
    table = dataSet.toTable(tableEnv, 'a, 'b, 'c)

    val dataSetWithNulls = dataSet.map(value => value match {
      case (x, y, s) => (x, y, if (s.toLowerCase().contains("world")) null else s)
    })
    tableWithNulls = dataSetWithNulls.toTable(tableEnv, 'a, 'b, 'c)
  }

  @Test
  def testGroupingSets() = {
    val t = table
      .groupingSets('b, 'c)
      .select('b, 'c, 'a.avg as 'a, groupId() as 'g)

    val expected =
      "6,null,18,1\n5,null,13,1\n4,null,8,1\n3,null,5,1\n2,null,2,1\n1,null,1,1\n" +
        "null,Luke Skywalker,6,2\nnull,I am fine.,5,2\nnull,Hi,1,2\n" +
        "null,Hello world, how are you?,4,2\nnull,Hello world,3,2\nnull,Hello,2,2\n" +
        "null,Comment#9,15,2\nnull,Comment#8,14,2\nnull,Comment#7,13,2\n" +
        "null,Comment#6,12,2\nnull,Comment#5,11,2\nnull,Comment#4,10,2\n" +
        "null,Comment#3,9,2\nnull,Comment#2,8,2\nnull,Comment#15,21,2\n" +
        "null,Comment#14,20,2\nnull,Comment#13,19,2\nnull,Comment#12,18,2\n" +
        "null,Comment#11,17,2\nnull,Comment#10,16,2\nnull,Comment#1,7,2"

    val results = t.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testGroupingSetsWithNulls() = {
    val t = tableWithNulls
      .groupingSets('b, 'c)
      .select('b, 'c, 'a.avg as 'a, groupId() as 'g)

    val expected =
      "6,null,18,1\n5,null,13,1\n4,null,8,1\n3,null,5,1\n2,null,2,1\n1,null,1,1\n" +
        "null,Luke Skywalker,6,2\nnull,I am fine.,5,2\nnull,Hi,1,2\n" +
        "null,null,3,2\nnull,Hello,2,2\nnull,Comment#9,15,2\nnull,Comment#8,14,2\n" +
        "null,Comment#7,13,2\nnull,Comment#6,12,2\nnull,Comment#5,11,2\n" +
        "null,Comment#4,10,2\nnull,Comment#3,9,2\nnull,Comment#2,8,2\n" +
        "null,Comment#15,21,2\nnull,Comment#14,20,2\nnull,Comment#13,19,2\n" +
        "null,Comment#12,18,2\nnull,Comment#11,17,2\nnull,Comment#10,16,2\n" +
        "null,Comment#1,7,2"

    val results = t.toDataSet[Row].collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testCubeAsGroupingSets() = {
    val t1 = table
      .cube('b, 'c)
      .select(
        'b, 'c, 'a.avg as 'a, groupId() as 'g,
        'b.grouping() as 'gb, grouping('c) as 'gc,
        'b.groupingId() as 'gib, groupingId('c) as 'gic,
        ('b, 'c).groupingId() as 'gid
      )

    val t2 = table
      .groupingSets(('b, 'c), 'b, 'c, ())
      .select(
        'b, 'c, 'a.avg as 'a, groupId() as 'g,
        grouping('b) as 'gb, 'c.grouping() as 'gc,
        groupingId('b) as 'gib, 'c.groupingId() as 'gic,
        groupingId('b, 'c) as 'gid
      )

    val results1 = t1.toDataSet[Row].map(_.toString).collect()
    val results2 = t2.toDataSet[Row].map(_.toString).collect()
    TestBaseUtils.compareResultCollections(results1.asJava, results2.asJava)
  }

  @Test
  def testRollupAsGroupingSets() = {
    val t1 = table
      .rollup('b, 'c)
      .select(
        'b, 'c, 'a.avg as 'a, groupId() as 'g,
        'b.grouping() as 'gb, grouping('c) as 'gc,
        'b.groupingId() as 'gib, groupingId('c) as 'gic,
        ('b, 'c).groupingId() as 'gid
      )

    val t2 = table
      .groupingSets(('b, 'c), 'b, ())
      .select(
        'b, 'c, 'a.avg as 'a, groupId() as 'g,
        grouping('b) as 'gb, 'c.grouping() as 'gc,
        groupingId('b) as 'gib, 'c.groupingId() as 'gic,
        groupingId('b, 'c) as 'gid
      )

    val results1 = t1.toDataSet[Row].map(_.toString).collect()
    val results2 = t2.toDataSet[Row].map(_.toString).collect()
    TestBaseUtils.compareResultCollections(results1.asJava, results2.asJava)
  }
}
