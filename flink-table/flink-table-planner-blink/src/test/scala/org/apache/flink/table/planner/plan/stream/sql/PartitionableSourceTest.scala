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

package org.apache.flink.table.planner.plan.stream.sql

import java.util

import org.apache.flink.table.catalog.{CatalogPartitionImpl, CatalogPartitionSpec, ObjectPath}
import org.apache.flink.table.planner.expressions.utils.Func1
import org.apache.flink.table.planner.factories.TestValuesCatalog
import org.apache.flink.table.planner.utils.TableTestBase
import org.junit.{Before, Test}
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

import scala.collection.JavaConversions._

@RunWith(classOf[Parameterized])
class PartitionableSourceTest(
  val sourceFetchPartitions: Boolean,
  val useCatalogFilter: Boolean) extends TableTestBase{

  private val util = streamTestUtil()

  @Before
  def setup() : Unit = {
    val ddlTemp =
      """
        |CREATE TABLE MyTable (
        |  id int,
        |  name string,
        |  part1 string,
        |  part2 int,
        |  virtualField as part2 + 1)
        |  partitioned by (part1, part2)
        |  with (
        |    'connector' = 'values',
        |    'bounded' = 'true',
        |    'partition-list' = '%s'
        |)
        |""".stripMargin

    if (sourceFetchPartitions) {
      val partitions = "part1:A,part2:1;part1:A,part2:2;part1:B,part2:3;part1:C,part2:1"
      util.tableEnv.executeSql(String.format(ddlTemp, partitions))
    } else {
      val catalog =
        new TestValuesCatalog("test_catalog", "test_database", useCatalogFilter)
      util.tableEnv.registerCatalog("test_catalog", catalog)
      util.tableEnv.useCatalog("test_catalog")
      // register table without partitions
      util.tableEnv.executeSql(String.format(ddlTemp, ""))
      val mytablePath = ObjectPath.fromString("test_database.MyTable")
      // partition map
      val partitions = Seq(
        Map("part1"->"A", "part2"->"1"),
        Map("part1"->"A", "part2"->"2"),
        Map("part1"->"B", "part2"->"3"),
        Map("part1"->"C", "part2"->"1"))
      partitions.foreach(partition => {
        val catalogPartitionSpec = new CatalogPartitionSpec(partition)
        val catalogPartition = new CatalogPartitionImpl(
          new java.util.HashMap[String, String](), "")
        catalog.createPartition(mytablePath, catalogPartitionSpec, catalogPartition, true)
      })
    }
  }

  @Test
  def testSimplePartitionFieldPredicate1(): Unit = {
    util.verifyPlan("SELECT * FROM MyTable WHERE part1 = 'A'")
  }

  @Test
  def testPartialPartitionFieldPredicatePushDown(): Unit = {
    util.verifyPlan("SELECT * FROM MyTable WHERE (id > 2 OR part1 = 'A') AND part2 > 1")
  }

  @Test
  def testWithUdfAndVirtualColumn(): Unit = {
    util.addFunction("MyUdf", Func1)
    util.verifyPlan("SELECT * FROM MyTable WHERE id > 2 AND MyUdf(part2) < 3")
  }

  @Test
  def testUnconvertedExpression(): Unit = {
    util.verifyPlan("select * from MyTable where trim(part1) = 'A' and part2 > 1")
  }
}

object PartitionableSourceTest {
  @Parameterized.Parameters(name = "sourceFetchPartitions={0}, useCatalogFilter={1}")
  def parameters(): util.Collection[Array[Any]] = {
    Seq[Array[Any]](
      Array(true, false),
      Array(false, false),
      Array(false, true)
    )
  }
}
