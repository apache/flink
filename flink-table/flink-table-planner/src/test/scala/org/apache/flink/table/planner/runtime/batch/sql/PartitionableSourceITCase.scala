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
package org.apache.flink.table.planner.runtime.batch.sql

import org.apache.flink.table.catalog.{CatalogPartitionImpl, CatalogPartitionSpec, ObjectPath}
import org.apache.flink.table.planner.factories.{TestValuesCatalog, TestValuesTableFactory}
import org.apache.flink.table.planner.runtime.utils.BatchAbstractTestBase.createTempFolder
import org.apache.flink.table.planner.runtime.utils.BatchTestBase
import org.apache.flink.table.planner.runtime.utils.BatchTestBase.row
import org.apache.flink.table.planner.utils.TestingTableEnvironment
import org.apache.flink.table.resource.{ResourceType, ResourceUri}
import org.apache.flink.testutils.junit.extensions.parameterized.{Parameter, ParameterizedTestExtension, Parameters}
import org.apache.flink.util.UserClassLoaderJarTestUtils

import org.junit.jupiter.api.{BeforeEach, TestTemplate}
import org.junit.jupiter.api.extension.ExtendWith

import java.util
import java.util.Collections

import scala.collection.JavaConversions._

@ExtendWith(Array(classOf[ParameterizedTestExtension]))
class PartitionableSourceITCase extends BatchTestBase {

  @Parameter val sourceFetchPartitions: Boolean = false

  @Parameter(value = 1)
  val useCatalogFilter: Boolean = false

  @BeforeEach
  override def before(): Unit = {
    super.before()

    env.setParallelism(1) // set sink parallelism to 1
    val data = Seq(
      row(1, "ZhangSan", "A", 1),
      row(2, "LiSi", "A", 1),
      row(3, "Jack", "A", 2),
      row(4, "Tom", "B", 3),
      row(5, "Vivi", "C", 1)
    )
    val dataId = TestValuesTableFactory.registerData(data)

    val partitionableTable =
      s"""
         |CREATE TABLE PartitionableTable (
         |  id int,
         |  name string,
         |  part1 string,
         |  part2 int,
         |  virtualField as part2 + 1)
         |  partitioned by (part1, part2)
         |  with (
         |    'connector' = 'values',
         |    'data-id' = '$dataId',
         |    'bounded' = 'true',
         |    'partition-list' = '%s'
         |)
         |""".stripMargin

    // test when PushDownFilter can consume all filters including fields partitionKeys
    val partitionableAndFilterableTable =
      s"""
         |CREATE TABLE PartitionableAndFilterableTable (
         |  id int,
         |  name string,
         |  part1 string,
         |  part2 int,
         |  virtualField as part2 + 1)
         |  partitioned by (part1, part2)
         |  with (
         |    'connector' = 'values',
         |    'data-id' = '$dataId',
         |    'bounded' = 'true',
         |    'partition-list' = '%s',
         |    'filterable-fields' = 'id;part1;part2'
         |)
         |""".stripMargin

    if (sourceFetchPartitions) {
      val partitions = "part1:A,part2:1;part1:A,part2:2;part1:B,part2:3;part1:C,part2:1"
      tEnv.executeSql(String.format(partitionableTable, partitions))
      tEnv.executeSql(String.format(partitionableAndFilterableTable, partitions))
    } else {
      val catalog =
        new TestValuesCatalog("test_catalog", "test_database", useCatalogFilter)
      tEnv.registerCatalog("test_catalog", catalog)
      tEnv.useCatalog("test_catalog")
      // register table without partitions
      tEnv.executeSql(String.format(partitionableTable, ""))
      tEnv.executeSql(String.format(partitionableAndFilterableTable, ""))
      val partitionableTablePath = ObjectPath.fromString("test_database.PartitionableTable")
      val partitionableAndFilterableTablePath =
        ObjectPath.fromString("test_database.PartitionableAndFilterableTable")
      val partitions = Seq(
        Map("part1" -> "A", "part2" -> "1"),
        Map("part1" -> "A", "part2" -> "2"),
        Map("part1" -> "B", "part2" -> "3"),
        Map("part1" -> "C", "part2" -> "1"))
      partitions.foreach(
        partition => {
          val catalogPartitionSpec = new CatalogPartitionSpec(partition)
          val catalogPartition =
            new CatalogPartitionImpl(new java.util.HashMap[String, String](), "")
          catalog.createPartition(
            partitionableTablePath,
            catalogPartitionSpec,
            catalogPartition,
            true)
          catalog.createPartition(
            partitionableAndFilterableTablePath,
            catalogPartitionSpec,
            catalogPartition,
            true)
        })
    }
  }

  @TestTemplate
  def testSimplePartitionFieldPredicate1(): Unit = {
    checkResult(
      "SELECT * FROM PartitionableTable WHERE part1 = 'A'",
      Seq(
        row(1, "ZhangSan", "A", 1, 2),
        row(2, "LiSi", "A", 1, 2),
        row(3, "Jack", "A", 2, 3)
      ))
  }

  @TestTemplate
  def testPartialPartitionFieldPredicatePushDown(): Unit = {
    checkResult(
      "SELECT * FROM PartitionableTable WHERE (id > 2 OR part1 = 'A') AND part2 > 1",
      Seq(
        row(3, "Jack", "A", 2, 3),
        row(4, "Tom", "B", 3, 4)
      ))
  }

  @TestTemplate
  def testUnconvertedExpression(): Unit = {
    checkResult(
      "select * from PartitionableTable where trim(part1) = 'A' and part2 > 1",
      Seq(
        row(3, "Jack", "A", 2, 3)
      ))
  }

  @TestTemplate
  def testPushDownPartitionAndFiltersContainPartitionKeys(): Unit = {
    checkResult(
      "SELECT * FROM PartitionableAndFilterableTable WHERE part1 = 'A' AND id > 1",
      Seq(
        row(2, "LiSi", "A", 1, 2),
        row(3, "Jack", "A", 2, 3)
      ))
  }

  @TestTemplate
  def testPushDownPartitionAndFiltersContainPartitionKeysWithSingleProjection(): Unit = {
    checkResult(
      "SELECT name FROM PartitionableAndFilterableTable WHERE part1 = 'A' AND id > 1",
      Seq(
        row("LiSi"),
        row("Jack")
      ))
  }

  @TestTemplate
  def testPartitionPrunerCompileClassLoader(): Unit = {
    val udfJavaCode =
      s"""
         |public class TrimUDF extends org.apache.flink.table.functions.ScalarFunction {
         |   public String eval(String str) {
         |     return str.trim();
         |   }
         |}
         |""".stripMargin
    val tmpJarDir = createTempFolder()
    val udfJarFile =
      UserClassLoaderJarTestUtils.createJarFile(
        tmpJarDir,
        "flink-test-udf.jar",
        "TrimUDF",
        udfJavaCode)

    tEnv
      .asInstanceOf[TestingTableEnvironment]
      .getResourceManager
      .registerJarResources(
        Collections.singletonList(new ResourceUri(ResourceType.JAR, udfJarFile.toURI.toString)))

    tEnv.executeSql("create temporary function trimUDF as 'TrimUDF'")
    checkResult(
      "select * from PartitionableTable where trimUDF(part1) = 'A' and part2 > 1",
      Seq(
        row(3, "Jack", "A", 2, 3)
      ))
  }
}

object PartitionableSourceITCase {
  @Parameters(name = "sourceFetchPartitions={0}, useCatalogFilter={1}")
  def parameters(): util.Collection[Array[Any]] = {
    Seq[Array[Any]](
      Array(true, false),
      Array(false, false),
      Array(false, true)
    )
  }
}
