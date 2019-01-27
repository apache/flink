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

package org.apache.flink.table.plan

import java.io.IOException
import java.nio.file._
import java.nio.file.attribute.BasicFileAttributes

import org.apache.calcite.rel.{AbstractRelNode, BiRel, RelNode, SingleRel}
import org.apache.flink.api.scala._
import org.apache.flink.table.api.{Table, TableConfigOptions, TableEnvironment}
import org.apache.flink.table.api.scala.{BatchTableEnvironment, _}
import org.apache.flink.table.api.types.DataType
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.factories.{BatchTableSinkFactory, BatchTableSourceFactory, TableFactory}
import org.apache.flink.table.plan.nodes.physical.batch._
import org.apache.flink.table.plan.subplan.BatchDAGOptimizer
import org.apache.flink.table.runtime.batch.sql.BatchTestBase
import org.apache.flink.table.sinks.csv.CsvTableSink
import org.apache.flink.table.sinks.{BatchTableSink, CollectRowTableSink}
import org.apache.flink.table.sources.csv.CsvTableSource
import org.apache.flink.table.sources.BatchTableSource
import org.apache.flink.table.temptable.{FlinkTableServiceFactory, FlinkTableServiceFactoryDescriptor}
import org.apache.flink.table.util.{CollectionBatchExecTable, TableProperties}
import org.apache.flink.test.util.TestBaseUtils
import org.junit.Assert._
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.{After, Before, Ignore, Test}

import _root_.scala.collection.JavaConverters._
import _root_.scala.collection.mutable

class CacheCsvTableFactory extends BatchTableSourceFactory[BaseRow]
  with BatchTableSinkFactory[BaseRow] {

  override def createBatchTableSource(properties: java.util.Map[String, String]):
  BatchTableSource[BaseRow] = {
    val tableProperties = new TableProperties
    tableProperties.putProperties(properties)
    val tableName = tableProperties.readTableNameFromProperties()
    val schema = tableProperties.readSchemaFromProperties(classOf[CacheCsvTableFactory]
      .getClassLoader)
    val path = tableProperties.getString("path", "") + tableName
    val builder = CsvTableSource.builder()
      .path(path)
      .fields(schema.getColumnNames, schema.getColumnTypes)
    builder.build()
  }


  override def createBatchTableSink(
    properties: java.util.Map[String, String]): BatchTableSink[BaseRow] = {
    val tableProperties = new TableProperties
    tableProperties.putProperties(properties)
    val tableName = tableProperties.readTableNameFromProperties()
    val path = tableProperties.getString("path", "") + tableName
    val schema = tableProperties
      .readSchemaFromProperties(classOf[CacheCsvTableFactory].getClassLoader)
    val types = schema.getColumnTypes.map(_.asInstanceOf[DataType])
    val sink = new CsvTableSink(path)
    sink.configure(schema.getColumnNames, types).asInstanceOf[BatchTableSink[BaseRow]]
  }


  override def requiredContext(): java.util.Map[String, String] =
    java.util.Collections.emptyMap()

  override def supportedProperties(): java.util.List[String] =
    java.util.Collections.emptyList()
}

@Ignore
@RunWith(classOf[Parameterized])
class CacheAwareRelNodePlanBuilderTest(
  factory: TableFactory,
  properties: TableProperties) extends BatchTestBase {

  var tableEnv: BatchTableEnvironment = null
  var table1: Table = null
  var table2: Table = null
  var table3: Table = null

  @Before
  def init(): Unit = {
    conf.setSubsectionOptimization(true)
    conf.getConf.setBoolean(TableConfigOptions.SQL_OPTIMIZER_REUSE_SUB_PLAN_ENABLED, true)
    conf.setTableServiceFactoryDescriptor(
      new FlinkTableServiceFactoryDescriptor(factory, properties))

    tableEnv = TableEnvironment.getBatchTableEnvironment(env, conf)

    // init test data
    val data1 = new mutable.MutableList[(String, String)]
    data1.+=(("The Doors", "Riders On The Storm"))
    data1.+=(("Tony Joe White", "Closing In On The Fire"))
    data1.+=(("Levon Helm", "Calvary"))
    table1 = tableEnv.fromCollection( data1, "Artist1, Song1")

    val data2 = new mutable.MutableList[(String, String)]
    data2.+=(("The Doors", "L.A. Woman"))
    data2.+=(("Tony Joe White", "Closing In On The Fire"))
    data2.+=(("Levon Helm", "Dirt Farmer"))
    table2 = tableEnv.fromCollection(data2, "Artist2, Album2")

    val data3 = new mutable.MutableList[(String, Int)]
    data3.+=(("L.A. Woman", 1971))
    data3.+=(("Closing In On The Fire", 1998))
    data3.+=(("Dirt Farmer", 2007))
    table3 = tableEnv.fromCollection(data3, "Album3, Year3")
  }

  @After
  def clean(): Unit = {
    // TODO: to be removed when cleanUpService is ready
    val path = Paths.get(CacheAwareRelNodePlanBuilderTest.tmpDir)
    if (Files.exists(path) && Files.isDirectory(path)) {
      Files.walkFileTree(path, new FileVisitor[Path] {
        def visitFileFailed(file: Path, exc: IOException) = FileVisitResult.CONTINUE

        def visitFile(file: Path, attrs: BasicFileAttributes) = {
          Files.delete(file)
          FileVisitResult.CONTINUE
        }

        def preVisitDirectory(dir: Path, attrs: BasicFileAttributes) = FileVisitResult.CONTINUE

        def postVisitDirectory(dir: Path, exc: IOException) = {
          Files.delete(dir)
          FileVisitResult.CONTINUE
        }
      })
    }
    Files.createDirectory(path)
    tableEnv.close()
  }

  @Test
  def testPersist(): Unit = {
    val ds1 = CollectionBatchExecTable.getSmall3TupleDataSet(tableEnv, "a, b, c")
    val ds2 = CollectionBatchExecTable.get5TupleDataSet(tableEnv, "d, e, f, g, h")

    val joinT = ds1.join(ds2)
      .where('b === 'e)
      .select('c, 'g)

    joinT.cache()

    val res1 = joinT.select('c)
    val sink1 = new CsvTableSink(CacheAwareRelNodePlanBuilderTest.tmpDir + "result1.csv", ",")
    res1.writeToSink(sink1)

    val sink2 = new CsvTableSink(CacheAwareRelNodePlanBuilderTest.tmpDir + "result2.csv", ",")
    val res2 = joinT.select('g)
    res2.writeToSink(sink2)
    res2.tableEnv.execute("write to sink 2")

    // 'joinT' has been cached now
    val res3 = joinT.select('g)
    val expected = "Hallo Welt\n" + "Hallo Welt\n" + "Hallo\n"
    val results = res3.collect()
    TestBaseUtils.compareResultAsText(results.asJava, expected)
  }

  @Test
  def testPersistWithBlockOptimization(): Unit = {
    val join = table1.where('Artist1 !== "The Doors")
      .join(table2)
      .where('Artist1 === 'Artist2)
      .select('Artist1, 'Album2, 'Song1)

    join.cache()

    val res0 = join.select('Artist1)
    val sink0 = new CsvTableSink(CacheAwareRelNodePlanBuilderTest.tmpDir + "result3.csv", ",")
    res0.writeToSink(sink0)
    tableEnv.execute("write to sink 0")

    val join2 = join.join(table3)
      .where('Album2 === 'Album3)
      .select('Artist1, 'Album2, 'Song1, 'Year3)

    val res1 = join2.where('Year3 > 2000)
    val sink1 = new CsvTableSink(CacheAwareRelNodePlanBuilderTest.tmpDir + "result4.csv", ",")
    res1.writeToSink(sink1)

    val res2 = join2.where('Year3 < 2000)
    val sink2 = new CsvTableSink(CacheAwareRelNodePlanBuilderTest.tmpDir + "result5.csv", ",")
    res2.writeToSink(sink2)

    tableEnv.execute("write to sink 1/2")
  }

  @Test
  def testRedundantPersist(): Unit = {
    val join = table1.where('Artist1 !== "The Doors")
      .join(table2)
      .where('Artist1 === 'Artist2)
      .select('Artist1, 'Album2, 'Song1)

    join.cache()  // redundant

    val tmp = join.select('Artist1, 'Album2, 'Song1)
    tmp.cache()

    val sink0 = new CsvTableSink(CacheAwareRelNodePlanBuilderTest.tmpDir + "result6.csv", ",")

    tmp.select('Artist1)
      .writeToSink(sink0)
    tableEnv.execute("write to sink 0")
  }

  // FIXME https://aone.alibaba-inc.com/task/18478823

  // following test cases verify the plans
  @Test
  def testPlanPersistSimple(): Unit = {
    val t = table1.select('Artist1)
    t.cache()
    writeToCollectSink(t.where('Artist1 === "The Doors"))

    // there should be two LogicalNodeBlock
    val sinks = compile()
    assertTrue(sinks.size == 2)

    mockExecute(tableEnv)

    writeToCollectSink(t.where('Artist1 === "The Doors"))
    val plan = compile()

    assertTrue(plan.size == 1)

    val expectedPre = List(
      classOf[BatchExecSink[_]],
      classOf[BatchExecCalc],
      classOf[BatchExecTableSourceScan]
    )

    val expectedMid = List(
      classOf[BatchExecTableSourceScan],
      classOf[BatchExecCalc],
      classOf[BatchExecSink[_]]
    )
    verifyLogicalNodeTree(plan(0), expectedPre, expectedMid)
  }

  @Test
  def testPlanRedundantPersist(): Unit = {
    val t = table1.select('Artist1)

    t.cache()
    t.cache()

    writeToCollectSink(t.where('Artist1 === "The Doors"))

    // there should be two LogicalNodeBlock
    assertTrue(compile().size == 2)

    mockExecute(tableEnv)

    writeToCollectSink(t.where('Artist1 === "The Doors"))
    val plan = compile()

    assertTrue(plan.size == 1)

    val expectedPre = List(
      classOf[BatchExecSink[_]],
      classOf[BatchExecCalc],
      classOf[BatchExecTableSourceScan]
    )

    val expectedMid = List(
      classOf[BatchExecTableSourceScan],
      classOf[BatchExecCalc],
      classOf[BatchExecSink[_]]
    )
    verifyLogicalNodeTree(plan(0), expectedPre, expectedMid)
  }

  @Test
  def testPlanRedundantPersist2(): Unit = {
    var t = table1.select('Artist1, 'Song1)
    t.cache()
    t = t.select('Artist1)
    t.cache()

    writeToCollectSink(t.where('Artist1 === "The Doors"))

    assertTrue(compile().size == 3)

    mockExecute(tableEnv)

    writeToCollectSink(t.where('Artist1 === "The Doors"))
    val plan = compile()

    assertTrue(plan.size == 1)

    val expectedPre = List(
      classOf[BatchExecSink[_]],
      classOf[BatchExecCalc],
      classOf[BatchExecTableSourceScan]
    )

    val expectedMid = List(
      classOf[BatchExecTableSourceScan],
      classOf[BatchExecCalc],
      classOf[BatchExecSink[_]]
    )
    verifyLogicalNodeTree(plan(0), expectedPre, expectedMid)
  }

  @Test
  def testPlanPersistWithoutReference(): Unit = {
    val t = table1.select('Artist1)
    t.cache()
    writeToCollectSink(t.where('Artist1 === "The Doors"))

    table2.select('Artist2).cache()

    // todo: to be optimized
    assertTrue(compile().size == 3)

    mockExecute(tableEnv)

    writeToCollectSink(t.where('Artist1 === "The Doors"))
    val plan = compile()

    assertTrue(plan.size == 1)

    val expectedPre = List(
      classOf[BatchExecSink[_]],
      classOf[BatchExecCalc],
      classOf[BatchExecTableSourceScan]
    )

    val expectedMid = List(
      classOf[BatchExecTableSourceScan],
      classOf[BatchExecCalc],
      classOf[BatchExecSink[_]]
    )
    verifyLogicalNodeTree(plan(0), expectedPre, expectedMid)
  }

  @Test
  def testPlanPersistWithBlockOptimization(): Unit = {
    val t1 = table1
      .join(table2)
      .where('Artist1 === 'Artist2)
      .select('Artist1, 'Album2, 'Song1)
    t1.cache()

    writeToCollectSink(t1.where('Artist1 === "The Doors"))

    mockExecute(tableEnv)

    val t2 = t1
      .join(table3)
      .where('Album2 ==='Album3)

    writeToCollectSink(t2.where('Artist1 === "The Doors"))
    writeToCollectSink(t2.where('Artist1 === "Levon Helm"))

    val plan = compile()

    // Block optimization should be OK.
    assertTrue(plan.size == 2)
    assertTrue(plan(0).getInput(0).getInput(0) == plan(1).getInput(0).getInput(0))

    // cached table is expected to be used
    val expectedPre = List(
      classOf[BatchExecSink[_]],
      classOf[BatchExecCalc],
      classOf[BatchExecJoinBase],
      classOf[BatchExecScan],
      classOf[BatchExecExchange],
      classOf[BatchExecScan]
    )

    val expectedMid = List(
      classOf[BatchExecScan],
      classOf[BatchExecJoinBase],
      classOf[BatchExecScan],
      classOf[BatchExecExchange],
      classOf[BatchExecCalc],
      classOf[BatchExecSink[_]]
    )

    verifyLogicalNodeTree(plan(0), expectedPre, expectedMid)
  }

  @Test
  def testInvalidateCache(): Unit = {
    val t1 = table1
      .join(table2)
      .where('Artist1 === 'Artist2)
      .select('Artist1, 'Album2, 'Song1)
    t1.cache()

    writeToCollectSink(t1.where('Artist1 === "The Doors"))

    mockExecute(tableEnv)

    val t2 = t1
      .join(table3)
      .where('Album2 ==='Album3)

    writeToCollectSink(t2.where('Artist1 === "The Doors"))
    var plan = compile()

    // only collect sink
    assertTrue(plan.size == 1)

    tableEnv.tableServiceManager.invalidateCachedTable()
    plan = compile()

    // collect sink and table service sink
    assertTrue(plan.size == 2)

    // plan without cache is expected to be used
    val expectedPre = List(
      classOf[BatchExecSink[_]],
      classOf[BatchExecCalc],
      classOf[BatchExecJoinBase],
      classOf[BatchExecScan],
      classOf[BatchExecExchange],
      classOf[BatchExecScan]
    )

    val expectedMid = List(
      classOf[BatchExecScan],
      classOf[BatchExecJoinBase],
      classOf[BatchExecScan],
      classOf[BatchExecExchange],
      classOf[BatchExecCalc],
      classOf[BatchExecSink[_]]
    )

    // plan(1).outputNode refers to the branch of TableServiceSink
    verifyLogicalNodeTree(plan(1), expectedPre, expectedMid)

  }

  private def verifyLogicalNodeTree(r: RelNode,
                                    expectedPreOrder: List[Class[_]],
                                    expectedMidOrder: List[Class[_]]) = {
    val preOrder = getPreOrderClazz(r)
    assertTrue(preOrder.size == expectedPreOrder.size)
    (preOrder zip expectedPreOrder).foreach {
      case (c1, c2) =>
      assertTrue(c1 == c2 || c2.isAssignableFrom(c1))
    }

    val midOrder = getMidOrderClazz(r)
    assertTrue(midOrder.size == expectedMidOrder.size)
    (midOrder zip expectedMidOrder).foreach {
      case (c1, c2) =>
        assertTrue(c1 == c2 || c2.isAssignableFrom(c1))
    }
  }

  private def getPreOrderClazz(t: RelNode): List[Class[_]] = t match {
    case n: SingleRel => n.getClass :: getPreOrderClazz(n.getInput)
    case n: BiRel => (n.getClass :: getPreOrderClazz(n.getLeft)) ::: getPreOrderClazz(n.getRight)
    case n: AbstractRelNode => n.getClass :: Nil
    case _ => Nil
  }

  private def getMidOrderClazz(t: RelNode): List[Class[_]] = t match {
    case n: SingleRel =>  getMidOrderClazz(n.getInput) :+ t.getClass
    case n: BiRel => (getMidOrderClazz(n.getLeft) :+ t.getClass) ::: getMidOrderClazz(n.getRight)
    case n: AbstractRelNode => n.getClass :: Nil
    case _ => Nil
  }

  private def mockExecute(tableEnv: TableEnvironment): Unit = {
    tableEnv.sinkNodes.clear()
    tableEnv.tableServiceManager.markAllTablesCached()
  }

  private def writeToCollectSink(t: Table) = {
    // get schema information of table
    val rowType = t.getRelNode.getRowType
    val fieldNames: Array[String] = rowType.getFieldNames.asScala.toArray
    val fieldTypes: Array[DataType] = rowType.getFieldList.asScala
      .map(field => FlinkTypeFactory.toInternalType(field.getType)).toArray

    val sink = new CollectRowTableSink
    sink.configure(fieldNames, fieldTypes)

    t.writeToSink(sink)
  }

  private def compile(): Seq[RelNode] = {
    val sinks = tableEnv.tableServiceManager.cachePlanBuilder.buildPlanIfNeeded(tableEnv.sinkNodes)
    BatchDAGOptimizer.optimize(sinks, tableEnv)
  }

}

object CacheAwareRelNodePlanBuilderTest {

  def tmpDir(): String =
    java.nio.file.Files.createTempDirectory("flink_cache_")
      .toAbsolutePath.toString + java.io.File.separatorChar

  private def getDefaultTableServiceFactory()  = {
    val factory = new FlinkTableServiceFactory()
    val properties = new TableProperties()

    Array(factory, properties)
  }

  private def getCsvTableServiceFactory() = {
    val csvTableFactory = new CacheCsvTableFactory()
    val properties = new TableProperties()
    properties.setString("type", "CSV")
    properties.setString("path", tmpDir)

    Array(csvTableFactory, properties)
  }

  @Parameterized.Parameters(name = "TableFactory={0}")
  def parameters(): java.util.Collection[Array[_]] = {
    java.util.Arrays.asList(
      getDefaultTableServiceFactory()
      , getCsvTableServiceFactory()
    )
  }
}
