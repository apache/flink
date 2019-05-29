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

package org.apache.flink.table.utils

import org.apache.calcite.plan.RelOptUtil
import org.apache.calcite.rel.RelNode
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.{LocalEnvironment, DataSet => JDataSet}
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.java.{BatchTableEnvImpl => JavaBatchTableEnvImpl, StreamTableEnvImpl => JavaStreamTableEnvImpl}
import org.apache.flink.table.api.scala.{BatchTableEnvironment => ScalaBatchTableEnv, StreamTableEnvironment => ScalaStreamTableEnv}
import org.apache.flink.table.api.scala.{BatchTableEnvImpl => ScalaBatchTableEnvImpl, StreamTableEnvImpl => ScalaStreamTableEnvImpl}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{Table, TableConfig, TableImpl, TableSchema}
import org.apache.flink.table.catalog.{CatalogManager, GenericCatalogDatabase, GenericInMemoryCatalog}
import org.apache.flink.table.expressions.Expression
import org.apache.flink.table.functions.{AggregateFunction, ScalarFunction, TableFunction}
import org.apache.flink.table.utils.TableTestUtil.{createCatalogManager, extractBuiltinPath}
import org.junit.Assert.assertEquals
import org.junit.{ComparisonFailure, Rule}
import org.junit.rules.ExpectedException
import org.mockito.Mockito.{mock, when}

import util.control.Breaks._

/**
  * Test base for testing Table API / SQL plans.
  */
class TableTestBase {

  // used for accurate exception information checking.
  val expectedException = ExpectedException.none()

  @Rule
  def thrown = expectedException

  def batchTestUtil(): BatchTableTestUtil = {
    BatchTableTestUtil()
  }

  def streamTestUtil(): StreamTableTestUtil = {
    StreamTableTestUtil()
  }

  def verifyTableEquals(expected: Table, actual: Table): Unit = {
    assertEquals(
      "Logical plans do not match",
      LogicalPlanFormatUtils.formatTempTableId(RelOptUtil.toString(
        expected.asInstanceOf[TableImpl].getRelNode)),
      LogicalPlanFormatUtils.formatTempTableId(RelOptUtil.toString(
        actual.asInstanceOf[TableImpl].getRelNode)))
  }
}

abstract class TableTestUtil(verifyCatalogPath: Boolean = false) {

  private var counter = 0

  def addTable[T: TypeInformation](fields: Expression*): Table = {
    counter += 1
    addTable[T](s"Table$counter", fields: _*)
  }

  def addTable[T: TypeInformation](name: String, fields: Expression*): Table

  def addFunction[T: TypeInformation](name: String, function: TableFunction[T]): TableFunction[T]

  def addFunction(name: String, function: ScalarFunction): Unit

  def verifySql(query: String, expected: String): Unit

  def verifyTable(resultTable: Table, expected: String): Unit

  def verifySchema(resultTable: Table, fields: Seq[(String, TypeInformation[_])]): Unit = {
    val actual = resultTable.asInstanceOf[TableImpl].getSchema
    val expected = new TableSchema(fields.map(_._1).toArray, fields.map(_._2).toArray)
    assertEquals(expected, actual)
  }

  // the print methods are for debugging purposes only
  def printTable(resultTable: Table): Unit

  def printSql(query: String): Unit

  protected def verifyString(expected: String, optimized: RelNode) {
    val actual = RelOptUtil.toString(optimized)
    // we remove the charset for testing because it
    // depends on the native machine (Little/Big Endian)
    val actualNoCharset = actual.replace("_UTF-16LE'", "'").replace("_UTF-16BE'", "'")

    // majority of tests did not assume existence of Catalog API.
    // this enables disabling catalog path verification
    val actualWithAdjustedPath = if (!verifyCatalogPath) {
      actualNoCharset.replaceAll("default_catalog, default_database, ", "")
    } else {
      actualNoCharset
    }

    val expectedLines = expected.split("\n").map(_.trim)
    val actualLines = actualWithAdjustedPath.split("\n").map(_.trim)

    val expectedMessage = expectedLines.mkString("\n")
    val actualMessage = actualLines.mkString("\n")

    breakable {
      for ((expectedLine, actualLine) <- expectedLines.zip(actualLines)) {
        if (expectedLine == TableTestUtil.ANY_NODE) {
        }
        else if (expectedLine == TableTestUtil.ANY_SUBTREE) {
          break
        } else if (expectedLine != actualLine) {
          throw new ComparisonFailure(null, expectedMessage, actualMessage)
        }
      }
    }
  }

  def explain(resultTable: Table): String
}

object TableTestUtil {
  val ANY_NODE = "%ANY_NODE%"

  val ANY_SUBTREE = "%ANY_SUBTREE%"

  /**
    * Creates a [[CatalogManager]] with a builtin default catalog & database set to values
    * specified in the [[TableConfig]].
    */
  def createCatalogManager(config: TableConfig): CatalogManager = {
    new CatalogManager(
      config.getBuiltInCatalogName,
      new GenericInMemoryCatalog(config.getBuiltInCatalogName, config.getBuiltInDatabaseName))
  }

  /**
    * Sets the configuration of the builtin catalog & databases in [[TableConfig]]
    * to the current catalog & database of the given [[CatalogManager]]. This should be used
    * to ensure sanity of a [[org.apache.flink.table.api.TableEnvironment]].
    */
  def extractBuiltinPath(config: TableConfig, catalogManager: CatalogManager): TableConfig = {
    config.setBuiltInCatalogName(catalogManager.getCurrentCatalog)
    config.setBuiltInDatabaseName(catalogManager.getCurrentDatabase)
    config
  }

  // this methods are currently just for simplifying string construction,
  // we could replace it with logic later

  def unaryAnyNode(input: String): String = {
    s"""$ANY_NODE
       |$input
       |""".stripMargin.stripLineEnd
  }

  def anySubtree(): String = {
    ANY_SUBTREE
  }

  def unaryNode(node: String, input: String, term: String*): String = {
    s"""$node(${term.mkString(", ")})
       |$input
       |""".stripMargin.stripLineEnd
  }

  def binaryNode(node: String, left: String, right: String, term: String*): String = {
    s"""$node(${term.mkString(", ")})
       |$left
       |$right
       |""".stripMargin.stripLineEnd
  }

  def naryNode(node: String, inputs: List[AnyRef], term: String*): String = {
    val strInputs = inputs.mkString("\n")
    s"""$node(${term.mkString(", ")})
       |$strInputs
       |""".stripMargin.stripLineEnd
  }

  def values(node: String, term: String*): String = {
    s"$node(${term.mkString(", ")})"
  }

  def term(term: AnyRef, value: AnyRef*): String = {
    s"$term=[${value.mkString(", ")}]"
  }

  def tuples(value: List[AnyRef]*): String = {
    val listValues = value.map(listValue => s"{ ${listValue.mkString(", ")} }")
    term("tuples", "[" + listValues.mkString(", ") + "]")
  }

  def batchTableNode(idx: Int): String = {
    s"DataSetScan(table=[[_DataSetTable_$idx]])"
  }

  def streamTableNode(idx: Int): String = {
    s"DataStreamScan(table=[[_DataStreamTable_$idx]])"
  }
}

case class BatchTableTestUtil(
    catalogManager: Option[CatalogManager] = None)
  extends TableTestUtil {
  val javaEnv = new LocalEnvironment()

  private def tableConfig = catalogManager match {
    case Some(c) =>
      TableTestUtil.extractBuiltinPath(new TableConfig, c)
    case None =>
      new TableConfig
  }

  val javaTableEnv = new JavaBatchTableEnvImpl(
    javaEnv,
    tableConfig,
    catalogManager.getOrElse(createCatalogManager(new TableConfig)))
  val env = new ExecutionEnvironment(javaEnv)
  val tableEnv = new ScalaBatchTableEnvImpl(
    env,
    tableConfig,
    catalogManager.getOrElse(createCatalogManager(new TableConfig)))

  def addTable[T: TypeInformation](
      name: String,
      fields: Expression*)
    : Table = {
    val ds = mock(classOf[DataSet[T]])
    val jDs = mock(classOf[JDataSet[T]])
    when(ds.javaSet).thenReturn(jDs)
    val typeInfo: TypeInformation[T] = implicitly[TypeInformation[T]]
    when(jDs.getType).thenReturn(typeInfo)

    val t = ds.toTable(tableEnv, fields: _*)
    tableEnv.registerTable(name, t)
    t
  }

  def addJavaTable[T](typeInfo: TypeInformation[T], name: String, fields: String): Table = {

    val jDs = mock(classOf[JDataSet[T]])
    when(jDs.getType).thenReturn(typeInfo)

    val t = javaTableEnv.fromDataSet(jDs, fields)
    javaTableEnv.registerTable(name, t)
    t
  }

  def addFunction[T: TypeInformation](
      name: String,
      function: TableFunction[T])
    : TableFunction[T] = {
    tableEnv.registerFunction(name, function)
    function
  }

  def addFunction(name: String, function: ScalarFunction): Unit = {
    tableEnv.registerFunction(name, function)
  }

  def addFunction[T: TypeInformation, ACC: TypeInformation](
      name: String,
      function: AggregateFunction[T, ACC]): Unit = {
    tableEnv.registerFunction(name, function)
  }

  def verifySql(query: String, expected: String): Unit = {
    verifyTable(tableEnv.sqlQuery(query), expected)
  }

  def verifyTable(resultTable: Table, expected: String): Unit = {
    val relNode = resultTable.asInstanceOf[TableImpl].getRelNode
    val optimized = tableEnv.optimize(relNode)
    verifyString(expected, optimized)
  }

  def verifyJavaSql(query: String, expected: String): Unit = {
    verifyJavaTable(javaTableEnv.sqlQuery(query), expected)
  }

  def verifyJavaTable(resultTable: Table, expected: String): Unit = {
    val relNode = resultTable.asInstanceOf[TableImpl].getRelNode
    val optimized = javaTableEnv.optimize(relNode)
    verifyString(expected, optimized)
  }

  def printTable(resultTable: Table): Unit = {
    val relNode = resultTable.asInstanceOf[TableImpl].getRelNode
    val optimized = tableEnv.optimize(relNode)
    println(RelOptUtil.toString(optimized))
  }

  def printSql(query: String): Unit = {
    printTable(tableEnv.sqlQuery(query))
  }

  def explain(resultTable: Table): String = {
    tableEnv.explain(resultTable)
  }
}

case class StreamTableTestUtil(
    catalogManager: Option[CatalogManager] = None)
  extends TableTestUtil {
  val javaEnv = new LocalStreamEnvironment()
  javaEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

  private def tableConfig = catalogManager match {
    case Some(c) =>
      TableTestUtil.extractBuiltinPath(new TableConfig, c)
    case None =>
      new TableConfig
  }

  val javaTableEnv = new JavaStreamTableEnvImpl(
    javaEnv,
    tableConfig,
    catalogManager.getOrElse(createCatalogManager(new TableConfig)))
  val env = new StreamExecutionEnvironment(javaEnv)
  val tableEnv = new StreamTableEnvImpl(
    env,
    tableConfig,
    catalogManager.getOrElse(createCatalogManager(new TableConfig)))

  def addTable[T: TypeInformation](
      name: String,
      fields: Expression*)
    : Table = {

    val table = env.fromElements().toTable(tableEnv, fields: _*)
    tableEnv.registerTable(name, table)
    table
  }

  def addJavaTable[T](typeInfo: TypeInformation[T], name: String, fields: String): Table = {
    val stream = javaEnv.addSource(new EmptySource[T], typeInfo)
    val table = javaTableEnv.fromDataStream(stream, fields)
    javaTableEnv.registerTable(name, table)
    table
  }

  def addFunction[T: TypeInformation](
      name: String,
      function: TableFunction[T])
    : TableFunction[T] = {
    tableEnv.registerFunction(name, function)
    function
  }

  def addFunction(name: String, function: ScalarFunction): Unit = {
    tableEnv.registerFunction(name, function)
  }

  def addFunction[T: TypeInformation, ACC: TypeInformation](
      name: String,
      function: AggregateFunction[T, ACC]): Unit = {
    tableEnv.registerFunction(name, function)
  }

  def verifySql(query: String, expected: String): Unit = {
    verifyTable(tableEnv.sqlQuery(query), expected)
  }

  def verifySqlPlansIdentical(query1: String, queries: String*): Unit = {
    val resultTable1 = tableEnv.sqlQuery(query1)
    queries.foreach(s => verify2Tables(resultTable1, tableEnv.sqlQuery(s)))
  }

  def verifyTable(resultTable: Table, expected: String): Unit = {
    val relNode = resultTable.asInstanceOf[TableImpl].getRelNode
    val optimized = tableEnv.optimize(relNode, updatesAsRetraction = false)
    verifyString(expected, optimized)
  }

  def verify2Tables(resultTable1: Table, resultTable2: Table): Unit = {
    val relNode1 = resultTable1.asInstanceOf[TableImpl].getRelNode
    val optimized1 = tableEnv.optimize(relNode1, updatesAsRetraction = false)
    val relNode2 = resultTable2.asInstanceOf[TableImpl].getRelNode
    val optimized2 = tableEnv.optimize(relNode2, updatesAsRetraction = false)
    assertEquals(RelOptUtil.toString(optimized1), RelOptUtil.toString(optimized2))
  }

  def verifyJavaSql(query: String, expected: String): Unit = {
    verifyJavaTable(javaTableEnv.sqlQuery(query), expected)
  }

  def verifyJavaTable(resultTable: Table, expected: String): Unit = {
    val relNode = resultTable.asInstanceOf[TableImpl].getRelNode
    val optimized = javaTableEnv.optimize(relNode, updatesAsRetraction = false)
    verifyString(expected, optimized)
  }

  // the print methods are for debugging purposes only
  def printTable(resultTable: Table): Unit = {
    val relNode = resultTable.asInstanceOf[TableImpl].getRelNode
    val optimized = tableEnv.optimize(relNode, updatesAsRetraction = false)
    println(RelOptUtil.toString(optimized))
  }

  def printSql(query: String): Unit = {
    printTable(tableEnv.sqlQuery(query))
  }

  def explain(resultTable: Table): String = {
    tableEnv.explain(resultTable)
  }
}

class EmptySource[T]() extends SourceFunction[T] {
  override def run(ctx: SourceFunction.SourceContext[T]): Unit = {
  }

  override def cancel(): Unit = {
  }
}
