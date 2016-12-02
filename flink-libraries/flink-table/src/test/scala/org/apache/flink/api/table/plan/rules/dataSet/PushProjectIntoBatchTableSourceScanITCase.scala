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

package org.apache.flink.api.table.plan.rules.dataSet

import collection.JavaConversions._
import org.apache.calcite.rel.{RelNode, RelVisitor}
import org.apache.calcite.rel.core.TableScan
import org.apache.flink.api.common.io.GenericInputFormat
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.java.table.BatchTableEnvironment
import org.apache.flink.api.java.{DataSet, ExecutionEnvironment}
import org.apache.flink.api.scala.batch.utils.TableProgramsTestBase
import org.apache.flink.api.scala.batch.utils.TableProgramsTestBase.TableConfigMode
import org.apache.flink.api.table.{Row, TableEnvironment}
import org.apache.flink.api.table.sources.{BatchTableSource, ProjectableTableSource}
import org.apache.flink.api.table.typeutils.RowTypeInfo
import org.apache.flink.test.util.MultipleProgramsTestBase.TestExecutionMode
import org.junit.{Assert, Before, Ignore, Test}
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

import scala.collection.mutable

/**
  * Test push project down to batchTableSourceScan optimization
  *
  * @param mode
  * @param configMode
  */
@RunWith(classOf[Parameterized])
class PushProjectIntoBatchTableSourceScanITCase(mode: TestExecutionMode,
  configMode: TableConfigMode)
  extends TableProgramsTestBase(mode, configMode) {

  private val tableName = "MyTable"
  private var tableEnv: BatchTableEnvironment = null

  @Before
  def initTableEnv(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    tableEnv = TableEnvironment.getTableEnvironment(env, config)
    tableEnv.registerTableSource(tableName, new TestProjectableTableSource)
  }

  @Test
  def testProjectOnFilterTableAPI(): Unit = {
    val table = tableEnv.scan(tableName).where("amount < 4").select("id, name")
    val expectedSelectedFields = Array[String]("name", "id", "amount")
    assertUsedFieldsEquals(table.getRelNode, tableName, expectedSelectedFields)
  }

  @Test
  def testProjectOnFilterSql(): Unit = {
    val table = tableEnv.sql(s"select id, name from $tableName where amount < 4 ")
    val expectedSelectedFields = Array[String]("name", "id", "amount")
    assertUsedFieldsEquals(table.getRelNode, tableName, expectedSelectedFields)
  }

  @Ignore
  def testProjectWithWindowTableAPI(): Unit = {
    val table = tableEnv.scan(tableName).select("id, amount.avg over (partition by name)")
    val expectedSelectedFields = Array[String]("name", "id", "amount")
    assertUsedFieldsEquals(table.getRelNode, tableName, expectedSelectedFields)
  }

  @Test
  def testProjectWithWindowSql(): Unit = {
    val table = tableEnv.sql(s"select id, avg(amount) over (partition by name) from $tableName")
    val expectedSelectedFields = Array[String]("name", "id", "amount")
    assertUsedFieldsEquals(table.getRelNode, tableName, expectedSelectedFields)
  }

  @Test
  def testMultipleProjectTableAPI(): Unit = {
    val table = tableEnv.scan(tableName)
      .where("amount < 4")
      .select("amount, id, name")
      .select("name")
    val expectedSelectedFields = Array[String]("amount", "name")
    assertUsedFieldsEquals(table.getRelNode, tableName, expectedSelectedFields)
  }

  @Test
  def testMultipleProjectSql(): Unit = {
    val table = tableEnv.sql(
      s"select name from (select amount, id, name from $tableName where amount < 4 ) t1")
    val expectedSelectedFields = Array[String]("name", "amount")
    assertUsedFieldsEquals(table.getRelNode, tableName, expectedSelectedFields)
  }

  @Test
  def testExpressionTableAPI(): Unit = {
    val table = tableEnv.scan(tableName).select("id - 1, amount * 2")
    val expectedSelectedFields = Array[String]("id", "amount")
    assertUsedFieldsEquals(table.getRelNode, tableName, expectedSelectedFields)
  }

  @Test
  def testExpressionSql(): Unit = {
    val table = tableEnv.sql("select id - 1, amount * 2 from MyTable")
    val expectedSelectedFields = Array[String]("id", "amount")
    assertUsedFieldsEquals(table.getRelNode, tableName, expectedSelectedFields)
  }

  @Test
  def testDuplicateFieldTableAPI(): Unit = {
    val table = tableEnv.scan(tableName).select("amount, id - 1, amount as amount1, amount * 2")
    val expectedSelectedFields = Array[String]("amount", "id")
    assertUsedFieldsEquals(table.getRelNode, tableName, expectedSelectedFields)
  }

  @Test
  def testDuplicateFieldSql(): Unit = {
    val table = tableEnv.sql(
      s"select amount, id - 1, amount as amount1, amount * 2 from $tableName")
    val expectedSelectedFields = Array[String]("amount", "id")
    assertUsedFieldsEquals(table.getRelNode, tableName, expectedSelectedFields)
  }

  @Test
  def testSelectStarTableAPI(): Unit = {
    val table = tableEnv.scan(tableName).select("amount as amount1, *")
    val expectedSelectedFields = Array[String]("amount", "name", "id", "price")
    assertUsedFieldsEquals(table.getRelNode, tableName, expectedSelectedFields)
  }

  @Test def testSelectStarSql(): Unit = {
    val table = tableEnv.sql(s"select * from $tableName")
    val expectedSelectedFields = Array[String]("name", "id", "amount", "price")
    assertUsedFieldsEquals(table.getRelNode, tableName, expectedSelectedFields)
  }

  @Ignore
  def testAggregateOnScanTableAPI(): Unit = {
    val table = tableEnv.scan(tableName).select("price.avg, amount.max")
    val expectedSelectedFields = Array[String]("amount", "price")
    assertUsedFieldsEquals(table.getRelNode, tableName, expectedSelectedFields)
  }

  @Test
  def testAggregateOnScanSql(): Unit = {
    val table = tableEnv.sql(s"select avg(price), max(amount) from $tableName")
    val expectedSelectedFields = Array[String]("amount", "price")
    assertUsedFieldsEquals(table.getRelNode, tableName, expectedSelectedFields)
  }

  @Test
  def testJoinOnScanTableAPI(): Unit = {
    val tableName1 = "MyTable1"
    tableEnv.registerTableSource(tableName1, new TestProjectableTableSource)
    val in = tableEnv.scan(tableName)
    val in1 = tableEnv
      .scan(tableName1)
      .select("name as name1,id as id1, amount as amount1, price as price1")
    val result = in.join(in1).where("id === id1 && amount < 2").select("name, amount1")
    val expectedSelectedFields = Array[String]("id", "amount", "name")
    assertUsedFieldsEquals(result.getRelNode, tableName, expectedSelectedFields)
    val expectedSelectedFields1 = Array[String]("id", "amount")
    assertUsedFieldsEquals(result.getRelNode, tableName1, expectedSelectedFields1)
  }

  @Test
  def testJoinOnScanSql(): Unit = {
    val tableName1 = "MyTable1"
    tableEnv.registerTableSource(tableName1, new TestProjectableTableSource)
    val result = tableEnv.sql(
      s"select $tableName.name, $tableName1.amount from $tableName, $tableName1 " +
        s"where $tableName.id = $tableName1.id and $tableName.amount < 2")
    val expectedSelectedFields = Array[String]("name", "id", "amount")
    assertUsedFieldsEquals(result.getRelNode, tableName, expectedSelectedFields)
    val expectedSelectedFields1 = Array[String]("id", "amount")
    assertUsedFieldsEquals(result.getRelNode, tableName1, expectedSelectedFields1)
  }

  @Ignore
  def testAggregateOnJoinTableAPI(): Unit = {
    val tableName1 = "MyTable1"
    tableEnv.registerTableSource(tableName1, new TestProjectableTableSource)
    val in = tableEnv.scan(tableName)
    val in1 = tableEnv
      .scan(tableName1)
      .select("name as name1,id as id1, amount as amount1, price as price1")
    val result = in
      .join(in1)
      .where("id === id1 && amount < 2")
      .select("id.count, price1.max")
    val expectedSelectedFields = Array[String]("id", "amount")
    assertUsedFieldsEquals(result.getRelNode, tableName, expectedSelectedFields)
    val expectedSelectedFields1 = Array[String]("id", "price")
    assertUsedFieldsEquals(result.getRelNode, tableName1, expectedSelectedFields1)
  }

  @Test
  def testAggregateOnJoinSql(): Unit = {
    val tableName1 = "MyTable1"
    tableEnv.registerTableSource(tableName1, new TestProjectableTableSource)
    val result = tableEnv.sql(
      s"select count($tableName.id), max($tableName1.price) from $tableName, $tableName1 " +
        s"where $tableName.id = $tableName1.id and $tableName.amount < 2")
    val expectedSelectedFields = Array[String]("id", "amount")
    assertUsedFieldsEquals(result.getRelNode, tableName, expectedSelectedFields)
    val expectedSelectedFields1 = Array[String]("id", "price")
    assertUsedFieldsEquals(result.getRelNode, tableName1, expectedSelectedFields1)
  }

  /**
    * visit RelNode tree to find the leaf TableScan node which contain specify table,
    * then get its projection fieldsName
    *
    * @param tableName   table name of which table to get
    * @param logicalRoot logical RelNode tree which is not optimized yet
    * @param tEnv        table environment
    * @return projection fieldNames of the specify table
    */
  private def extractProjectedFieldNames(
    tableName: String,
    logicalRoot: RelNode,
    tEnv: BatchTableEnvironment)
  : Option[Array[String]] = {
    val optimizedRelNode = tEnv.optimize(logicalRoot)
    val tableVisitor = new TableVisitor
    tableVisitor.run(optimizedRelNode)
    tableVisitor.fieldsName(tableName)
  }

  private def assertUsedFieldsEquals(
    root: RelNode,
    tableName: String,
    expectUsedFields: Array[String])
  : Unit = {
    val optionalUsedFields = extractProjectedFieldNames(tableName, root, tableEnv)
    optionalUsedFields match {
      case Some(usedFields) =>
        Assert.assertTrue(
          usedFields.size == expectUsedFields.size && usedFields.toSet == expectUsedFields.toSet)
      case None => Assert.fail(s"cannot find table $tableName in optimized RelNode tree")
    }
  }
}

/**
  * This class is responsible for collect table fields names of every underlying TableScan node
  * Note: if RelNode tree contains same table for more than one time, this visitor would complain by
  * throwing an IllegalArgumentException. For example, e.g,
  * 'select t.name, t.amount from t, t as t1 where t.id = t1.id and t1.amount < 2'
  */
class TableVisitor extends RelVisitor {
  private val tableToUsedFieldsMapping = mutable.HashMap[String, Array[String]]()

  /**
    * get fields name of table
    *
    * @param tableName
    * @return
    */
  def fieldsName(tableName: String): Option[Array[String]] = {
    tableToUsedFieldsMapping.get(tableName)
  }

  def run(input: RelNode) {
    go(input)
  }

  override def visit(node: RelNode, ordinal: Int, parent: RelNode) = {
    node match {
      case ts: TableScan =>
        val usedFieldsName = ts.getRowType.getFieldNames.toArray(Array[String]())
        ts.getTable.getQualifiedName.foreach(
          tableName => {
            tableToUsedFieldsMapping.get(tableName) match {
              case Some(_) =>
                throw new IllegalArgumentException(
                  s"there already exists table $tableName in RelNode tree")
              case None => tableToUsedFieldsMapping += (tableName -> usedFieldsName)
            }

          })
      case _ =>
    }
    super.visit(node, ordinal, parent)
  }
}

class TestProjectableTableSource(
  fieldTypes: Array[TypeInformation[_]],
  fieldNames: Array[String])
  extends BatchTableSource[Row] with ProjectableTableSource[Row] {

  def this() = this(
    fieldTypes = Array(
      BasicTypeInfo.STRING_TYPE_INFO,
      BasicTypeInfo.LONG_TYPE_INFO,
      BasicTypeInfo.INT_TYPE_INFO,
      BasicTypeInfo.DOUBLE_TYPE_INFO),
    fieldNames = Array[String]("name", "id", "amount", "price")
  )

  /** Returns the data of the table as a [[org.apache.flink.api.java.DataSet]]. */
  override def getDataSet(execEnv: ExecutionEnvironment): DataSet[Row] = {
    execEnv.createInput(new ProjectableInputFormat(33, fieldNames), getReturnType).setParallelism(1)
  }

  /** Returns the types of the table fields. */
  override def getFieldTypes: Array[TypeInformation[_]] = fieldTypes

  /** Returns the names of the table fields. */
  override def getFieldsNames: Array[String] = fieldNames

  /** Returns the [[TypeInformation]] for the return type. */
  override def getReturnType: TypeInformation[Row] = new RowTypeInfo(fieldTypes)

  /** Returns the number of fields of the table. */
  override def getNumberOfFields: Int = fieldNames.length

  override def projectFields(fields: Array[Int]): TestProjectableTableSource = {
    val projectedFieldTypes = new Array[TypeInformation[_]](fields.length)
    val projectedFieldNames = new Array[String](fields.length)

    fields.zipWithIndex.foreach(f => {
      projectedFieldTypes(f._2) = fieldTypes(f._1)
      projectedFieldNames(f._2) = fieldNames(f._1)})
    new TestProjectableTableSource(projectedFieldTypes, projectedFieldNames)
  }
}

class ProjectableInputFormat(
  num: Int, fieldNames: Array[String]) extends GenericInputFormat[Row] {

  val possibleFieldsName = Set("name", "id", "amount", "price")
  var cnt = 0L
  require(num > 0, "the num must be positive")
  require(fieldNames.toSet.subsetOf(possibleFieldsName), "input field names contain illegal name")

  override def reachedEnd(): Boolean = cnt >= num

  override def nextRecord(reuse: Row): Row = {
    fieldNames.zipWithIndex.foreach(f =>
      f._1 match {
        case "name" =>
          reuse.setField(f._2, "Record_" + cnt)
        case "id" =>
          reuse.setField(f._2, cnt)
        case "amount" =>
          reuse.setField(f._2, cnt.toInt % 16)
        case "price" =>
          reuse.setField(f._2, cnt.toDouble / 3)
        case _ =>
          throw new IllegalArgumentException("unknown field name")
      }
    )
    cnt += 1
    reuse
  }
}
