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

package org.apache.flink.table.planner.plan.schema

import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.catalog.{Catalog, CatalogManager, FunctionCatalog, GenericInMemoryCatalog, ObjectIdentifier}
import org.apache.flink.table.functions.ScalarFunction
import org.apache.flink.table.module.ModuleManager
import org.apache.flink.table.operations.Operation
import org.apache.flink.table.operations.ddl.CreateTableOperation
import org.apache.flink.table.planner.calcite.{CalciteParser, FlinkPlannerImpl}
import org.apache.flink.table.planner.catalog.CatalogManagerCalciteSchema
import org.apache.flink.table.planner.delegation.PlannerContext
import org.apache.flink.table.planner.expressions.utils.Func0
import org.apache.flink.table.planner.operations.{PlannerQueryOperation, SqlToOperationConverter}
import org.apache.flink.table.planner.plan.schema.ComputedColumnToRelTest.diffRepository
import org.apache.flink.table.planner.utils.DiffRepository

import org.apache.calcite.jdbc.CalciteSchemaBuilder.asRootSchema
import org.apache.calcite.plan.ConventionTraitDef
import org.junit.rules.TestName
import org.junit.{Rule, Test}
import ComputedColumnToRelTest._
import org.apache.flink.table.planner.plan.utils.FlinkRelOptUtil

import org.apache.calcite.rel.RelNode

import java.util.Collections

/**
  * Test cases to verify the plan for DDL tables with computed column.
  */
class ComputedColumnToRelTest {
  // Used for get test case method name.
  val testName: TestName = new TestName

  @Rule
  def name: TestName = testName

  @Test
  def testProjectSourceWithVirtualColumn(): Unit = {
    // Create table with field as atom expression.
    val ddl1 =
      s"""
         |create table t1(
         |  a int,
         |  b varchar,
         |  c as a + 1
         |) with (
         |  'connector' = 'COLLECTION'
         |)
       """.stripMargin
    createTable(ddl1)
    checkConvertTo("select * from t1")
  }

  @Test
  def testProjectSourceWithVirtualColumnAsBuiltinFunc(): Unit = {
    // Create table with field as builtin function.
    val ddl2 =
      s"""
         |create table t2(
         |  a int,
         |  b varchar,
         |  c as to_timestamp(b)
         |) with (
         |  'connector' = 'COLLECTION'
         |)
       """.stripMargin
    createTable(ddl2)
    checkConvertTo("select * from t2")
  }

  @Test
  def testProjectSourceWithVirtualColumnAsUDF(): Unit = {
    // Create table with field as user defined function.
    createScalarFunc("my_udf", Func0)
    val ddl3 =
      s"""
         |create table t3(
         |  a int,
         |  b varchar,
         |  c as my_udf(a)
         |) with (
         |  'connector' = 'COLLECTION'
         |)
       """.stripMargin
    createTable(ddl3)
    checkConvertTo("select * from t3")
  }

  @Test
  def testProjectSourceWithVirtualColumnAsExternalUDF(): Unit = {
    // Create table with field as user defined function.
    createScalarFunc(ObjectIdentifier.of("my_catalog", "my_database", "my_udf"), Func0)
    val ddl3 =
      s"""
         |create table t4(
         |  a int,
         |  b varchar,
         |  c as my_catalog.my_database.my_udf(a)
         |) with (
         |  'connector' = 'COLLECTION'
         |)
       """.stripMargin
    createTable(ddl3)
    checkConvertTo("select * from t4")
  }

  /**
    * Check the relational expression plan right after FlinkPlannerImpl conversion.
    *
    * @param sql The SQL string
    */
  def checkConvertTo(sql: String): Unit = {
    val actual = FlinkRelOptUtil.toString(toRel(sql))
    diffRepository.assertEquals(name.getMethodName, "plan", s"$${plan}", actual)
  }
}

object ComputedColumnToRelTest {
  val diffRepository: DiffRepository = DiffRepository.lookup(classOf[ComputedColumnToRelTest])

  private val tableConfig: TableConfig = new TableConfig
  private val catalog: Catalog = new GenericInMemoryCatalog("MockCatalog", "default_database")
  private val catalogManager: CatalogManager = new CatalogManager("default_catalog", catalog)
  private val moduleManager: ModuleManager = new ModuleManager
  private val functionCatalog: FunctionCatalog = new FunctionCatalog(catalogManager, moduleManager)
  private val plannerContext: PlannerContext =
    new PlannerContext(tableConfig,
      functionCatalog,
      catalogManager,
      asRootSchema(new CatalogManagerCalciteSchema(catalogManager, false)),
      Collections.singletonList(ConventionTraitDef.INSTANCE))

  /** Creates a FlinkPlannerImpl instance. */
  private def createPlanner(): FlinkPlannerImpl = {
      plannerContext.createFlinkPlanner(catalogManager.getCurrentCatalog,
        catalogManager.getCurrentDatabase)
  }

  /** Creates a FlinkPlannerImpl instance. */
  private def createCalciteParser(): CalciteParser = {
    plannerContext.createCalciteParser()
  }

  private def parse(sql: String, planner: FlinkPlannerImpl, parser: CalciteParser): Operation = {
    val node = parser.parse(sql)
    SqlToOperationConverter.convert(planner, catalogManager, node).get
  }

  def toRel(sql: String): RelNode = {
    val operation: Operation = parse(sql, createPlanner(), createCalciteParser())
    assert(operation.isInstanceOf[PlannerQueryOperation])
    operation.asInstanceOf[PlannerQueryOperation].getCalciteTree
  }

  def createTable(sql: String): Unit = {
    val calciteParser = plannerContext.createCalciteParser()
    val operation = parse(sql, createPlanner(), calciteParser)
    assert(operation.isInstanceOf[CreateTableOperation])
    val createTableOperation: CreateTableOperation = operation.asInstanceOf[CreateTableOperation]
    catalogManager.createTable(createTableOperation.getCatalogTable,
      createTableOperation.getTableIdentifier,
      createTableOperation.isIgnoreIfExists)
  }

  def createScalarFunc(name: String, func: ScalarFunction): Unit = {
    functionCatalog.registerTempSystemScalarFunction("my_udf", func)
  }

  def createScalarFunc(objectIdentifier: ObjectIdentifier, func: ScalarFunction): Unit = {
    functionCatalog.registerTempCatalogScalarFunction(objectIdentifier, func)
  }
}
