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

package org.apache.flink.table.planner.codegen

import java.lang.{Integer => JInt, Long => JLong}
import java.util.Collections
import org.apache.calcite.jdbc.CalciteSchemaBuilder.asRootSchema
import org.apache.calcite.plan.ConventionTraitDef
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.util.MockStreamingRuntimeContext
import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.catalog.{CatalogManager, FunctionCatalog, ObjectIdentifier}
import org.apache.flink.table.dataformat.{GenericRow, SqlTimestamp}
import org.apache.flink.table.module.ModuleManager
import org.apache.flink.table.planner.calcite.{FlinkContext, FlinkPlannerImpl, FlinkTypeFactory}
import org.apache.flink.table.planner.catalog.CatalogManagerCalciteSchema
import org.apache.flink.table.planner.delegation.PlannerContext
import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedScalarFunctions.JavaFunc5
import org.apache.flink.table.runtime.generated.WatermarkGenerator
import org.apache.flink.table.types.logical.{IntType, TimestampType}
import org.apache.flink.table.utils.CatalogManagerMocks

import org.junit.Assert.{assertEquals, assertTrue}
import org.junit.Test

/**
  * Tests the generated [[WatermarkGenerator]] from [[WatermarkGeneratorCodeGenerator]].
  */
class WatermarkGeneratorCodeGenTest {

  // mock FlinkPlannerImpl to avoid discovering TableEnvironment and Executor.
  val config = new TableConfig
  val catalogManager: CatalogManager = CatalogManagerMocks.createEmptyCatalogManager()
  val functionCatalog = new FunctionCatalog(config, catalogManager, new ModuleManager)
  val plannerContext = new PlannerContext(
    config,
    functionCatalog,
    catalogManager,
    asRootSchema(new CatalogManagerCalciteSchema(catalogManager, false)),
    Collections.singletonList(ConventionTraitDef.INSTANCE))
  val planner: FlinkPlannerImpl = plannerContext.createFlinkPlanner(
    catalogManager.getCurrentCatalog,
    catalogManager.getCurrentDatabase)

  val data = List(
    GenericRow.of(SqlTimestamp.fromEpochMillis(1000L), JInt.valueOf(5)),
    GenericRow.of(null, JInt.valueOf(4)),
    GenericRow.of(SqlTimestamp.fromEpochMillis(3000L), null),
    GenericRow.of(SqlTimestamp.fromEpochMillis(5000L), JInt.valueOf(3)),
    GenericRow.of(SqlTimestamp.fromEpochMillis(4000L), JInt.valueOf(10)),
    GenericRow.of(SqlTimestamp.fromEpochMillis(6000L), JInt.valueOf(8))
  )

  @Test
  def testAscendingWatermark(): Unit = {
    val generator = generateWatermarkGenerator("ts - INTERVAL '0.001' SECOND")
    val results = data.map(d => generator.currentWatermark(d))
    val expected = List(
      JLong.valueOf(999L),
      null,
      JLong.valueOf(2999),
      JLong.valueOf(4999),
      JLong.valueOf(3999),
      JLong.valueOf(5999))
    assertEquals(expected, results)
  }

  @Test
  def testBoundedOutOfOrderWatermark(): Unit = {
    val generator = generateWatermarkGenerator("ts - INTERVAL '5' SECOND")
    val results = data.map(d => generator.currentWatermark(d))
    val expected = List(
      JLong.valueOf(-4000L),
      null,
      JLong.valueOf(-2000L),
      JLong.valueOf(0L),
      JLong.valueOf(-1000L),
      JLong.valueOf(1000L))
    assertEquals(expected, results)
  }

  @Test
  def testCustomizedWatermark(): Unit = {
    JavaFunc5.openCalled = false
    JavaFunc5.closeCalled = false
    functionCatalog.registerTempCatalogScalarFunction(
      ObjectIdentifier.of(
        CatalogManagerMocks.DEFAULT_CATALOG,
        CatalogManagerMocks.DEFAULT_DATABASE,
        "myFunc"),
      new JavaFunc5
    )
    val generator = generateWatermarkGenerator("myFunc(ts, `offset`)")
    // mock open and close invoking
    generator.setRuntimeContext(new MockStreamingRuntimeContext(false, 1, 1))
    generator.open(new Configuration())
    val results = data.map(d => generator.currentWatermark(d))
    generator.close()
    val expected = List(
      JLong.valueOf(995L),
      null,
      null,
      JLong.valueOf(4997L),
      JLong.valueOf(3990L),
      JLong.valueOf(5992L))
    assertEquals(expected, results)
    assertTrue(JavaFunc5.openCalled)
    assertTrue(JavaFunc5.closeCalled)
  }

  private def generateWatermarkGenerator(expr: String): WatermarkGenerator = {
    val tableRowType = plannerContext.getTypeFactory.buildRelNodeRowType(
      Seq("ts", "offset"),
      Seq(
        new TimestampType(3),
        new IntType()
      ))
    val rowType = FlinkTypeFactory.toLogicalRowType(tableRowType)
    val converter = planner.createToRelContext()
        .getCluster
        .getPlanner
        .getContext
        .unwrap(classOf[FlinkContext])
        .getSqlExprToRexConverterFactory
        .create(tableRowType)
    val rexNode = converter.convertToRexNode(expr)
    val generated = WatermarkGeneratorCodeGenerator
      .generateWatermarkGenerator(new TableConfig(), rowType, rexNode)
    generated.newInstance(Thread.currentThread().getContextClassLoader)
  }

}
