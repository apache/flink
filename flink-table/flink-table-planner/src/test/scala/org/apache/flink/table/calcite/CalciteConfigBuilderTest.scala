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

package org.apache.flink.table.calcite

import org.apache.calcite.rel.rules._
import org.apache.calcite.sql.fun.{OracleSqlOperatorTable, SqlStdOperatorTable}
import org.apache.calcite.sql2rel.SqlToRelConverter
import org.apache.calcite.tools.RuleSets
import org.apache.flink.table.api.PlannerConfig
import org.apache.flink.table.plan.rules.datastream.DataStreamRetractionRules
import org.junit.Assert._
import org.junit.Test

import scala.collection.JavaConverters._

class CalciteConfigBuilderTest {

  @Test
  def testDefaultRules(): Unit = {

    val cc: CalciteConfig = new CalciteConfigBuilder().build()

    assertFalse(cc.replacesNormRuleSet)
    assertFalse(cc.normRuleSet.isDefined)

    assertFalse(cc.replacesLogicalOptRuleSet)
    assertFalse(cc.logicalOptRuleSet.isDefined)

    assertFalse(cc.replacesPhysicalOptRuleSet)
    assertFalse(cc.physicalOptRuleSet.isDefined)

    assertFalse(cc.replacesDecoRuleSet)
    assertFalse(cc.decoRuleSet.isDefined)

    assertFalse(cc.replacesLogicalRewriteRuleSet)
    assertFalse(cc.logicalRewriteRuleSet.isDefined)
  }

  @Test
  def testRules(): Unit = {

    val cc: CalciteConfig = new CalciteConfigBuilder()
      .addNormRuleSet(RuleSets.ofList(CoreRules.FILTER_REDUCE_EXPRESSIONS))
      .replaceLogicalOptRuleSet(RuleSets.ofList(CoreRules.FILTER_MERGE))
      .replacePhysicalOptRuleSet(RuleSets.ofList(CoreRules.FILTER_MERGE))
      .replaceLogicalRewriteRuleSet(RuleSets.ofList(CoreRules.FILTER_MERGE))
      .replaceDecoRuleSet(RuleSets.ofList(DataStreamRetractionRules.DEFAULT_RETRACTION_INSTANCE))
      .build()

    assertFalse(cc.replacesNormRuleSet)
    assertTrue(cc.normRuleSet.isDefined)

    assertTrue(cc.replacesLogicalOptRuleSet)
    assertTrue(cc.logicalOptRuleSet.isDefined)

    assertTrue(cc.replacesLogicalOptRuleSet)
    assertTrue(cc.logicalRewriteRuleSet.isDefined)

    assertTrue(cc.replacesPhysicalOptRuleSet)
    assertTrue(cc.physicalOptRuleSet.isDefined)

    assertTrue(cc.replacesDecoRuleSet)
    assertTrue(cc.decoRuleSet.isDefined)
  }

  @Test
  def testReplaceNormalizationRules(): Unit = {

    val cc: CalciteConfig = new CalciteConfigBuilder()
      .replaceNormRuleSet(RuleSets.ofList(CoreRules.FILTER_REDUCE_EXPRESSIONS))
      .build()

    assertEquals(true, cc.replacesNormRuleSet)
    assertTrue(cc.normRuleSet.isDefined)
    val cSet = cc.normRuleSet.get.iterator().asScala.toSet
    assertEquals(1, cSet.size)
    assertTrue(cSet.contains(CoreRules.FILTER_REDUCE_EXPRESSIONS))
  }

  @Test
  def testReplaceNormalizationAddRules(): Unit = {

    val cc: CalciteConfig = new CalciteConfigBuilder()
      .replaceNormRuleSet(RuleSets.ofList(CoreRules.FILTER_REDUCE_EXPRESSIONS))
      .addNormRuleSet(RuleSets.ofList(CoreRules.PROJECT_REDUCE_EXPRESSIONS))
      .build()

    assertEquals(true, cc.replacesNormRuleSet)
    assertTrue(cc.normRuleSet.isDefined)
    val cSet = cc.normRuleSet.get.iterator().asScala.toSet
    assertEquals(2, cSet.size)
    assertTrue(cSet.contains(CoreRules.FILTER_REDUCE_EXPRESSIONS))
    assertTrue(cSet.contains(CoreRules.PROJECT_REDUCE_EXPRESSIONS))
  }

  @Test
  def testAddNormalizationRules(): Unit = {

    val cc: CalciteConfig = new CalciteConfigBuilder()
      .addNormRuleSet(RuleSets.ofList(CoreRules.FILTER_REDUCE_EXPRESSIONS))
      .build()

    assertEquals(false, cc.replacesNormRuleSet)
    assertTrue(cc.normRuleSet.isDefined)
    val cSet = cc.normRuleSet.get.iterator().asScala.toSet
    assertEquals(1, cSet.size)
    assertTrue(cSet.contains(CoreRules.FILTER_REDUCE_EXPRESSIONS))
  }

  @Test
  def testAddAddNormalizationRules(): Unit = {

    val cc: CalciteConfig = new CalciteConfigBuilder()
      .addNormRuleSet(RuleSets.ofList(CoreRules.FILTER_REDUCE_EXPRESSIONS))
      .addNormRuleSet(RuleSets.ofList(CoreRules.PROJECT_REDUCE_EXPRESSIONS,
        CoreRules.CALC_REDUCE_EXPRESSIONS))
      .build()

    assertEquals(false, cc.replacesNormRuleSet)
    assertTrue(cc.normRuleSet.isDefined)
    val cList = cc.normRuleSet.get.iterator().asScala.toList
    assertEquals(3, cList.size)
    assertEquals(cList.head, CoreRules.FILTER_REDUCE_EXPRESSIONS)
    assertEquals(cList(1), CoreRules.PROJECT_REDUCE_EXPRESSIONS)
    assertEquals(cList(2), CoreRules.CALC_REDUCE_EXPRESSIONS)
  }

  @Test
  def testReplaceLogicalOptimizationRules(): Unit = {

    val cc: CalciteConfig = new CalciteConfigBuilder()
        .replaceLogicalOptRuleSet(RuleSets.ofList(CoreRules.FILTER_MERGE))
        .build()

    assertEquals(true, cc.replacesLogicalOptRuleSet)
    assertTrue(cc.logicalOptRuleSet.isDefined)
    val cSet = cc.logicalOptRuleSet.get.iterator().asScala.toSet
    assertEquals(1, cSet.size)
    assertTrue(cSet.contains(CoreRules.FILTER_MERGE))
  }

  @Test
  def testReplaceLogicalOptimizationAddRules(): Unit = {

    val cc: CalciteConfig = new CalciteConfigBuilder()
        .replaceLogicalOptRuleSet(RuleSets.ofList(CoreRules.FILTER_MERGE))
        .addLogicalOptRuleSet(RuleSets.ofList(CoreRules.CALC_MERGE, CoreRules.CALC_SPLIT))
        .build()

    assertEquals(true, cc.replacesLogicalOptRuleSet)
    assertTrue(cc.logicalOptRuleSet.isDefined)
    val cSet = cc.logicalOptRuleSet.get.iterator().asScala.toSet
    assertEquals(3, cSet.size)
    assertTrue(cSet.contains(CoreRules.FILTER_MERGE))
    assertTrue(cSet.contains(CoreRules.CALC_MERGE))
    assertTrue(cSet.contains(CoreRules.CALC_SPLIT))
  }

  @Test
  def testAddLogicalOptimizationRules(): Unit = {

    val cc: CalciteConfig = new CalciteConfigBuilder()
        .addLogicalOptRuleSet(RuleSets.ofList(CoreRules.FILTER_MERGE))
        .addLogicalOptRuleSet(RuleSets.ofList(CoreRules.CALC_MERGE, CoreRules.CALC_SPLIT))
        .build()

    assertEquals(false, cc.replacesLogicalOptRuleSet)
    assertTrue(cc.logicalOptRuleSet.isDefined)
    val cSet = cc.logicalOptRuleSet.get.iterator().asScala.toSet
    assertEquals(3, cSet.size)
    assertTrue(cSet.contains(CoreRules.FILTER_MERGE))
    assertTrue(cSet.contains(CoreRules.CALC_MERGE))
    assertTrue(cSet.contains(CoreRules.CALC_SPLIT))
  }

  @Test
  def testReplaceLogicalRewriteRules(): Unit = {

    val cc: CalciteConfig = new CalciteConfigBuilder()
      .replaceLogicalRewriteRuleSet(RuleSets.ofList(CoreRules.FILTER_MERGE))
      .build()

    assertEquals(true, cc.replacesLogicalRewriteRuleSet)
    assertTrue(cc.logicalRewriteRuleSet.isDefined)
    val cSet = cc.logicalRewriteRuleSet.get.iterator().asScala.toSet
    assertEquals(1, cSet.size)
    assertTrue(cSet.contains(CoreRules.FILTER_MERGE))
  }

  @Test
  def testReplaceLogicalRewriteAddRules(): Unit = {

    val cc: CalciteConfig = new CalciteConfigBuilder()
      .replaceLogicalRewriteRuleSet(RuleSets.ofList(CoreRules.FILTER_MERGE))
      .addLogicalRewriteRuleSet(RuleSets.ofList(CoreRules.CALC_MERGE, CoreRules.CALC_SPLIT))
      .build()

    assertEquals(true, cc.replacesLogicalRewriteRuleSet)
    assertTrue(cc.logicalRewriteRuleSet.isDefined)
    val cSet = cc.logicalRewriteRuleSet.get.iterator().asScala.toSet
    assertEquals(3, cSet.size)
    assertTrue(cSet.contains(CoreRules.FILTER_MERGE))
    assertTrue(cSet.contains(CoreRules.CALC_MERGE))
    assertTrue(cSet.contains(CoreRules.CALC_SPLIT))
  }

  @Test
  def testAddLogicalRewriteRules(): Unit = {

    val cc: CalciteConfig = new CalciteConfigBuilder()
      .addLogicalRewriteRuleSet(RuleSets.ofList(CoreRules.FILTER_MERGE))
      .addLogicalRewriteRuleSet(RuleSets.ofList(CoreRules.CALC_MERGE, CoreRules.CALC_SPLIT))
      .build()

    assertEquals(false, cc.replacesLogicalRewriteRuleSet)
    assertTrue(cc.logicalRewriteRuleSet.isDefined)
    val cSet = cc.logicalRewriteRuleSet.get.iterator().asScala.toSet
    assertEquals(3, cSet.size)
    assertTrue(cSet.contains(CoreRules.FILTER_MERGE))
    assertTrue(cSet.contains(CoreRules.CALC_MERGE))
    assertTrue(cSet.contains(CoreRules.CALC_SPLIT))
  }

  @Test
  def testReplacePhysicalOptimizationRules(): Unit = {

    val cc: CalciteConfig = new CalciteConfigBuilder()
        .replacePhysicalOptRuleSet(RuleSets.ofList(CoreRules.FILTER_MERGE))
        .build()

    assertEquals(true, cc.replacesPhysicalOptRuleSet)
    assertTrue(cc.physicalOptRuleSet.isDefined)
    val cSet = cc.physicalOptRuleSet.get.iterator().asScala.toSet
    assertEquals(1, cSet.size)
    assertTrue(cSet.contains(CoreRules.FILTER_MERGE))
  }

  @Test
  def testReplacePhysicalOptimizationAddRules(): Unit = {

    val cc: CalciteConfig = new CalciteConfigBuilder()
        .replacePhysicalOptRuleSet(RuleSets.ofList(CoreRules.FILTER_MERGE))
        .addPhysicalOptRuleSet(RuleSets.ofList(CoreRules.CALC_MERGE, CoreRules.CALC_SPLIT))
        .build()

    assertEquals(true, cc.replacesPhysicalOptRuleSet)
    assertTrue(cc.physicalOptRuleSet.isDefined)
    val cSet = cc.physicalOptRuleSet.get.iterator().asScala.toSet
    assertEquals(3, cSet.size)
    assertTrue(cSet.contains(CoreRules.FILTER_MERGE))
    assertTrue(cSet.contains(CoreRules.CALC_MERGE))
    assertTrue(cSet.contains(CoreRules.CALC_SPLIT))
  }

  @Test
  def testAddPhysicalOptimizationRules(): Unit = {

    val cc: CalciteConfig = new CalciteConfigBuilder()
        .addPhysicalOptRuleSet(RuleSets.ofList(CoreRules.FILTER_MERGE))
        .addPhysicalOptRuleSet(RuleSets.ofList(CoreRules.CALC_MERGE, CoreRules.CALC_SPLIT))
        .build()

    assertEquals(false, cc.replacesPhysicalOptRuleSet)
    assertTrue(cc.physicalOptRuleSet.isDefined)
    val cSet = cc.physicalOptRuleSet.get.iterator().asScala.toSet
    assertEquals(3, cSet.size)
    assertTrue(cSet.contains(CoreRules.FILTER_MERGE))
    assertTrue(cSet.contains(CoreRules.CALC_MERGE))
    assertTrue(cSet.contains(CoreRules.CALC_SPLIT))
  }

  @Test
  def testReplaceDecorationRules(): Unit = {

    val cc: CalciteConfig = new CalciteConfigBuilder()
      .replaceDecoRuleSet(RuleSets.ofList(DataStreamRetractionRules.DEFAULT_RETRACTION_INSTANCE))
      .build()

    assertEquals(true, cc.replacesDecoRuleSet)
    assertTrue(cc.decoRuleSet.isDefined)
    val cSet = cc.decoRuleSet.get.iterator().asScala.toSet
    assertEquals(1, cSet.size)
    assertTrue(cSet.contains(DataStreamRetractionRules.DEFAULT_RETRACTION_INSTANCE))
  }

  @Test
  def testReplaceDecorationAddRules(): Unit = {

    val cc: CalciteConfig = new CalciteConfigBuilder()
      .replaceDecoRuleSet(RuleSets.ofList(DataStreamRetractionRules.DEFAULT_RETRACTION_INSTANCE))
      .addDecoRuleSet(RuleSets.ofList(DataStreamRetractionRules.UPDATES_AS_RETRACTION_INSTANCE))
      .build()

    assertEquals(true, cc.replacesDecoRuleSet)
    assertTrue(cc.decoRuleSet.isDefined)
    val cSet = cc.decoRuleSet.get.iterator().asScala.toSet
    assertEquals(2, cSet.size)
    assertTrue(cSet.contains(DataStreamRetractionRules.DEFAULT_RETRACTION_INSTANCE))
    assertTrue(cSet.contains(DataStreamRetractionRules.UPDATES_AS_RETRACTION_INSTANCE))
  }

  @Test
  def testAddDecorationRules(): Unit = {

    val cc: CalciteConfig = new CalciteConfigBuilder()
      .addDecoRuleSet(RuleSets.ofList(DataStreamRetractionRules.DEFAULT_RETRACTION_INSTANCE))
      .build()

    assertEquals(false, cc.replacesDecoRuleSet)
    assertTrue(cc.decoRuleSet.isDefined)
    val cSet = cc.decoRuleSet.get.iterator().asScala.toSet
    assertEquals(1, cSet.size)
    assertTrue(cSet.contains(DataStreamRetractionRules.DEFAULT_RETRACTION_INSTANCE))
  }

  @Test
  def testAddAddDecorationRules(): Unit = {

    val cc: CalciteConfig = new CalciteConfigBuilder()
      .addDecoRuleSet(RuleSets.ofList(DataStreamRetractionRules.DEFAULT_RETRACTION_INSTANCE))
      .addDecoRuleSet(RuleSets.ofList(DataStreamRetractionRules.UPDATES_AS_RETRACTION_INSTANCE,
                                      DataStreamRetractionRules.ACCMODE_INSTANCE))
      .build()

    assertEquals(false, cc.replacesDecoRuleSet)
    assertTrue(cc.decoRuleSet.isDefined)
    val cList = cc.decoRuleSet.get.iterator().asScala.toList
    assertEquals(3, cList.size)
    assertEquals(cList.head, DataStreamRetractionRules.DEFAULT_RETRACTION_INSTANCE)
    assertEquals(cList(1), DataStreamRetractionRules.UPDATES_AS_RETRACTION_INSTANCE)
    assertEquals(cList(2), DataStreamRetractionRules.ACCMODE_INSTANCE)
  }

  @Test
  def testDefaultOperatorTable(): Unit = {

    val cc: CalciteConfig = new CalciteConfigBuilder()
      .build()

    assertEquals(false, cc.replacesSqlOperatorTable)
    assertFalse(cc.sqlOperatorTable.isDefined)
  }

  @Test
  def testReplaceOperatorTable(): Unit = {

    val oracleTable = new OracleSqlOperatorTable

    val cc: CalciteConfig = new CalciteConfigBuilder()
      .replaceSqlOperatorTable(oracleTable)
      .build()

    val oracleOps = oracleTable.getOperatorList.asScala

    assertEquals(true, cc.replacesSqlOperatorTable)
    assertTrue(cc.sqlOperatorTable.isDefined)
    val ops = cc.sqlOperatorTable.get.getOperatorList
      .asScala.toSet
    assertEquals(oracleOps.size, ops.size)
    for (o <- oracleOps) {
      assertTrue(ops.contains(o))
    }
  }

  @Test
  def testReplaceAddOperatorTable(): Unit = {

    val oracleTable = new OracleSqlOperatorTable
    val stdTable = new SqlStdOperatorTable

    val cc: CalciteConfig = new CalciteConfigBuilder()
      .replaceSqlOperatorTable(oracleTable)
      .addSqlOperatorTable(stdTable)
      .build()

    val oracleOps = oracleTable.getOperatorList.asScala
    val stdOps = stdTable.getOperatorList.asScala

    assertEquals(true, cc.replacesSqlOperatorTable)
    assertTrue(cc.sqlOperatorTable.isDefined)
    val ops = cc.sqlOperatorTable.get.getOperatorList
      .asScala.toSet
    assertEquals(oracleOps.size + stdOps.size, ops.size)
    for (o <- oracleOps) {
      assertTrue(ops.contains(o))
    }
    for (o <- stdOps) {
      assertTrue(ops.contains(o))
    }

  }

  @Test
  def testAddOperatorTable(): Unit = {

    val oracleTable = new OracleSqlOperatorTable

    val cc: CalciteConfig = new CalciteConfigBuilder()
      .addSqlOperatorTable(oracleTable)
      .build()

    val oracleOps = oracleTable.getOperatorList.asScala

    assertEquals(false, cc.replacesSqlOperatorTable)
    assertTrue(cc.sqlOperatorTable.isDefined)
    val ops = cc.sqlOperatorTable.get.getOperatorList
      .asScala.toSet
    assertEquals(oracleOps.size, ops.size)
    for (o <- oracleOps) {
      assertTrue(ops.contains(o))
    }
  }

  @Test
  def testAddAddOperatorTable(): Unit = {

    val oracleTable = new OracleSqlOperatorTable
    val stdTable = new SqlStdOperatorTable

    val cc: CalciteConfig = new CalciteConfigBuilder()
      .addSqlOperatorTable(oracleTable)
      .addSqlOperatorTable(stdTable)
      .build()

    val oracleOps = oracleTable.getOperatorList.asScala
    val stdOps = stdTable.getOperatorList.asScala

    assertEquals(false, cc.replacesSqlOperatorTable)
    assertTrue(cc.sqlOperatorTable.isDefined)
    val ops = cc.sqlOperatorTable.get.getOperatorList
      .asScala.toSet
    assertEquals(oracleOps.size + stdOps.size, ops.size)
    for (o <- oracleOps) {
      assertTrue(ops.contains(o))
    }
    for (o <- stdOps) {
      assertTrue(ops.contains(o))
    }

  }

  @Test
  def testReplaceSqlToRelConverterConfig(): Unit = {
    val config = SqlToRelConverter.config()
      .withTrimUnusedFields(false)
      .withInSubQueryThreshold(Integer.MAX_VALUE)

    val cc: CalciteConfig = new CalciteConfigBuilder()
      .replaceSqlToRelConverterConfig(config)
      .build()

    assertTrue(cc.sqlToRelConverterConfig.isDefined)
    assertEquals(Integer.MAX_VALUE, cc.sqlToRelConverterConfig.get.getInSubQueryThreshold)
  }

  @Test
  def testUnWrap(): Unit = {

    val config = SqlToRelConverter.config()
      .withTrimUnusedFields(false)
      .withInSubQueryThreshold(Integer.MAX_VALUE)

    val pc: PlannerConfig = new CalciteConfigBuilder()
      .replaceSqlToRelConverterConfig(config)
      .build()

    val cc = pc.unwrap(classOf[CalciteConfig]).get()
    assertTrue(cc.sqlToRelConverterConfig.isDefined)
    assertEquals(Integer.MAX_VALUE, cc.sqlToRelConverterConfig.get.getInSubQueryThreshold)
  }
}
