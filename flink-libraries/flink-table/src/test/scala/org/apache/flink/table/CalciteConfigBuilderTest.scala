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

package org.apache.flink.table

import org.apache.calcite.rel.rules._
import org.apache.calcite.sql.fun.{OracleSqlOperatorTable, SqlStdOperatorTable}
import org.apache.calcite.tools.RuleSets
import org.apache.flink.table.calcite.{CalciteConfig, CalciteConfigBuilder}
import org.junit.Assert._
import org.junit.Test

import scala.collection.JavaConverters._

class CalciteConfigBuilderTest {

  @Test
  def testDefaultRules(): Unit = {

    val cc: CalciteConfig = new CalciteConfigBuilder().build()

    assertFalse(cc.replacesNormRuleSet)
    assertFalse(cc.getNormRuleSet.isDefined)

    assertFalse(cc.replacesOptRuleSet)
    assertFalse(cc.getOptRuleSet.isDefined)

    assertFalse(cc.replacesDecoRuleSet)
    assertFalse(cc.getDecoRuleSet.isDefined)
  }

  @Test
  def testRules(): Unit = {

    val cc: CalciteConfig = new CalciteConfigBuilder()
      .addNormRuleSet(RuleSets.ofList(ReduceExpressionsRule.FILTER_INSTANCE))
      .replaceOptRuleSet(RuleSets.ofList(FilterMergeRule.INSTANCE))
      .replaceDecoRuleSet(RuleSets.ofList(ReduceExpressionsRule.FILTER_INSTANCE))
      .build()

    assertFalse(cc.replacesNormRuleSet)
    assertTrue(cc.getNormRuleSet.isDefined)

    assertTrue(cc.replacesOptRuleSet)
    assertTrue(cc.getOptRuleSet.isDefined)

    assertTrue(cc.replacesDecoRuleSet)
    assertTrue(cc.getDecoRuleSet.isDefined)
  }

  @Test
  def testReplaceNormalizationRules(): Unit = {

    val cc: CalciteConfig = new CalciteConfigBuilder()
      .replaceNormRuleSet(RuleSets.ofList(ReduceExpressionsRule.FILTER_INSTANCE))
      .build()

    assertEquals(true, cc.replacesNormRuleSet)
    assertTrue(cc.getNormRuleSet.isDefined)
    val cSet = cc.getNormRuleSet.get.iterator().asScala.toSet
    assertEquals(1, cSet.size)
    assertTrue(cSet.contains(ReduceExpressionsRule.FILTER_INSTANCE))
  }

  @Test
  def testReplaceNormalizationAddRules(): Unit = {

    val cc: CalciteConfig = new CalciteConfigBuilder()
      .replaceNormRuleSet(RuleSets.ofList(ReduceExpressionsRule.FILTER_INSTANCE))
      .addNormRuleSet(RuleSets.ofList(ReduceExpressionsRule.PROJECT_INSTANCE))
      .build()

    assertEquals(true, cc.replacesNormRuleSet)
    assertTrue(cc.getNormRuleSet.isDefined)
    val cSet = cc.getNormRuleSet.get.iterator().asScala.toSet
    assertEquals(2, cSet.size)
    assertTrue(cSet.contains(ReduceExpressionsRule.FILTER_INSTANCE))
    assertTrue(cSet.contains(ReduceExpressionsRule.PROJECT_INSTANCE))
  }

  @Test
  def testAddNormalizationRules(): Unit = {

    val cc: CalciteConfig = new CalciteConfigBuilder()
      .addNormRuleSet(RuleSets.ofList(ReduceExpressionsRule.FILTER_INSTANCE))
      .build()

    assertEquals(false, cc.replacesNormRuleSet)
    assertTrue(cc.getNormRuleSet.isDefined)
    val cSet = cc.getNormRuleSet.get.iterator().asScala.toSet
    assertEquals(1, cSet.size)
    assertTrue(cSet.contains(ReduceExpressionsRule.FILTER_INSTANCE))
  }

  @Test
  def testAddAddNormalizationRules(): Unit = {

    val cc: CalciteConfig = new CalciteConfigBuilder()
      .addNormRuleSet(RuleSets.ofList(ReduceExpressionsRule.FILTER_INSTANCE))
      .addNormRuleSet(RuleSets.ofList(ReduceExpressionsRule.PROJECT_INSTANCE,
        ReduceExpressionsRule.CALC_INSTANCE))
      .build()

    assertEquals(false, cc.replacesNormRuleSet)
    assertTrue(cc.getNormRuleSet.isDefined)
    val cList = cc.getNormRuleSet.get.iterator().asScala.toList
    assertEquals(3, cList.size)
    assertEquals(cList.head, ReduceExpressionsRule.FILTER_INSTANCE)
    assertEquals(cList(1), ReduceExpressionsRule.PROJECT_INSTANCE)
    assertEquals(cList(2), ReduceExpressionsRule.CALC_INSTANCE)
  }

  @Test
  def testReplaceOptimizationRules(): Unit = {

    val cc: CalciteConfig = new CalciteConfigBuilder()
      .replaceOptRuleSet(RuleSets.ofList(FilterMergeRule.INSTANCE))
      .build()

    assertEquals(true, cc.replacesOptRuleSet)
    assertTrue(cc.getOptRuleSet.isDefined)
    val cSet = cc.getOptRuleSet.get.iterator().asScala.toSet
    assertEquals(1, cSet.size)
    assertTrue(cSet.contains(FilterMergeRule.INSTANCE))
  }

  @Test
  def testReplaceOptimizationAddRules(): Unit = {

    val cc: CalciteConfig = new CalciteConfigBuilder()
      .replaceOptRuleSet(RuleSets.ofList(FilterMergeRule.INSTANCE))
      .addOptRuleSet(RuleSets.ofList(CalcMergeRule.INSTANCE, CalcSplitRule.INSTANCE))
      .build()

    assertEquals(true, cc.replacesOptRuleSet)
    assertTrue(cc.getOptRuleSet.isDefined)
    val cSet = cc.getOptRuleSet.get.iterator().asScala.toSet
    assertEquals(3, cSet.size)
    assertTrue(cSet.contains(FilterMergeRule.INSTANCE))
    assertTrue(cSet.contains(CalcMergeRule.INSTANCE))
    assertTrue(cSet.contains(CalcSplitRule.INSTANCE))
  }

  @Test
  def testAddOptimizationRules(): Unit = {

    val cc: CalciteConfig = new CalciteConfigBuilder()
      .addOptRuleSet(RuleSets.ofList(FilterMergeRule.INSTANCE))
      .build()

    assertEquals(false, cc.replacesOptRuleSet)
    assertTrue(cc.getOptRuleSet.isDefined)
    val cSet = cc.getOptRuleSet.get.iterator().asScala.toSet
    assertEquals(1, cSet.size)
    assertTrue(cSet.contains(FilterMergeRule.INSTANCE))
  }

  @Test
  def testAddAddOptimizationRules(): Unit = {

    val cc: CalciteConfig = new CalciteConfigBuilder()
      .addOptRuleSet(RuleSets.ofList(FilterMergeRule.INSTANCE))
      .addOptRuleSet(RuleSets.ofList(CalcMergeRule.INSTANCE, CalcSplitRule.INSTANCE))
      .build()

    assertEquals(false, cc.replacesOptRuleSet)
    assertTrue(cc.getOptRuleSet.isDefined)
    val cSet = cc.getOptRuleSet.get.iterator().asScala.toSet
    assertEquals(3, cSet.size)
    assertTrue(cSet.contains(FilterMergeRule.INSTANCE))
    assertTrue(cSet.contains(CalcMergeRule.INSTANCE))
    assertTrue(cSet.contains(CalcSplitRule.INSTANCE))
  }

  @Test
  def testReplaceDecorationRules(): Unit = {

    val cc: CalciteConfig = new CalciteConfigBuilder()
      .replaceDecoRuleSet(RuleSets.ofList(ReduceExpressionsRule.FILTER_INSTANCE))
      .build()

    assertEquals(true, cc.replacesDecoRuleSet)
    assertTrue(cc.getDecoRuleSet.isDefined)
    val cSet = cc.getDecoRuleSet.get.iterator().asScala.toSet
    assertEquals(1, cSet.size)
    assertTrue(cSet.contains(ReduceExpressionsRule.FILTER_INSTANCE))
  }

  @Test
  def testReplaceDecorationAddRules(): Unit = {

    val cc: CalciteConfig = new CalciteConfigBuilder()
      .replaceDecoRuleSet(RuleSets.ofList(ReduceExpressionsRule.FILTER_INSTANCE))
      .addDecoRuleSet(RuleSets.ofList(ReduceExpressionsRule.PROJECT_INSTANCE))
      .build()

    assertEquals(true, cc.replacesDecoRuleSet)
    assertTrue(cc.getDecoRuleSet.isDefined)
    val cSet = cc.getDecoRuleSet.get.iterator().asScala.toSet
    assertEquals(2, cSet.size)
    assertTrue(cSet.contains(ReduceExpressionsRule.FILTER_INSTANCE))
    assertTrue(cSet.contains(ReduceExpressionsRule.PROJECT_INSTANCE))
  }

  @Test
  def testAddDecorationRules(): Unit = {

    val cc: CalciteConfig = new CalciteConfigBuilder()
      .addDecoRuleSet(RuleSets.ofList(ReduceExpressionsRule.FILTER_INSTANCE))
      .build()

    assertEquals(false, cc.replacesDecoRuleSet)
    assertTrue(cc.getDecoRuleSet.isDefined)
    val cSet = cc.getDecoRuleSet.get.iterator().asScala.toSet
    assertEquals(1, cSet.size)
    assertTrue(cSet.contains(ReduceExpressionsRule.FILTER_INSTANCE))
  }

  @Test
  def testAddAddDecorationRules(): Unit = {

    val cc: CalciteConfig = new CalciteConfigBuilder()
      .addDecoRuleSet(RuleSets.ofList(ReduceExpressionsRule.FILTER_INSTANCE))
      .addDecoRuleSet(RuleSets.ofList(ReduceExpressionsRule.PROJECT_INSTANCE,
                                      ReduceExpressionsRule.CALC_INSTANCE))
      .build()

    assertEquals(false, cc.replacesDecoRuleSet)
    assertTrue(cc.getDecoRuleSet.isDefined)
    val cList = cc.getDecoRuleSet.get.iterator().asScala.toList
    assertEquals(3, cList.size)
    assertEquals(cList.head, ReduceExpressionsRule.FILTER_INSTANCE)
    assertEquals(cList(1), ReduceExpressionsRule.PROJECT_INSTANCE)
    assertEquals(cList(2), ReduceExpressionsRule.CALC_INSTANCE)
  }

  @Test
  def testDefaultOperatorTable(): Unit = {

    val cc: CalciteConfig = new CalciteConfigBuilder()
      .build()

    assertEquals(false, cc.replacesSqlOperatorTable)
    assertFalse(cc.getSqlOperatorTable.isDefined)
  }

  @Test
  def testReplaceOperatorTable(): Unit = {

    val oracleTable = new OracleSqlOperatorTable

    val cc: CalciteConfig = new CalciteConfigBuilder()
      .replaceSqlOperatorTable(oracleTable)
      .build()

    val oracleOps = oracleTable.getOperatorList.asScala

    assertEquals(true, cc.replacesSqlOperatorTable)
    assertTrue(cc.getSqlOperatorTable.isDefined)
    val ops = cc.getSqlOperatorTable.get.getOperatorList
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
    assertTrue(cc.getSqlOperatorTable.isDefined)
    val ops = cc.getSqlOperatorTable.get.getOperatorList
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
    assertTrue(cc.getSqlOperatorTable.isDefined)
    val ops = cc.getSqlOperatorTable.get.getOperatorList
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
    assertTrue(cc.getSqlOperatorTable.isDefined)
    val ops = cc.getSqlOperatorTable.get.getOperatorList
      .asScala.toSet
    assertEquals(oracleOps.size + stdOps.size, ops.size)
    for (o <- oracleOps) {
      assertTrue(ops.contains(o))
    }
    for (o <- stdOps) {
      assertTrue(ops.contains(o))
    }

  }


}
