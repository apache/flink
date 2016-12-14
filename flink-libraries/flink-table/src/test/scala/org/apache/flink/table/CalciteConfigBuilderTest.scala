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

import org.apache.calcite.rel.rules.{CalcSplitRule, CalcMergeRule, FilterMergeRule}
import org.apache.calcite.sql.fun.{SqlStdOperatorTable, OracleSqlOperatorTable}
import org.apache.calcite.tools.RuleSets
import org.apache.flink.table.calcite.{CalciteConfigBuilder, CalciteConfig}
import org.junit.Test
import org.junit.Assert._

import scala.collection.JavaConverters._

class CalciteConfigBuilderTest {

  @Test
  def testDefaultRules(): Unit = {

    val cc: CalciteConfig = new CalciteConfigBuilder()
      .build()

    assertEquals(false, cc.replacesRuleSet)
    assertFalse(cc.getRuleSet.isDefined)
  }

  @Test
  def testReplaceRules(): Unit = {

    val cc: CalciteConfig = new CalciteConfigBuilder()
      .replaceRuleSet(RuleSets.ofList(FilterMergeRule.INSTANCE))
      .build()

    assertEquals(true, cc.replacesRuleSet)
    assertTrue(cc.getRuleSet.isDefined)
    val cSet = cc.getRuleSet.get.iterator().asScala.toSet
    assertEquals(1, cSet.size)
    assertTrue(cSet.contains(FilterMergeRule.INSTANCE))
  }

  @Test
  def testReplaceAddRules(): Unit = {

    val cc: CalciteConfig = new CalciteConfigBuilder()
      .replaceRuleSet(RuleSets.ofList(FilterMergeRule.INSTANCE))
      .addRuleSet(RuleSets.ofList(CalcMergeRule.INSTANCE, CalcSplitRule.INSTANCE))
      .build()

    assertEquals(true, cc.replacesRuleSet)
    assertTrue(cc.getRuleSet.isDefined)
    val cSet = cc.getRuleSet.get.iterator().asScala.toSet
    assertEquals(3, cSet.size)
    assertTrue(cSet.contains(FilterMergeRule.INSTANCE))
    assertTrue(cSet.contains(CalcMergeRule.INSTANCE))
    assertTrue(cSet.contains(CalcSplitRule.INSTANCE))
  }

  @Test
  def testAddRules(): Unit = {

    val cc: CalciteConfig = new CalciteConfigBuilder()
      .addRuleSet(RuleSets.ofList(FilterMergeRule.INSTANCE))
      .build()

    assertEquals(false, cc.replacesRuleSet)
    assertTrue(cc.getRuleSet.isDefined)
    val cSet = cc.getRuleSet.get.iterator().asScala.toSet
    assertEquals(1, cSet.size)
    assertTrue(cSet.contains(FilterMergeRule.INSTANCE))
  }

  @Test
  def testAddAddRules(): Unit = {

    val cc: CalciteConfig = new CalciteConfigBuilder()
      .addRuleSet(RuleSets.ofList(FilterMergeRule.INSTANCE))
      .addRuleSet(RuleSets.ofList(CalcMergeRule.INSTANCE, CalcSplitRule.INSTANCE))
      .build()

    assertEquals(false, cc.replacesRuleSet)
    assertTrue(cc.getRuleSet.isDefined)
    val cSet = cc.getRuleSet.get.iterator().asScala.toSet
    assertEquals(3, cSet.size)
    assertTrue(cSet.contains(FilterMergeRule.INSTANCE))
    assertTrue(cSet.contains(CalcMergeRule.INSTANCE))
    assertTrue(cSet.contains(CalcSplitRule.INSTANCE))
  }

  @Test
  def testDefaultOperatorTable(): Unit = {

    val cc: CalciteConfig = new CalciteConfigBuilder()
      .build()

    assertEquals(false, cc.replacesSqlOperatorTable)
    assertFalse(cc.getSqlOperatorTable.isDefined)
  }

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
