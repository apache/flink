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

import org.apache.calcite.sql.fun.{OracleSqlOperatorTable, SqlStdOperatorTable}
import org.apache.flink.table.plan.optimize._
import org.apache.flink.table.plan.rules.FlinkRuleSets
import org.junit.Assert._
import org.junit.Test

import scala.collection.JavaConverters._

class CalciteConfigBuilderTest {

  @Test
  def testPrograms(): Unit = {
    val cc: CalciteConfig = new CalciteConfigBuilder().build()

    assertNotNull(cc.getBatchPrograms)
    assertNotNull(cc.getStreamPrograms)

    val builder = new CalciteConfigBuilder()
    val batchPrograms = new FlinkChainedPrograms[BatchOptimizeContext]
    batchPrograms.addLast(FlinkBatchPrograms.NORMALIZATION,
      new FlinkHepRuleSetProgramBuilder().add(FlinkRuleSets.DATASET_NORM_RULES).build())
    builder.replaceBatchPrograms(batchPrograms)

    val streamPrograms = FlinkStreamPrograms.buildPrograms()
    streamPrograms.remove(FlinkStreamPrograms.DECORATE)
    builder.replaceStreamPrograms(streamPrograms)

    val config = builder.build()
    assertTrue(batchPrograms == config.getBatchPrograms)
    assertTrue(streamPrograms == config.getStreamPrograms)
  }

  @Test
  def testDefaultOperatorTable(): Unit = {

    val cc: CalciteConfig = new CalciteConfigBuilder().build()

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
