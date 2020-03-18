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

package org.apache.flink.table.planner.calcite

import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.planner.plan.optimize.program.FlinkStreamProgram

import org.apache.calcite.config.Lex
import org.apache.calcite.sql.fun.{OracleSqlOperatorTable, SqlStdOperatorTable}
import org.apache.calcite.sql.parser.SqlParser
import org.apache.calcite.sql2rel.SqlToRelConverter
import org.junit.Assert._
import org.junit.Test

import scala.collection.JavaConverters._

class CalciteConfigBuilderTest {

  @Test
  def testProgram(): Unit = {
    val cc: CalciteConfig = new CalciteConfigBuilder().build()
    assertTrue(cc.getStreamProgram.isEmpty)

    val builder = new CalciteConfigBuilder()
    val streamPrograms = FlinkStreamProgram.buildProgram(TableConfig.getDefault.getConfiguration)
    streamPrograms.remove(FlinkStreamProgram.PHYSICAL)
    builder.replaceStreamProgram(streamPrograms)

    val config = builder.build()
    assertTrue(config.getStreamProgram.isDefined)
    assertTrue(streamPrograms == config.getStreamProgram.get)
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

  @Test
  def testReplaceSqlToRelConverterConfig(): Unit = {
    val config = SqlToRelConverter.configBuilder()
      .withTrimUnusedFields(false)
      .withConvertTableAccess(false)
      .withExpand(false)
      .build()

    val cc: CalciteConfig = new CalciteConfigBuilder()
      .replaceSqlToRelConverterConfig(config)
      .build()

    assertTrue(cc.getSqlToRelConverterConfig.isDefined)
    assertEquals(false, cc.getSqlToRelConverterConfig.get.isExpand)
  }

  @Test
  def testCreateBuilderBasedOnAntherConfig(): Unit = {
    val builder = CalciteConfig.createBuilder(CalciteConfig.DEFAULT)
    val config = builder.build()
    assertTrue(config.getBatchProgram.isEmpty)
    assertTrue(config.getStreamProgram.isEmpty)
    assertTrue(config.getSqlOperatorTable.isEmpty)
    assertFalse(config.replacesSqlOperatorTable)
    assertTrue(config.getSqlParserConfig.isEmpty)
    assertTrue(config.getSqlToRelConverterConfig.isEmpty)

    val streamPrograms = FlinkStreamProgram.buildProgram(TableConfig.getDefault.getConfiguration)
    streamPrograms.remove(FlinkStreamProgram.PHYSICAL)
    builder.replaceStreamProgram(streamPrograms)
    val baseConfig1 = builder.build()
    val builder1 = CalciteConfig.createBuilder(baseConfig1)
    val config1 = builder1.build()
    assertTrue(streamPrograms == config1.getStreamProgram.orNull)
    assertTrue(config1.getBatchProgram.isEmpty)
    assertTrue(config1.getSqlOperatorTable.isEmpty)
    assertFalse(config1.replacesSqlOperatorTable)
    assertTrue(config1.getSqlParserConfig.isEmpty)
    assertTrue(config1.getSqlToRelConverterConfig.isEmpty)

    val sqlToRelConvertConfig = SqlToRelConverter.configBuilder()
      .withTrimUnusedFields(false)
      .withConvertTableAccess(false)
      .withExpand(false)
      .build()
    builder.replaceSqlToRelConverterConfig(sqlToRelConvertConfig)
    val baseConfig2 = builder.build()
    val builder2 = CalciteConfig.createBuilder(baseConfig2)
    val config2 = builder2.build()
    assertTrue(streamPrograms == config2.getStreamProgram.orNull)
    assertTrue(sqlToRelConvertConfig == config2.getSqlToRelConverterConfig.orNull)
    assertTrue(config2.getBatchProgram.isEmpty)
    assertTrue(config2.getSqlOperatorTable.isEmpty)
    assertFalse(config2.replacesSqlOperatorTable)
    assertTrue(config2.getSqlParserConfig.isEmpty)

    val sqlParserConfig = SqlParser.configBuilder()
      .setLex(Lex.ORACLE)
      .build()
    builder.replaceSqlParserConfig(sqlParserConfig)
    val baseConfig3 = builder.build()
    val builder3 = CalciteConfig.createBuilder(baseConfig3)
    val config3 = builder3.build()
    assertTrue(streamPrograms == config3.getStreamProgram.orNull)
    assertTrue(sqlToRelConvertConfig == config3.getSqlToRelConverterConfig.orNull)
    assertTrue(sqlParserConfig == config3.getSqlParserConfig.orNull)
    assertTrue(config3.getBatchProgram.isEmpty)
    assertTrue(config3.getSqlOperatorTable.isEmpty)
    assertFalse(config3.replacesSqlOperatorTable)

    val oracleTable = new OracleSqlOperatorTable
    builder.addSqlOperatorTable(oracleTable)
    val baseConfig4 = builder.build()
    val builder4 = CalciteConfig.createBuilder(baseConfig4)
    val config4 = builder4.build()
    assertTrue(streamPrograms == config4.getStreamProgram.orNull)
    assertTrue(sqlToRelConvertConfig == config4.getSqlToRelConverterConfig.orNull)
    assertTrue(sqlParserConfig == config4.getSqlParserConfig.orNull)
    assertTrue(config4.getBatchProgram.isEmpty)
    assertTrue(config4.getSqlOperatorTable.isDefined)
    assertTrue(oracleTable == config4.getSqlOperatorTable.get)
    assertFalse(config4.replacesSqlOperatorTable)

    val stdTable = new SqlStdOperatorTable
    builder.replaceSqlOperatorTable(stdTable)
    val baseConfig5 = builder.build()
    val builder5 = CalciteConfig.createBuilder(baseConfig5)
    val config5 = builder5.build()
    assertTrue(streamPrograms == config5.getStreamProgram.orNull)
    assertTrue(sqlToRelConvertConfig == config5.getSqlToRelConverterConfig.orNull)
    assertTrue(sqlParserConfig == config5.getSqlParserConfig.orNull)
    assertTrue(config5.getBatchProgram.isEmpty)
    assertTrue(config5.getSqlOperatorTable.isDefined)
    assertTrue(stdTable == config5.getSqlOperatorTable.get)
    assertTrue(config5.replacesSqlOperatorTable)
  }
}
