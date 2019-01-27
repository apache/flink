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

package org.apache.flink.table.runtime.batch.sql

import java.util

import junit.framework.TestCase.assertTrue
import junit.framework.TestCase.fail
import org.apache.flink.runtime.io.network.DataExchangeMode
import org.apache.flink.streaming.api.graph.{StreamEdge, StreamGraph, StreamNode}
import org.apache.flink.table.api.TableConfigOptions
import org.apache.flink.table.api.types.DataTypes
import org.apache.flink.table.runtime.batch.sql.BatchTestBase.row
import org.apache.flink.table.runtime.utils.CommonTestData.createCsvTableSource
import org.apache.flink.table.sinks.csv.CsvTableSink
import org.junit.Test

import scala.collection.JavaConverters._

/**
  * Test for setting dataExchangeMode in streamGraph.
  */
class DataExchangeModeTest extends BatchTestBase {

  @Test
  def testReuseBatch(): Unit = {
    tEnv.getConfig.setSubsectionOptimization(true)
    tEnv.getConfig.getConf.setBoolean(TableConfigOptions.SQL_OPTIMIZER_REUSE_SUB_PLAN_ENABLED, true)
    tEnv.getConfig.getConf.setBoolean(
      TableConfigOptions.SQL_OPTIMIZER_REUSE_TABLE_SOURCE_ENABLED, true)
    val data = (1 until 100).map(row(_))
    tEnv.registerTableSource("t", createCsvTableSource(data, Array("a"), Array(DataTypes.LONG)))
    verifySql(
      """
        |WITH r AS (SELECT a FROM t LIMIT 100000)
        |SELECT r1.a FROM r r1, r r2 WHERE r1.a = r2.a ORDER BY r1.a LIMIT 3
      """.stripMargin, true)
  }

  @Test
  def testAllDataExchangeModeBatch(): Unit = {
    tEnv.getConfig.setSubsectionOptimization(true)
    tEnv.getConfig.getConf.setBoolean(
      TableConfigOptions.SQL_EXEC_DATA_EXCHANGE_MODE_ALL_BATCH, true)
    tEnv.getConfig.getConf.setBoolean(TableConfigOptions.SQL_OPTIMIZER_REUSE_SUB_PLAN_ENABLED, true)
    tEnv.getConfig.getConf.setBoolean(
      TableConfigOptions.SQL_OPTIMIZER_REUSE_TABLE_SOURCE_ENABLED, true)
    val data = (1 until 100).map(row(_))
    tEnv.registerTableSource("t", createCsvTableSource(data, Array("a"), Array(DataTypes.LONG)))
    verifySql(
      """
        |WITH r AS (SELECT a FROM t LIMIT 100000)
        |SELECT r1.a FROM r r1, r r2 WHERE r1.a = r2.a ORDER BY r1.a LIMIT 3
      """.stripMargin, true)
  }

  @Test
  def testNotContainsBatch(): Unit = {
    tEnv.getConfig.setSubsectionOptimization(true)
    tEnv.getConfig.getConf.setBoolean(TableConfigOptions.SQL_OPTIMIZER_REUSE_SUB_PLAN_ENABLED, true)
    tEnv.getConfig.getConf.setBoolean(
      TableConfigOptions.SQL_OPTIMIZER_REUSE_TABLE_SOURCE_ENABLED, true)
    val data = (1 until 100).map(row(_))
    tEnv.registerTableSource("t1", createCsvTableSource(data, Array("a"), Array(DataTypes.LONG)))
    tEnv.registerTableSource("t2", createCsvTableSource(data, Array("b"), Array(DataTypes.LONG)))
    verifySql(
      """
        |SELECT t1.a FROM t1, t2 WHERE t1.a = t2.b
      """.stripMargin, false)
  }

  private def verifySql(sql: String, containsBatch: Boolean): Unit = {
    tEnv.sqlQuery(sql).writeToSink(new CsvTableSink("/tmp"))
    val streamGraph = tEnv.generateStreamGraph()
    val edgeSet = new util.HashSet[StreamEdge]()
    streamGraph.getSinkIDs.asScala.foreach(id => {
      this.collectStreamEdges(streamGraph, streamGraph.getStreamNode(id), edgeSet)
    })
    var autoNum: Int = 0
    var batchNum: Int = 0
    edgeSet.asScala.foreach(e => {
      if (e.getDataExchangeMode eq DataExchangeMode.AUTO) {
        autoNum += 1
      } else {
        if (e.getDataExchangeMode eq DataExchangeMode.BATCH) {
          batchNum += 1
        } else {
          fail("DataExchangeMode only could be auto or batch.")
        }
      }
    })
    assertTrue("there are at least one auto exchange mode.", autoNum > 0)
    if (containsBatch) {
      assertTrue("there are at least one batch exchange mode.", batchNum > 0)
    }
  }

  private def collectStreamEdges(streamGraph: StreamGraph,
      streamNode: StreamNode, edgeSet: java.util.Set[StreamEdge]): Unit = {
    streamNode.getInEdges.asScala.foreach(e => {
      edgeSet.add(e)
      collectStreamEdges(streamGraph, streamGraph.getStreamNode(e.getSourceId()), edgeSet)
    })
  }
}
