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

package org.apache.flink.table.tpc

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{TableConfig, TableConfigOptions, TableEnvironment}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.plan.util.FlinkRelOptUtil
import org.apache.flink.table.runtime.utils.TestingRetractSink
import org.apache.flink.table.sources.csv.CsvTableSource
import org.apache.flink.types.Row

import java.util

import scala.collection.JavaConversions._

import org.junit.{Before, Ignore, Test}
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

@RunWith(classOf[Parameterized])
class TpcHStreamExecPlanTest(caseName: String, twoStageAgg: Boolean) {

  private val env = StreamExecutionEnvironment.getExecutionEnvironment
  env.setParallelism(4)
  var tableConfig: TableConfig = _
  private var tEnv: StreamTableEnvironment = _
  def getDataFile(tableName: String): String = {
    getClass.getResource(s"/tpch/data/$tableName/$tableName.tbl").getFile
  }

  val retractSinkCase = Seq("06", "11", "14", "15", "17", "19")

  @Before
  def prepare(): Unit = {
    val tableConfig = new TableConfig
    tableConfig.getConf.setBoolean(TableConfigOptions.SQL_OPTIMIZER_JOIN_REORDER_ENABLED, true)
    if (twoStageAgg) {
      tableConfig.getConf.setLong(TableConfigOptions.SQL_EXEC_MINIBATCH_ALLOW_LATENCY, 1000L)
    }
    tEnv = TableEnvironment.getTableEnvironment(env, tableConfig)
    for ((tableName, schema) <- TpcHSchemaProvider.schemaMap) {
      lazy val tableSource = CsvTableSource.builder()
          .path(getDataFile(tableName))
          .fields(schema.getFieldNames, schema.getFieldTypes, schema.getFieldNullables)
          .fieldDelimiter("|")
          .lineDelimiter("\n")
          .uniqueKeys(schema.getUniqueKeys)
          .build()
      tEnv.registerTableSource(tableName, tableSource)
    }
  }

  @Test
  def testPlan(): Unit = {
    prepare()
    val sql = TpcUtils.getStreamTpcHQuery(caseName)
    val table = tEnv.sqlQuery(sql)
    val relNode = table.getRelNode
    val optimized = tEnv.optimize(relNode, updatesAsRetraction = false)
    val result = FlinkRelOptUtil.toString(optimized)
    println(s"caseName: tpch$caseName.sql, plan:\n$result")
  }

  @Ignore
  @Test
  def testRun(): Unit = {
    prepare()
    val sql = TpcUtils.getStreamTpcHQuery(caseName)
    val sink = new TestingRetractSink
    val result = tEnv.sqlQuery(sql).toRetractStream[Row]
      .addSink(sink).setParallelism(1)
    if (retractSinkCase.contains(caseName)) {
      result.setParallelism(1)
    }
    env.execute()
    println(sink.getRetractResults.mkString("\n"))
  }

}

object TpcHStreamExecPlanTest {
  @Parameterized.Parameters(name = "caseName={0}, twoStageAgg={1}")
  def parameters(): util.Collection[Array[Any]] = {
    // tpch15 has precision problem, should set double to decimal
    // 15 plan: VIEW is unsupported
    util.Arrays.asList(
      "01", "02", "03", "04", "05", "06",
      "07", "08", "09", "10", "11", "12", "13",
      "14",  "15", "16", "17", "18", "19", "20", "21", "22"
    ).flatMap { s =>
      Seq(Array(s, true), Array(s, false))
    }
  }
}
