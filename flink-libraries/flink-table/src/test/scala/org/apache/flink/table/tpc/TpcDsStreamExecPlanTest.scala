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
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.table.plan.util.FlinkRelOptUtil
import org.apache.flink.table.runtime.utils.TestingRetractSink
import org.apache.flink.table.sources.csv.CsvTableSource
import org.apache.flink.types.Row

import org.junit.{Ignore, Test}
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

@RunWith(classOf[Parameterized])
class TpcDsStreamExecPlanTest(caseName: String) {

  val env = StreamExecutionEnvironment.getExecutionEnvironment
  env.setParallelism(4)
  val tEnv = TableEnvironment.getTableEnvironment(env)
  def getDataFile(tableName: String): String = {
    getClass.getResource(s"/tpcds/data/$tableName").getFile
  }

  val retractSql = Set("q1", "q2")

  def prepare(): Unit = {
    for ((tableName, schema) <- TpcDsSchemaProvider.schemaMap) {
      lazy val tableSource = CsvTableSource.builder()
          .path(getDataFile(tableName))
          .fields(schema.getFieldNames, schema.getFieldTypes, schema.getFieldNullables)
          .fieldDelimiter("|")
          .lineDelimiter("\n")
          .enableEmptyColumnAsNull()
          .build()
      tEnv.registerTableSource(tableName, tableSource)
    }
  }

  @Test
  def testPlan(): Unit = {
    prepare()
    val sql = TpcUtils.getStreamTpcDsQuery(caseName)
    val table = tEnv.sqlQuery(sql)
    val relNode = table.getRelNode
    val optimized = tEnv.optimize(relNode, updatesAsRetraction = false)
    val result = FlinkRelOptUtil.toString(optimized)
    // println(s"caseName: tpcds$caseName.sql, plan:\n$result")
  }

  @Ignore
  @Test
  def testRun(): Unit = {
    prepare()
    val sql = TpcUtils.getStreamTpcDsQuery(caseName)
    val sink = new TestingRetractSink
    val result = tEnv.sqlQuery(sql).toRetractStream[Row]
      .addSink(sink).setParallelism(1)
    if (retractSql.contains(caseName)) {
      result.setParallelism(1)
    }
    env.execute()
    println(sink.getRetractResults.mkString("\n"))
  }

}

object TpcDsStreamExecPlanTest {
  @Parameterized.Parameters(name = "caseName={0}")
  def parameters() = {
    Array("q1", "q2", "q3", "q4", "q5",
      "q6", "q7", "q8", "q9", "q10", "q11",
      "q12", "q13", "q14a", "q14b", "q15",
      "q16", "q17", "q18", "q19", "q20",
      "q21", "q22", "q23a", "q23b", "q24a", "q24b",
      "q25", "q26", "q27", "q28", "q29", "q30",
      "q31", "q32", "q33", "q34", "q35", "q36",
      "q37", "q38", "q39a","q39b", "q40", "q41", "q42",
      "q43", "q44", "q45", "q46", "q47", "q48",
      "q49", "q50", "q51", "q52", "q53", "q54",
      "q55", "q56", "q57", "q58", "q59", "q60",
      "q61", "q62", "q63", "q64", "q65", "q66",
      "q67", "q68", "q69", "q70", "q71", "q72",
      "q73", "q74", "q75", "q76", "q77", "q78",
      "q79", "q80", "q81", "q82", "q83", "q84",
      "q85", "q86", "q87", "q88", "q89", "q90",
      "q91", "q92", "q93", "q94", "q95", "q96",
      "q97", "q98", "q99").flatMap {
      q => Seq(Array(q))
    }
  }

}
