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

import org.apache.flink.table.api.TableConfigOptions
import org.apache.flink.table.runtime.batch.sql.BatchTestBase
import org.apache.flink.table.sources.csv.CsvTableSource
import org.apache.flink.table.tpc.TpcUtils.getTpcHQuery
import org.apache.flink.test.util.TestBaseUtils

import org.junit.{Before, Test}
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.scalatest.prop.PropertyChecks

import java.util

import scala.collection.JavaConversions._

// TODO support externalShuffle test.
// TODO now there is no way to test in externalShuffle.
@RunWith(classOf[Parameterized])
class TpcHBatchExecITCase(caseName: String,
    subsectionOptimization: Boolean)
  extends BatchTestBase with PropertyChecks {

  def getDataFile(tableName: String): String = {
    getClass.getResource(s"/tpch/data/$tableName/$tableName.tbl").getFile
  }

  @Before
  def prepareOp(): Unit = {
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
    tEnv.getConfig.getConf.setInteger(TableConfigOptions.SQL_RESOURCE_DEFAULT_PARALLELISM, 3)
    tEnv.getConfig.getConf.setInteger(TableConfigOptions.SQL_EXEC_SORT_DEFAULT_LIMIT, -1)
    TpcUtils.disableBroadcastHashJoin(tEnv)
    TpcUtils.disableRangeSort(tEnv)
    tEnv.getConfig.setSubsectionOptimization(subsectionOptimization)
  }

  def execute(caseName: String): Unit = {
    val result = TpcUtils.formatResult(executeQuery(parseQuery(getTpcHQuery(caseName))))
    TestBaseUtils.compareResultAsText(result, TpcUtils.getTpcHResult(caseName))
  }

  @Test
  def test(): Unit = {
    execute(caseName)
  }
}

object TpcHBatchExecITCase {
  @Parameterized.Parameters(name = "{0}, {1}")
  def parameters(): util.Collection[Array[_]] = {
    // 15 plan: VIEW is unsupported
    util.Arrays.asList(
      "01", "02", "03", "04", "05", "06", "07", "08", "09", "10",
      "11", "12", "13", "14", "15_1", "16", "17", "18", "19",
      "20", "21", "22"
    ).flatMap { s => Seq(
      Array(s, true),
      Array(s, false))}
  }
}
