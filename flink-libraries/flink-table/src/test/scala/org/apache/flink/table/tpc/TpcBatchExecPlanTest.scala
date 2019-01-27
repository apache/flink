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

import org.apache.flink.core.fs.Path
import org.apache.flink.table.api.TableConfigOptions
import org.apache.flink.table.api.types.InternalType
import org.apache.flink.table.dataformat.ColumnarRow
import org.apache.flink.table.plan.rules.physical.batch.runtimefilter.InsertRuntimeFilterRule
import org.apache.flink.table.plan.stats.TableStats
import org.apache.flink.table.plan.util.FlinkNodeOptUtil
import org.apache.flink.table.sources.parquet.{ParquetTableSource, ParquetVectorizedColumnRowTableSource}
import org.apache.flink.table.tpc.STATS_MODE.STATS_MODE
import org.apache.flink.table.util.TableTestBase

import org.apache.calcite.sql.SqlExplainLevel

import java.util.{Set => JSet}

import scala.collection.JavaConverters._
import scala.collection.JavaConversions._

import org.junit.{Before, Test}
import org.scalatest.prop.PropertyChecks

abstract class TpcBatchExecPlanTest(
    caseName: String,
    factor: Int,
    statsMode: STATS_MODE,
    explainLevel: SqlExplainLevel,
    joinReorderEnabled: Boolean,
    printOptimizedResult: Boolean,
    schemaMap: Map[String, Schema],
    statsMap: Map[String, TableStats])
  extends TableTestBase
  with PropertyChecks {

  private val util = batchTestUtil()
  protected val tEnv = util.tableEnv

  @Before
  def before(): Unit = {
    for ((tableName, schema) <- schemaMap) {
      lazy val tableSource = new TestParquetTableSource(
        tableName,
        schema.getFieldTypes,
        schema.getFieldNames,
        schema.getFieldNullables,
        schema.getUniqueKeys)
      tEnv.registerTableSource(tableName, tableSource)
    }
    // alter TableStats
    for ((tableName, tableStats) <- statsMap) {
      tEnv.alterTableStats(tableName, Some(tableStats))
    }
    // set table config
    setUpTableConfig()
  }

  // create a new ParquetTableSource to override `createTableSource` and `getTableStats` methods
  protected class TestParquetTableSource(
    tableName: String,
    fieldTypes: Array[InternalType],
    fieldNames: Array[String],
    fieldNullables: Array[Boolean],
    uniqueKeySet: JSet[JSet[String]] = null) extends ParquetVectorizedColumnRowTableSource(
    new Path("/tmp"), fieldTypes, fieldNames, fieldNullables, true, uniqueKeySet = uniqueKeySet) {

    override protected def createTableSource(
      fieldTypes: Array[InternalType],
      fieldNames: Array[String],
      fieldNullables: Array[Boolean]): ParquetTableSource[ColumnarRow] = {
      val newUniqueKeys = if (uniqueKeySet != null) {
        uniqueKeySet.filter(_.forall(fieldNames.contains)).asJava
      } else {
        null
      }
      val tableSource = new TestParquetTableSource(
        tableName,
        fieldTypes,
        fieldNames,
        fieldNullables,
        newUniqueKeys)
      tableSource.setFilterPredicate(filterPredicate)
      tableSource.setFilterPushedDown(filterPushedDown)
      tableSource
    }

    override def getTableStats: TableStats = {
      // the `filterPredicate` in TPCH queries can not drop any row group for current test data,
      // we can directly use the static statistics.
      // TODO if the test data or TPCH queries are changed, the statistics should also be updated.
      TpchTableStatsProvider.getTableStatsMap(factor, STATS_MODE.PART).get(tableName).orNull
    }

    override def explainSource(): String = {
      s"TestParquetTableSource -> " +
        s"selectedFields=[${fieldNames.mkString(", ")}];" +
        s"filterPredicates=[${if (filterPredicate == null) "" else filterPredicate.toString}]"
    }
  }

  @Test
  def test(): Unit = {
    InsertRuntimeFilterRule.resetBroadcastIdCounter()
    val sqlQuery = getQuery
    if (printOptimizedResult) {
      val table = tEnv.sqlQuery(sqlQuery)
      val optimizedNode = tEnv.optimizeAndTranslateNodeDag(false, table.logicalPlan).head
      val result = FlinkNodeOptUtil.treeToString(optimizedNode, detailLevel = explainLevel)
      println(s"caseName:$caseName, factor: $factor, statsMode:$statsMode\n$result")
    } else {
      util.verifyPlan(sqlQuery, explainLevel)
    }
  }

  def getQuery(): String

  def setUpTableConfig(): Unit = {
    TpcUtils.disableParquetFilterPushDown(tEnv)
    tEnv.getConfig.getConf.setBoolean(
      TableConfigOptions.SQL_OPTIMIZER_JOIN_REORDER_ENABLED, joinReorderEnabled)
    tEnv.getConfig.getConf.setBoolean(TableConfigOptions.SQL_EXEC_RUNTIME_FILTER_ENABLED, true)
  }

}
