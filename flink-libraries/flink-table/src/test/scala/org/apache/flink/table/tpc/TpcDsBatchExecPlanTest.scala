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
import org.apache.flink.table.tpc.STATS_MODE.STATS_MODE

import org.apache.calcite.sql.SqlExplainLevel

/**
  * This class is used to get optimized TPC-DS queries plan with TableStats.
  * The TableStats of each table in TpcDsTableStatsProvider is static result,
  * and is independent of any specific TableSource.
  */
abstract class TpcDsBatchExecPlanTest(
    caseName: String,
    factor: Int,
    statsMode: STATS_MODE,
    explainLevel: SqlExplainLevel,
    joinReorderEnabled: Boolean,
    printOptimizedResult: Boolean)
  extends TpcBatchExecPlanTest(
    caseName, factor, statsMode, explainLevel,
    joinReorderEnabled, printOptimizedResult,
    TpcDsSchemaProvider.schemaMap,
    TpcDsTableStatsProvider.getTableStatsMap(factor, statsMode)) {

  override def setUpTableConfig(): Unit = {
    super.setUpTableConfig()
    tEnv.getConfig.getConf.setBoolean(TableConfigOptions.SQL_OPTIMIZER_REUSE_SUB_PLAN_ENABLED, true)
    tEnv.getConfig.getConf.setBoolean(
      TableConfigOptions.SQL_OPTIMIZER_REUSE_TABLE_SOURCE_ENABLED, false)
    tEnv.getConfig.getConf.setLong(TableConfigOptions.SQL_EXEC_HASH_JOIN_BROADCAST_THRESHOLD,
      10 * 1024 * 1024)
  }

  def getQuery(): String = TpcUtils.getTpcDsQuery(caseName, factor)

}
