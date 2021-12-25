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
package org.apache.flink.table.planner.plan.rules.logical.subquery

import org.apache.flink.table.planner.calcite.CalciteConfig
import org.apache.flink.table.planner.plan.optimize.program.FlinkBatchProgram
import org.apache.flink.table.planner.utils.{BatchTableTestUtil, TableConfigUtils, TableTestBase}

import org.apache.calcite.sql2rel.SqlToRelConverter

/**
  * Tests for [[org.apache.flink.table.planner.plan.rules.logical.FlinkSubQueryRemoveRule]].
  */
class SubQueryTestBase extends TableTestBase {
  protected val util: BatchTableTestUtil = batchTestUtil()

  util.buildBatchProgram(FlinkBatchProgram.DEFAULT_REWRITE)
  var calciteConfig = TableConfigUtils.getCalciteConfig(util.tableEnv.getConfig)
  val builder = CalciteConfig.createBuilder(calciteConfig)
  builder.replaceSqlToRelConverterConfig(
    SqlToRelConverter.config()
      .withTrimUnusedFields(false)
      .withExpand(false)
      .withInSubQueryThreshold(3))

  util.tableEnv.getConfig.setPlannerConfig(builder.build())
}
