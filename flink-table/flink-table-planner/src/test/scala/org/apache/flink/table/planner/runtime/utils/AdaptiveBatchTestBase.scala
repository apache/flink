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
package org.apache.flink.table.planner.runtime.utils

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.api.{EnvironmentSettings, TableConfig, TableEnvironment}
import org.apache.flink.table.api.config.ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM
import org.apache.flink.table.api.internal.TableEnvironmentImpl
import org.apache.flink.table.planner.delegation.PlannerBase
import org.apache.flink.table.planner.factories.TestValuesTableFactory
import org.apache.flink.table.planner.runtime.utils.AdaptiveBatchAbstractTestBase.DEFAULT_PARALLELISM
import org.apache.flink.table.planner.runtime.utils.BatchTableEnvUtil.parseFieldNames
import org.apache.flink.table.planner.runtime.utils.BatchTestBase.{check, checkSame}
import org.apache.flink.table.planner.utils.TestingTableEnvironment
import org.apache.flink.types.Row

import org.junit.jupiter.api.{AfterEach, BeforeEach}

class AdaptiveBatchTestBase extends AdaptiveBatchAbstractTestBase {
  protected var tEnv: TableEnvironment = _

  @throws(classOf[Exception])
  @BeforeEach
  def setupEnv(): Unit = {
    tEnv = TestingTableEnvironment
      .create(
        EnvironmentSettings.newInstance().inBatchMode().build(),
        catalogManager = None,
        TableConfig.getDefault)
    tEnv
      .asInstanceOf[TableEnvironmentImpl]
      .getPlanner
      .asInstanceOf[PlannerBase]
      .getExecEnv
      .getConfig
      .enableObjectReuse()
  }

  @throws(classOf[Exception])
  @BeforeEach
  def before(): Unit = {}

  @AfterEach
  def after(): Unit = {
    TestValuesTableFactory.clearAllData()
  }

  def registerCollection[T](
      tableName: String,
      data: Iterable[T],
      typeInfo: TypeInformation[T],
      fields: String,
      fieldNullables: Array[Boolean],
      forceNonParallel: Boolean = true): Unit = {
    BatchTableEnvUtil.registerCollection(
      tEnv,
      tableName,
      data,
      typeInfo,
      Some(parseFieldNames(fields)),
      Option(fieldNullables),
      None,
      forceNonParallel)
  }

  def checkResult(sqlQuery: String, expectedResult: Seq[Row], isSorted: Boolean = false): Unit = {
    check(sqlQuery, (result: Seq[Row]) => checkSame(expectedResult, result, isSorted), tEnv)
  }

  def executeQuery(sqlQuery: String): Seq[Row] = {
    BatchTestBase.executeQuery(sqlQuery, tEnv)
  }
}

object AdaptiveBatchTestBase {
  def configForMiniCluster(tableConfig: TableConfig): Unit = {
    tableConfig.set(TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, Int.box(DEFAULT_PARALLELISM))
  }

  def row(args: Any*): Row = {
    BatchTestBase.row(args: _*)
  }
}
