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
package org.apache.flink.table.planner.runtime.stream.sql

import org.apache.flink.table.api.Schema
import org.apache.flink.table.api.bridge.scala.internal.StreamTableEnvironmentImpl
import org.apache.flink.table.api.config.{ExecutionConfigOptions, OptimizerConfigOptions}
import org.apache.flink.table.api.config.OptimizerConfigOptions.DeltaJoinStrategy
import org.apache.flink.table.catalog._
import org.apache.flink.table.planner.factories.TestValuesRuntimeFunctions.AsyncTestValueLookupFunction
import org.apache.flink.table.planner.runtime.utils.StreamingTestBase
import org.apache.flink.testutils.junit.extensions.parameterized.{ParameterizedTestExtension, Parameters}

import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.extension.ExtendWith

import scala.collection.JavaConversions._

/** Base class for delta join integration tests. */
@ExtendWith(Array(classOf[ParameterizedTestExtension]))
class DeltaJoinITCaseBase(enableCache: Boolean) extends StreamingTestBase {

  @BeforeEach
  override def before(): Unit = {
    super.before()

    tEnv.getConfig.set(
      OptimizerConfigOptions.TABLE_OPTIMIZER_DELTA_JOIN_STRATEGY,
      DeltaJoinStrategy.FORCE)

    tEnv.getConfig.set(
      ExecutionConfigOptions.TABLE_EXEC_DELTA_JOIN_CACHE_ENABLED,
      Boolean.box(enableCache))

    AsyncTestValueLookupFunction.invokeCount.set(0)
  }

  /** TODO add [[Index]] and [[ImmutableColumnsConstraint]] in DDL. */
  protected def addIndexesAndImmutableCols(
      tableName: String,
      indexes: List[List[String]],
      immutableCols: List[String]): Unit = {
    if (indexes.isEmpty && immutableCols.isEmpty) {
      return
    }

    val catalogName = tEnv.getCurrentCatalog
    val databaseName = tEnv.getCurrentDatabase
    val tablePath = new ObjectPath(databaseName, tableName)
    val catalog = tEnv.getCatalog(catalogName).get()
    val catalogManager = tEnv.asInstanceOf[StreamTableEnvironmentImpl].getCatalogManager
    val schemaResolver = catalogManager.getSchemaResolver

    val resolvedTable = catalog.getTable(tablePath).asInstanceOf[ResolvedCatalogTable]
    val originTable = resolvedTable.getOrigin
    val originSchema = originTable.getUnresolvedSchema

    val newSchemaBuilder = Schema
      .newBuilder()
      .fromSchema(originSchema)

    if (indexes.nonEmpty) {
      indexes.foreach(index => newSchemaBuilder.index(index))
    }
    if (immutableCols.nonEmpty) {
      newSchemaBuilder.immutableColumns(immutableCols)
    }

    val newSchema = newSchemaBuilder.build()

    val newTable = CatalogTable
      .newBuilder()
      .schema(newSchema)
      .comment(originTable.getComment)
      .partitionKeys(originTable.getPartitionKeys)
      .options(originTable.getOptions)
      .build()
    val newResolvedTable = new ResolvedCatalogTable(newTable, schemaResolver.resolve(newSchema))

    catalog.dropTable(tablePath, false)
    catalog.createTable(tablePath, newResolvedTable, false)
  }

}

object DeltaJoinITCaseBase {
  @Parameters(name = "EnableCache={0}")
  def parameters(): java.util.Collection[Boolean] = {
    Seq[Boolean](true, false)
  }
}
