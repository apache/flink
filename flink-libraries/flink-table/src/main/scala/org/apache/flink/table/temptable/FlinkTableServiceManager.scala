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

package org.apache.flink.table.temptable

import java.util.UUID

import org.apache.flink.annotation.VisibleForTesting
import org.apache.flink.api.common.JobSubmissionResult
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.service.ServiceDescriptor
import org.apache.flink.table.plan.logical.LogicalNode
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.factories.TableFactory
import org.apache.flink.table.plan.CacheAwareRelNodePlanBuilder
import org.apache.flink.table.temptable.util.TableServiceUtil
import org.apache.flink.table.util.TableProperties
import org.apache.flink.table.api.{Table, TableEnvironment}

import scala.collection.JavaConverters._

/**
  * TableServiceManager is for maintain status of TableService and CachedTables.
  */
class FlinkTableServiceManager(tEnv: TableEnvironment) {

  private val toBeCachedTables = new java.util.IdentityHashMap[LogicalNode, String]

  private[flink] val cachedTables = new java.util.IdentityHashMap[LogicalNode, String]

  private[flink] var tableServiceStarted = false

  private[flink] var submitResult: JobSubmissionResult = _

  private var tableServiceEnv: StreamExecutionEnvironment = _

  private lazy val tableServiceId: String = UUID.randomUUID().toString

  def getTableServiceFactory(): Option[TableFactory] = {
    Option(tEnv.getConfig.getTableServiceFactoryDescriptor().getTableFactory)
  }

  def getTableServiceFactoryProperties(): Option[TableProperties] = {
    Option(tEnv.getConfig.getTableServiceFactoryDescriptor().getTableProperties)
  }

  def getTableServiceDescriptor(): Option[ServiceDescriptor] = {
    Option(tEnv.getConfig.getTableServiceDescriptor)
  }

  private[flink] val cachePlanBuilder: CacheAwareRelNodePlanBuilder =
    new CacheAwareRelNodePlanBuilder(tEnv)

  /**
    * Record the uuid of a table to be cached & adding Source/Sink LogicalNode
    * @param table The table to be cached.
    */
  private def cacheTable(table: Table, tableUUID: String): Unit = {
    toBeCachedTables.put(table.logicalPlan, tableUUID)

    val cacheSink = cachePlanBuilder.createCacheTableSink(tableUUID, table.logicalPlan)
    tEnv.writeToSink(table, cacheSink)

    val cacheSource = cachePlanBuilder.createCacheTableSource(tableUUID, table.logicalPlan)
    if (tEnv.getTable(tableUUID).isEmpty) {
      tEnv.registerTableSource(tableUUID, cacheSource)
    }
  }

  def cacheTable(table: Table): Unit = {
    val name = java.util.UUID.randomUUID().toString
    cacheTable(table, name)
  }

  /**
    * invalidate all cached tables if TableService is failed.
    */
  def invalidateCachedTable(): Unit = {
    cachedTables.asScala.foreach {
      case (plan, name) => cacheTable(new Table(tEnv, plan), name)
    }
    cachedTables.clear()
  }

  private[flink] def markAllTablesCached(): Unit = {
    cachedTables.putAll(toBeCachedTables)
    toBeCachedTables.clear()
  }

  private[flink] def getToBeCachedTableName(logicalPlan: LogicalNode): Option[String] = {
    Option(toBeCachedTables.get(logicalPlan))
  }

  private[flink] def getCachedTableName(logicalPlan: LogicalNode): Option[String] = {
    Option(cachedTables.get(logicalPlan))
  }

  def startTableServiceJob(): Unit = {
    if(!toBeCachedTables.isEmpty){
      val descriptor = getTableServiceDescriptor().get
      startTableServiceJobInternally(descriptor)
    }
  }

  def getTableServiceId(): String = tableServiceId

  @VisibleForTesting
  private[flink] def startTableServiceJobInternally(descriptor: ServiceDescriptor): Unit = {
    if (!tableServiceStarted) {
      val executionEnv = StreamExecutionEnvironment.getExecutionEnvironment
      executionEnv.setRestartStrategy(
        RestartStrategies.fixedDelayRestart(Integer.MAX_VALUE, 500L))
      descriptor.getConfiguration.setString(TableServiceOptions.TABLE_SERVICE_ID, tableServiceId)
      TableServiceUtil.createTableServiceJob(executionEnv, descriptor)
      submitResult = executionEnv.submit("FlinkTableServiceJob")
      tableServiceEnv = executionEnv
      tableServiceStarted = true
    }
  }

  def close(): Unit = {
    if (tableServiceStarted) {
      tableServiceEnv.stopJob(submitResult.getJobID)
      tableServiceStarted = false
    }
  }

}
