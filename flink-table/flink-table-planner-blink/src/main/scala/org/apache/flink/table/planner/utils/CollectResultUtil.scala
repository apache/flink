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

package org.apache.flink.table.planner.utils

import org.apache.flink.api.common.accumulators.SerializedListAccumulator
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.api.internal.{TableEnvironmentImpl, TableImpl}
import org.apache.flink.table.api.{EnvironmentSettings, Table, TableException}
import org.apache.flink.table.planner.delegation.{PlannerBase, StreamPlanner}
import org.apache.flink.table.planner.sinks.{CollectRowTableSink, CollectTableSink}
import org.apache.flink.table.runtime.types.TypeInfoLogicalTypeConverter
import org.apache.flink.table.types.utils.TypeConversions.fromDataTypeToLegacyInfo
import org.apache.flink.types.Row
import org.apache.flink.util.AbstractID

import java.util.{UUID, ArrayList => JArrayList, List => JList}

object CollectResultUtil {

  /**
    * Returns an collection that contains all rows in this Table.
    */
  def collect(table: Table): JList[Row] = {
    collect(table, null)
  }

  /**
    * Returns an collection that contains all rows in this Table.
    */
  def collect(
      table: Table,
      jobName: String,
      builtInCatalogName: String = EnvironmentSettings.DEFAULT_BUILTIN_CATALOG,
      builtInDBName: String = EnvironmentSettings.DEFAULT_BUILTIN_DATABASE): JList[Row] = {
    collectSink(
      table, new CollectRowTableSink, Option.apply(jobName), builtInCatalogName, builtInDBName)
  }

  /**
    * Returns an collection as expected type that contains all rows in this Table.
    */
  def collectAsT[T](table: Table, t: TypeInformation[_]): JList[T] = {
    collectAsT(table, t, null)
  }

  /**
    * Returns an collection as expected type that contains all rows in this Table.
    */
  def collectAsT[T](table: Table, t: TypeInformation[_], jobName: String): JList[T] = {
    val sink = new CollectTableSink(_ => t.asInstanceOf[TypeInformation[T]])
    collectSink(table, sink, Option.apply(jobName))
  }

  def collectSink[T](
      table: Table,
      sink: CollectTableSink[T],
      jobName: Option[String],
      builtInCatalogName: String = EnvironmentSettings.DEFAULT_BUILTIN_CATALOG,
      builtInDBName: String = EnvironmentSettings.DEFAULT_BUILTIN_DATABASE): JList[T] = {
    val schema = table.getSchema
    val fieldNames = schema.getFieldNames
    val fieldTypes = schema.getFieldDataTypes.map {
      t => TypeInfoLogicalTypeConverter.fromLogicalTypeToTypeInfo(t.getLogicalType)
    }
    val configuredSink = sink.configure(fieldNames, fieldTypes).asInstanceOf[CollectTableSink[T]]
    collectConfiguredSink(table, configuredSink, jobName, builtInCatalogName, builtInDBName)
  }

  private[flink] def collectConfiguredSink[T](
      table: Table,
      configuredSink: CollectTableSink[T],
      jobName: Option[String],
      builtInCatalogName: String,
      builtInDBName: String): JList[T] = {
    val tEnv = table.asInstanceOf[TableImpl].getTableEnvironment.asInstanceOf[TableEnvironmentImpl]
    if (tEnv.getPlanner.isInstanceOf[StreamPlanner]) {
      throw new TableException("Stream job is not supported yet")
    }

    val execConfig = tEnv.getPlanner.asInstanceOf[PlannerBase].getExecEnv.getConfig
    val typeSerializer = fromDataTypeToLegacyInfo(configuredSink.getConsumedDataType)
      .asInstanceOf[TypeInformation[T]]
      .createSerializer(execConfig)

    val id = new AbstractID().toString
    configuredSink.init(typeSerializer, id)

    val sinkName = UUID.randomUUID().toString
    tEnv.registerTableSink(sinkName, configuredSink)
    tEnv.insertInto(table, builtInCatalogName, builtInDBName, sinkName)

    val res = tEnv.execute(jobName.getOrElse("Flink Job"))
    val accResult: JArrayList[Array[Byte]] = res.getAccumulatorResult(id)
    SerializedListAccumulator.deserializeList(accResult, typeSerializer)
  }

}
