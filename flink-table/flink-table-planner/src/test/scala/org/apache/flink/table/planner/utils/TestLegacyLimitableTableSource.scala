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

import java.util

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.io.CollectionInputFormat
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.api.internal.TableEnvironmentInternal
import org.apache.flink.table.api.{TableEnvironment, TableSchema}
import org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_TYPE
import org.apache.flink.table.descriptors.DescriptorProperties
import org.apache.flink.table.descriptors.Schema.SCHEMA
import org.apache.flink.table.factories.StreamTableSourceFactory
import org.apache.flink.table.sources._
import org.apache.flink.table.utils.EncodingUtils
import org.apache.flink.types.Row

import scala.collection.JavaConverters._

/**
  * The table source which support push-down the limit to the source.
  */
class TestLegacyLimitableTableSource(
    data: Seq[Row],
    rowType: RowTypeInfo,
    var limit: Long = -1,
    var limitablePushedDown: Boolean = false)
  extends StreamTableSource[Row]
  with LimitableTableSource[Row] {

  override def isBounded = true

  override def getDataStream(execEnv: StreamExecutionEnvironment): DataStream[Row] = {
    if (limit == 0) {
      throw new RuntimeException("limit 0 should be optimize to single values.")
    }
    val dataSet = if (limit > 0) {
      data.take(limit.toInt).asJava
    } else {
      data.asJava
    }
    execEnv.createInput(
      new CollectionInputFormat(dataSet, rowType.createSerializer(new ExecutionConfig)),
      rowType)
  }

  override def applyLimit(limit: Long): TableSource[Row] = {
    new TestLegacyLimitableTableSource(data, rowType, limit, limitablePushedDown)
  }

  override def isLimitPushedDown: Boolean = limitablePushedDown

  override def getReturnType: TypeInformation[Row] = rowType

  override def explainSource(): String = {
    if (limit > 0) {
      "limit: " + limit
    } else {
      ""
    }
  }

  override def getTableSchema: TableSchema = TableSchema.fromTypeInfo(rowType)
}

object TestLegacyLimitableTableSource {
  def createTemporaryTable(
      tEnv: TableEnvironment,
      data: Seq[Row],
      schema: TableSchema,
      tableName: String): Unit = {
    val source = new TestLegacyLimitableTableSource(data,
      schema.toRowType.asInstanceOf[RowTypeInfo], -1L, false)
    tEnv.asInstanceOf[TableEnvironmentInternal].registerTableSourceInternal(tableName, source)
  }
}

class TestLegacyLimitableTableSourceFactory extends StreamTableSourceFactory[Row] {
  override def createStreamTableSource(
      properties: util.Map[String, String]): StreamTableSource[Row] = {
    val dp = new DescriptorProperties
    dp.putProperties(properties)
    val tableSchema = dp.getTableSchema(SCHEMA)

    val serializedData = dp.getOptionalString("data").orElse(null)
    val data = if (serializedData != null) {
      EncodingUtils.decodeStringToObject(serializedData, classOf[List[Row]])
    } else {
      null
    }

    val limit = dp.getOptionalLong("limit").orElse(-1L)

    val limitablePushedDown = dp.getOptionalBoolean("limitable-push-down").orElse(false)
    new TestLegacyLimitableTableSource(
      data, tableSchema.toRowType.asInstanceOf[RowTypeInfo], limit, limitablePushedDown)
  }

  override def requiredContext(): util.Map[String, String] = {
    val context = new util.HashMap[String, String]()
    context.put(CONNECTOR_TYPE, "TestLimitableTableSource")
    context
  }

  override def supportedProperties(): util.List[String] = {
    val supported = new util.ArrayList[String]()
    supported.add("*")
    supported
  }
}

