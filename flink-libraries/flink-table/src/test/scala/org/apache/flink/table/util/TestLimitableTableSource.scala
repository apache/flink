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

package org.apache.flink.table.util

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.io.CollectionInputFormat
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.types.DataType
import org.apache.flink.table.sources._
import org.apache.flink.types.Row

import scala.collection.JavaConverters._

/**
  * The table source which support push-down the limit to the source.
  */
class TestLimitableTableSource(
    data: Seq[Row],
    rowType: RowTypeInfo,
    var limit: Long = -1,
    var limitablePushedDown: Boolean = false)
  extends BatchTableSource[Row]
  with LimitableTableSource {

  /**
   * Returns the data of the table as a [[org.apache.flink.streaming.api.datastream.DataStream]].
   *
   * NOTE: This method is for internal use only for defining a [[TableSource]].
   * Do not use it in Table API programs.
   */
  override def getBoundedStream(streamEnv: StreamExecutionEnvironment): DataStream[Row] = {
    if (limit == 0 && limit >= 0) {
      throw new RuntimeException("This source can't generate data due abnormal limit " + limit)
    }
    val dataSet = if (limit > 0) {
      data.take(limit.toInt).asJava
    } else {
      new java.util.ArrayList[Row]
    }
    streamEnv.createInput(
      new CollectionInputFormat(dataSet, rowType.createSerializer(new ExecutionConfig)),
      rowType, "TestLimitedTableSource")
  }

  override def applyLimit(limit: Long): TableSource = {
    this.limit = limit
    limitablePushedDown = true
    new TestLimitableTableSource(data, rowType, limit, limitablePushedDown)
  }

  /**
   * Return the flag to indicate whether limit push down has been tried. Must return true on
   * the returned instance of [[applyLimit]].
   */
  override def isLimitPushedDown: Boolean = limitablePushedDown

  /** Returns the [[TypeInformation]] for the return type of the [[TableSource]]. */
  override def getReturnType: DataType = rowType

  /** Describes the table source, it will be used for computing qualified names of
   * [[org.apache.flink.table.plan.schema.FlinkRelOptTable]].
   *
   * Note: After project push down or filter push down, the explainSource value of new TableSource
   * should be different with the original one.
   *
   * */
  override def explainSource(): String = {
    if (limit > 0 && limit < Long.MaxValue) "limit: " + limit else ""
  }

  /** Returns the table schema of the table source */
  override def getTableSchema = TableSchemaUtil.fromDataType(getReturnType)
}

