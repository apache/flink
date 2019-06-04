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

package org.apache.flink.table.runtime.utils

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.table.api.{BatchTableEnvironment, TableImpl}
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.sinks.{CollectRowTableSink, CollectTableSink}
import org.apache.flink.table.types.TypeInfoLogicalTypeConverter
import org.apache.flink.types.Row

import _root_.scala.collection.JavaConversions._
import _root_.scala.collection.JavaConverters._

object TableUtil {

  /**
    * Returns an collection that contains all rows in this Table.
    *
    * Note: The difference between print() and collect() is
    * - print() prints data on workers and collect() collects data to the client.
    * - You have to call TableEnvironment.execute() to run the job for print(), while collect()
    * calls execute automatically.
    */
  def collect(table: TableImpl): Seq[Row] = collectSink(table, new CollectRowTableSink, None)

  def collect(table: TableImpl, jobName: String): Seq[Row] =
    collectSink(table, new CollectRowTableSink, Option.apply(jobName))

  def collectAsT[T](table: TableImpl, t: TypeInformation[_], jobName: String = null): Seq[T] =
    collectSink(
      table,
      new CollectTableSink(_ => t.asInstanceOf[TypeInformation[T]]), Option(jobName))

  def collectSink[T](
      table: TableImpl, sink: CollectTableSink[T], jobName: Option[String] = None): Seq[T] = {
    // get schema information of table
    val rowType = table.getRelNode.getRowType
    val fieldNames = rowType.getFieldNames.asScala.toArray
    val fieldTypes = rowType.getFieldList
      .map(field => FlinkTypeFactory.toLogicalType(field.getType)).toArray
    val configuredSink = sink.configure(
      fieldNames, fieldTypes.map(TypeInfoLogicalTypeConverter.fromLogicalTypeToTypeInfo))
    BatchTableEnvUtil.collect(table.tableEnv.asInstanceOf[BatchTableEnvironment],
      table, configuredSink.asInstanceOf[CollectTableSink[T]], jobName)
  }

}
