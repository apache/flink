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

package org.apache.flink.table.planner.sinks

import org.apache.flink.annotation.Internal
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.table.api.{Table, TableException, TableSchema}
import org.apache.flink.table.sinks.TableSink

/**
  * A [[DataStreamTableSink]] specifies how to emit a [[Table]] to an DataStream[T]
  *
  * @param outputType The [[TypeInformation]] that specifies the type of the [[DataStream]].
  * @param updatesAsRetraction Set to true to encode updates as retraction messages.
  * @param withChangeFlag Set to true to emit records with change flags.
  * @tparam T The type of the resulting [[DataStream]].
  */
@Internal
class DataStreamTableSink[T](
    tableSchema: TableSchema,
    outputType: TypeInformation[T],
    val updatesAsRetraction: Boolean,
    val withChangeFlag: Boolean) extends TableSink[T] {

  /**
    * Return the type expected by this [[TableSink]].
    *
    * This type should depend on the types returned by [[getTableSchema]].
    *
    * @return The type expected by this [[TableSink]].
    */
  override def getOutputType: TypeInformation[T] = outputType

  override def getTableSchema: TableSchema = tableSchema

  override def configure(
      fieldNames: Array[String],
      fieldTypes: Array[TypeInformation[_]]): TableSink[T] = {
    throw new TableException(s"configure is not supported.")
  }

}
