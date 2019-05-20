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

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.datastream.{DataStream, DataStreamSource}
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.api.TableSchema
import org.apache.flink.table.runtime.utils.TimeTestUtil.EventTimeSourceFunction
import org.apache.flink.table.sources._
import org.apache.flink.table.sources.tsextractors.ExistingField
import org.apache.flink.table.sources.wmstrategies.{AscendingTimestamps, PreserveWatermarks}

import java.util
import java.util.Collections

import scala.collection.JavaConversions._

class TestTableSourceWithTime[T](
    tableSchema: TableSchema,
    returnType: TypeInformation[T],
    values: Seq[T],
    rowtime: String = null,
    proctime: String = null,
    mapping: Map[String, String] = null)
  extends StreamTableSource[T]
  with BatchTableSource[T]
  with DefinedRowtimeAttributes
  with DefinedProctimeAttribute
  with DefinedFieldMapping {

  override def getDataStream(execEnv: StreamExecutionEnvironment): DataStream[T] = {
    execEnv.fromCollection(values, returnType)
  }

  override def getBoundedStream(streamEnv: StreamExecutionEnvironment): DataStreamSource[T] = {
    streamEnv.fromCollection(values, returnType)
  }

  override def getRowtimeAttributeDescriptors: util.List[RowtimeAttributeDescriptor] = {
    // return a RowtimeAttributeDescriptor if rowtime attribute is defined
    if (rowtime != null) {
      Collections.singletonList(new RowtimeAttributeDescriptor(
        rowtime,
        new ExistingField(rowtime),
        new AscendingTimestamps))
    } else {
      Collections.EMPTY_LIST.asInstanceOf[util.List[RowtimeAttributeDescriptor]]
    }
  }

  override def getProctimeAttribute: String = proctime

  override def getReturnType: TypeInformation[T] = returnType

  override def getTableSchema: TableSchema = tableSchema

  override def explainSource(): String = ""

  override def getFieldMapping: util.Map[String, String] = {
    if (mapping != null) mapping else null
  }
}

class TestPreserveWMTableSource[T](
    tableSchema: TableSchema,
    returnType: TypeInformation[T],
    values: Seq[Either[(Long, T), Long]],
    rowtime: String)
  extends StreamTableSource[T]
  with DefinedRowtimeAttributes {

  override def getRowtimeAttributeDescriptors: util.List[RowtimeAttributeDescriptor] = {
    Collections.singletonList(new RowtimeAttributeDescriptor(
      rowtime,
      new ExistingField(rowtime),
      PreserveWatermarks.INSTANCE))
  }

  override def getDataStream(execEnv: StreamExecutionEnvironment): DataStream[T] = {
    execEnv.addSource(new EventTimeSourceFunction[T](values)).setParallelism(1).returns(returnType)
  }

  override def getReturnType: TypeInformation[T] = returnType

  override def getTableSchema: TableSchema = tableSchema

  override def explainSource(): String = ""

}
