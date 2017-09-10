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

package org.apache.flink.table.utils

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.sources._
import org.apache.flink.types.Row

import scala.collection.JavaConverters._

class TestTableSourceWithTime(
    rows: Seq[Row],
    rowType: RowTypeInfo,
    rowtime: String,
    proctime: String)
  extends StreamTableSource[Row]
    with DefinedRowtimeAttribute
    with DefinedProctimeAttribute {

  override def getDataStream(execEnv: StreamExecutionEnvironment): DataStream[Row] = {

    // The source deliberately does not assign timestamps and watermarks.
    // If a rowtime field is configured, the field carries the timestamp.
    // The FromElementsFunction sends out a Long.MaxValue watermark when all rows are emitted.
    execEnv.fromCollection(rows.asJava, rowType)
  }

  override def getRowtimeAttribute: String = rowtime

  override def getProctimeAttribute: String = proctime

  override def getReturnType: TypeInformation[Row] = rowType
}
