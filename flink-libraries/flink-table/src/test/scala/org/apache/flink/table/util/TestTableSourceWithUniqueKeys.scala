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

import java.util.{Set => JSet}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.TableSchema
import org.apache.flink.table.api.types.DataType
import org.apache.flink.table.sources._

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

class TestTableSourceWithUniqueKeys[T](
    rows: Seq[T],
    fieldNames: Array[String],
    fieldIndices: Array[Int],
    uniqueKeySet: JSet[JSet[String]])(implicit rowType: TypeInformation[T])
  extends StreamTableSource[T] with DefinedFieldMapping {

  override def getDataStream(execEnv: StreamExecutionEnvironment): DataStream[T] = {

    // The source deliberately does not assign timestamps and watermarks.
    // If a rowtime field is configured, the field carries the timestamp.
    // The FromElementsFunction sends out a Long.MaxValue watermark when all rows are emitted.
    execEnv.fromCollection(rows.asJava, rowType)
  }

  override def getReturnType: DataType = rowType

  override def getTableSchema: TableSchema = {
    val builder = TableSchema.builder()
    val physicalSchema = TableSchemaUtil.fromDataType(getReturnType)
    fieldNames.zipWithIndex.foreach {
      case (name, idx) => builder.field(name, physicalSchema.getType(fieldIndices.apply(idx)))
    }

    if (uniqueKeySet != null) {
      uniqueKeySet.foreach {
        case uniqueKey: JSet[String] =>
          builder.uniqueIndex(uniqueKey.toArray(new Array[String](0)):_*)
      }
    }
    builder.build()
  }

  override def explainSource(): String = ""

  override def getFieldMapping = {
    val physicalSchema = TableSchemaUtil.fromDataType(getReturnType)
    val mapping = new java.util.HashMap[String, String]()
    fieldNames.zipWithIndex.foreach {
      case (name, idx) => mapping.put(name, physicalSchema.getColumnName(fieldIndices.apply(idx)))
    }
    mapping
  }
}
