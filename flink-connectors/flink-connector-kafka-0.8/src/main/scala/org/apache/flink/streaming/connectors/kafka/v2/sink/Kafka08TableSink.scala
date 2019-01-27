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

package org.apache.flink.streaming.connectors.kafka.v2.sink

import java.lang.{Boolean => JBool}

import org.apache.flink.api.java.tuple.{Tuple2 => JTuple2}
import org.apache.flink.streaming.api.datastream.{DataStream, DataStreamSink}
import org.apache.flink.streaming.connectors.kafka.v2.common.TupleOutputFormatAdapterSink
import org.apache.flink.streaming.connectors.kafka.v2.common.util.SourceUtils
import org.apache.flink.table.api.RichTableSchema
import org.apache.flink.table.api.types.{DataType, DataTypes}
import org.apache.flink.table.connector.DefinedDistribution
import org.apache.flink.table.sinks.{BatchCompatibleStreamTableSink, TableSinkBase, UpsertStreamTableSink}
import org.apache.flink.types.Row

class Kafka08TableSink(
    outputFormatBuilder: Kafka08OutputFormat.Builder,
    schema: RichTableSchema = null)
  extends TableSinkBase[JTuple2[JBool, Row]]
  with UpsertStreamTableSink[Row]
  with DefinedDistribution
  with BatchCompatibleStreamTableSink[JTuple2[JBool, Row]] {
  override protected def copy: TableSinkBase[JTuple2[JBool, Row]] = {
    val sink = new Kafka08TableSink(outputFormatBuilder, schema)
    sink.setPartitionedField(partitionedField)
    sink.setShuffleEmptyKey(_shuffleEmptyKey)
    sink
  }

  override def setKeyFields(keys: Array[String]): Unit = {}

  override def setIsAppendOnly(isAppendOnly: JBool): Unit = {}

  override def getRecordType: DataType = DataTypes.createRowType(getFieldTypes, getFieldNames)

  override def emitDataStream(dataStream: DataStream[JTuple2[JBool, Row]])
    : DataStreamSink[JTuple2[JBool, Row]] = {
    outputFormatBuilder.setRowTypeInfo(SourceUtils.toRowTypeInfo(getRecordType))
    val sink = new TupleOutputFormatAdapterSink[Row](outputFormatBuilder.build())
    //对于retraction的delete请求，直接ignore掉
    dataStream.addSink(sink).name(sink.toString)
  }

  override def emitBoundedStream(d: DataStream[JTuple2[JBool, Row]])
    : DataStreamSink[JTuple2[JBool, Row]] = {
    outputFormatBuilder.setRowTypeInfo(SourceUtils.toRowTypeInfo(getRecordType))
    d.writeUsingOutputFormat(outputFormatBuilder.build())
      .name(String.format("%s-%s", toString, "batch"))
  }

  private var partitionedField: String = null

  private var _shuffleEmptyKey: Boolean = true

  def setPartitionedField(partitionedField: String): Unit = {
    this.partitionedField = partitionedField
  }

  def setShuffleEmptyKey(shuffleEmptyKey: Boolean): Unit = {
    this._shuffleEmptyKey = shuffleEmptyKey
  }

  override def getPartitionField() = partitionedField

  override def shuffleEmptyKey() = _shuffleEmptyKey
}
