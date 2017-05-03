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
package org.apache.flink.table.runtime.aggregate

import java.io.ByteArrayOutputStream
import java.util

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.api.java.typeutils.runtime.RowSerializer
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.memory.DataOutputViewStreamWrapper
import org.apache.flink.types.Row
import org.apache.flink.util.Collector

/**
  * Wrap input to rawBytes
  */
class HashWrapper(raw: Row, serializer: TypeSerializer[Row]) extends Serializable {
  val rawBytes = raw match {
    case row: Row =>
      val baos: ByteArrayOutputStream = new ByteArrayOutputStream
      val out: DataOutputViewStreamWrapper = new DataOutputViewStreamWrapper(baos)
      serializer.asInstanceOf[RowSerializer].serialize(row, out)
      baos.toByteArray
    case _ =>
      val baos: ByteArrayOutputStream = new ByteArrayOutputStream
      val out: DataOutputViewStreamWrapper = new DataOutputViewStreamWrapper(baos)
      serializer.serialize(raw, out)
      baos.toByteArray
  }

  override def hashCode(): Int = util.Arrays.hashCode(rawBytes)

  def canEqual(other: Any) = other.isInstanceOf[HashWrapper]

  override def equals(other: Any) = other match {
    case that: HashWrapper =>
      that.canEqual(this) && util.Arrays.equals(this.rawBytes, that.rawBytes)
    case _ => false
  }
}

class DataStreamDistinctReduce(rowType: TypeInformation[Row])
  extends RichFlatMapFunction[Row, Row] {
  @transient var state: MapState[HashWrapper, Boolean] = null
  @transient var serializer: TypeSerializer[Row] = null;
  @transient var descriptor: MapStateDescriptor[HashWrapper, Boolean] = null


  override def open(parameters: Configuration): Unit = {
    serializer = rowType.createSerializer(getRuntimeContext.getExecutionConfig)
    descriptor = new MapStateDescriptor("left", classOf[HashWrapper], classOf[Boolean])
    state = getRuntimeContext.getMapState(descriptor)
  }

  override def flatMap(value: Row, out: Collector[Row]): Unit = {
    val serializedValue = new HashWrapper(value, serializer)
    if (!state.contains(serializedValue)) {
      state.put(serializedValue, true)
      out.collect(value)
    }
  }

}
