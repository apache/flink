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

package org.apache.flink.table.sinks

import java.lang.{Boolean => JBool}

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TupleTypeInfo
import org.apache.flink.table.api.Types

import org.apache.flink.api.java.tuple.{Tuple2 => JTuple2}
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.table.api.Table

/**
  * Defines an external [[TableSink]] to emit a streaming [[Table]] with insert, update, and delete
  * changes.
  *
  * The table will be converted into a stream of accumulate and retraction messages which are
  * encoded as [[JTuple2]].
  * The first field is a [[JBool]] flag to indicate the message type.
  * The second field holds the record of the requested type [[T]].
  *
  * A message with true [[JBool]] flag is an accumulate (or add) message.
  * A message with false flag is a retract message.
  *
  * @tparam T Type of records that this [[TableSink]] expects and supports.
  */
trait RetractStreamTableSink[T] extends TableSink[JTuple2[JBool, T]] {

  /** Returns the requested record type */
  def getRecordType: TypeInformation[T]

  /** Emits the DataStream. */
  def emitDataStream(dataStream: DataStream[JTuple2[JBool, T]]): Unit

  override def getOutputType = new TupleTypeInfo(Types.BOOLEAN, getRecordType)

}
