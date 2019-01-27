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

import org.apache.flink.api.java.tuple.{Tuple2 => JTuple2}
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.types.{DataType, DataTypes}

/**
  * Defines an external [[TableSink]] to emit a streaming [[Table]] with insert, update, and delete
  * changes. The [[Table]] must be have unique key fields (atomic or composite) or be append-only.
  *
  * If the [[Table]] does not have a unique key and is not append-only, a
  * [[org.apache.flink.table.api.TableException]] will be thrown.
  *
  * The unique key of the table is configured by the [[UpsertStreamTableSink#setKeyFields()]]
  * method.
  *
  * The [[Table]] will be converted into a stream of upsert and delete messages which are encoded as
  * [[JTuple2]]. The first field is a [[JBool]] flag to indicate the message type. The second field
  * holds the record of the requested type [[T]].
  *
  * A message with true [[JBool]] field is an upsert message for the configured key.
  * A message with false flag is a delete message for the configured key.
  *
  * If the table is append-only, all messages will have a true flag and must be interpreted
  * as insertions.
  *
  * @tparam T Type of records that this [[TableSink]] expects and supports.
  */
trait UpsertStreamTableSink[T] extends BaseUpsertStreamTableSink[JTuple2[JBool, T]] {

  /** Returns the requested record type */
  def getRecordType: DataType

  override def getOutputType = DataTypes.createTupleType(DataTypes.BOOLEAN, getRecordType)
}
