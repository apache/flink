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

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.tuple.{Tuple2 => JTuple2}
import org.apache.flink.api.java.typeutils.TupleTypeInfo
import org.apache.flink.table.api.{Table, Types}

import java.lang.{Boolean => JBool}

/**
  * Defines an external [[TableSink]] to emit a streaming [[Table]] with insert, update, and delete
  * changes.
  *
  * @tparam T Type of records that this [[TableSink]] expects and supports.
  */
trait RetractStreamTableSink[T] extends StreamTableSink[JTuple2[JBool, T]] {

  /** Returns the requested record type */
  def getRecordType: TypeInformation[T]

  override def getOutputType = new TupleTypeInfo(Types.BOOLEAN, getRecordType)

}
