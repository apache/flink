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

package org.apache.flink.table.typeutils

import java.sql.Timestamp

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.typeinfo.SqlTimeTypeInfo
import org.apache.flink.api.common.typeutils.{TypeComparator, TypeSerializer}
import org.apache.flink.api.common.typeutils.base.{LongSerializer, SqlTimestampComparator, SqlTimestampSerializer}

/**
  * Type information for indicating event or processing time. However, it behaves like a
  * regular SQL timestamp but is serialized as Long.
  */
class TimeIndicatorTypeInfo(val isEventTime: Boolean)
  extends SqlTimeTypeInfo[Timestamp](
    classOf[Timestamp],
    SqlTimestampSerializer.INSTANCE,
    classOf[SqlTimestampComparator].asInstanceOf[Class[TypeComparator[Timestamp]]]) {

  // this replaces the effective serializer by a LongSerializer
  // it is a hacky but efficient solution to keep the object creation overhead low but still
  // be compatible with the corresponding SqlTimestampTypeInfo
  override def createSerializer(executionConfig: ExecutionConfig): TypeSerializer[Timestamp] =
    LongSerializer.INSTANCE.asInstanceOf[TypeSerializer[Timestamp]]

  override def toString: String =
    s"TimeIndicatorTypeInfo(${if (isEventTime) "rowtime" else "proctime" })"
}

object TimeIndicatorTypeInfo {

  val ROWTIME_STREAM_MARKER: Int = -1
  val PROCTIME_STREAM_MARKER: Int = -2

  val ROWTIME_BATCH_MARKER: Int = -3
  val PROCTIME_BATCH_MARKER: Int = -4

  val ROWTIME_INDICATOR = new TimeIndicatorTypeInfo(true)
  val PROCTIME_INDICATOR = new TimeIndicatorTypeInfo(false)

}
