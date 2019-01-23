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

package org.apache.flink.table.plan.schema

import org.apache.flink.api.java.typeutils.TupleTypeInfo
import org.apache.flink.api.scala.typeutils.CaseClassTypeInfo
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.plan.stats.FlinkStatistic

/**
  * Table that to be registered in the [[TableEnvironment]]'s catalog. The dataStream is an upsert
  * stream, while [[AppendStreamTable]] contains an append stream.
  */
class UpsertStreamTable[T](
    override val dataStream: DataStream[T],
    override val fieldIndexes: Array[Int],
    override val fieldNames: Array[String],
    val uniqueKeys: Array[String] = Array(),
    override val statistic: FlinkStatistic = FlinkStatistic.UNKNOWN)
  extends DataStreamTable[T](
    dataStream,
    dataStream.getType match {
      case c: CaseClassTypeInfo[_] => c.getTypeAt(1)
      case t: TupleTypeInfo[_] => t.getTypeAt(1)},
    fieldIndexes,
    fieldNames,
    statistic)
