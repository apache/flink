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

package org.apache.flink.table.sinks.queryable

import java.lang.{Boolean => JBool}

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.api.java.typeutils.ResultTypeQueryable
import org.apache.flink.types.Row
import org.apache.flink.api.java.tuple.{Tuple2 => JTuple2}

class RowKeySelector(
  private val keyIndices: Array[Int],
  @transient private val returnType: TypeInformation[Row])
  extends KeySelector[JTuple2[JBool, Row], Row]
    with ResultTypeQueryable[Row] {

  override def getKey(value: JTuple2[JBool, Row]): Row = {
    Row.project(value.f1, keyIndices)
  }

  override def getProducedType: TypeInformation[Row] = returnType
}
