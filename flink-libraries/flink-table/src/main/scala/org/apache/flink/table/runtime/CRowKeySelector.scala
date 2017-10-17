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

package org.apache.flink.table.runtime

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.api.java.typeutils.ResultTypeQueryable
import org.apache.flink.table.runtime.types.CRow
import org.apache.flink.types.Row

/**
  * Null-aware key selector.
  */
class CRowKeySelector(
    val keyFields: Array[Int],
    @transient var returnType: TypeInformation[Row])
  extends KeySelector[CRow, Row]
  with ResultTypeQueryable[Row] {

  override def getKey(value: CRow): Row = {
    Row.project(value.row, keyFields)
  }

  override def getProducedType: TypeInformation[Row] = returnType
}
