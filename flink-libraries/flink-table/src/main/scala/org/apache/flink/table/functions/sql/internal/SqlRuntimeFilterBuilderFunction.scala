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

package org.apache.flink.table.functions.sql.internal

import org.apache.calcite.sql.`type`.{OperandTypes, ReturnTypes, SqlTypeFamily}
import org.apache.calcite.sql.{SqlFunction, SqlFunctionCategory, SqlKind}
import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.plan.rules.physical.batch.runtimefilter.UselessRuntimeFilterRemoveRule

import scala.collection.mutable

class SqlRuntimeFilterBuilderFunction(
    val broadcastId: String,
    var ndv: Double,
    var rowCount: Double) extends SqlFunction(
  s"RUNTIME_FILTER_BUILDER_$broadcastId",
  SqlKind.OTHER_FUNCTION,
  ReturnTypes.BOOLEAN,
  null,
  OperandTypes.family(SqlTypeFamily.ANY),
  SqlFunctionCategory.USER_DEFINED_FUNCTION){

  val filters: mutable.ArrayBuffer[SqlRuntimeFilterFunction] = new mutable.ArrayBuffer

  def minFpp(conf: TableConfig): Double = {
    require(filters.nonEmpty)
    var min = Double.MaxValue
    for (elem <- filters) {
      val fpp = UselessRuntimeFilterRemoveRule.getMinSuitableFpp(conf, elem.rowCount, ndv)
      min = Math.min(fpp, min)
    }
    min
  }

  override def toString = s"RUNTIME_FILTER_BUILDER_$broadcastId"
}
