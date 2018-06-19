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

import org.apache.calcite.rel.`type`.{RelDataType, RelDataTypeFactory}
import org.apache.calcite.schema.Statistic
import org.apache.calcite.schema.impl.AbstractTable

class TableSourceSinkTable[T1, T2](val tableSourceTableOpt: Option[TableSourceTable[T1]],
                                   val tableSinkTableOpt: Option[TableSinkTable[T2]])
  extends AbstractTable {

  // In streaming case, the table schema as source and sink can differ because of extra
  // rowtime/proctime fields. We will always return the source table schema if tableSourceTable
  // is not None, otherwise return the sink table schema. We move the Calcite validation logic of
  // the sink table schema into Flink. This allows us to have different schemas as source and sink
  // of the same table.
  override def getRowType(typeFactory: RelDataTypeFactory): RelDataType = {
    tableSourceTableOpt.map(_.getRowType(typeFactory))
      .orElse(tableSinkTableOpt.map(_.getRowType(typeFactory))).orNull
  }

  override def getStatistic: Statistic = {
    tableSourceTableOpt.map(_.getStatistic)
      .orElse(tableSinkTableOpt.map(_.getStatistic)).orNull
  }
}
