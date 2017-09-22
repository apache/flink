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

package org.apache.flink.table.calcite

import java.util

import org.apache.calcite.jdbc.CalciteSchema
import org.apache.calcite.prepare.CalciteCatalogReader
import org.apache.calcite.rel.`type`.RelDataTypeFactory
import org.apache.calcite.schema.Table
import org.apache.calcite.prepare.Prepare.PreparingTable
import org.apache.flink.table.plan.schema.{FlinkRelOptTable, FlinkTable, RelTable, TableSinkTable}

/**
  * Flink specific [[CalciteCatalogReader]] that changes the RelOptTable which wrapped a
  * FlinkTable to a [[org.apache.flink.table.plan.schema.FlinkRelOptTable]].
  */
class FlinkCalciteCatalogReader(
    rootSchema: CalciteSchema,
    caseSensitive: Boolean,
    defaultSchema: util.List[String],
    typeFactory: RelDataTypeFactory)
    extends CalciteCatalogReader(rootSchema, caseSensitive, defaultSchema, typeFactory) {

  override def getTable(names: util.List[String]): PreparingTable = {
    val originRelOptTable = super.getTable(names)
    if (originRelOptTable == null) {
      originRelOptTable
    } else {
      val table = originRelOptTable.unwrap(classOf[Table])
      table match {
        case _: RelTable | _: FlinkTable[_] | _: TableSinkTable[_] =>
          FlinkRelOptTable.create(
            originRelOptTable.getRelOptSchema,
            originRelOptTable.getRowType,
            originRelOptTable.getQualifiedName,
            table)
        case _ => originRelOptTable
      }
    }
  }
}
