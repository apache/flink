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

package org.apache.flink.table.plan.nodes

import org.apache.flink.table.api.TableSchema
import org.apache.flink.table.sources.TableSource

import org.apache.calcite.plan.{RelOptCluster, RelOptTable, RelTraitSet}
import org.apache.calcite.rel.RelWriter
import org.apache.calcite.rel.core.TableScan

import scala.collection.JavaConverters._

abstract class PhysicalTableSourceScan(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    table: RelOptTable,
    tableSchema: TableSchema,
    tableSource: TableSource[_],
    val selectedFields: Option[Array[Int]])
  extends TableScan(cluster, traitSet, table) {

  override def explainTerms(pw: RelWriter): RelWriter = {
    val terms = super.explainTerms(pw)
        .item("fields", deriveRowType().getFieldNames.asScala.mkString(", "))

    val sourceDesc = tableSource.explainSource()
    if (sourceDesc.nonEmpty) {
      terms.item("source", sourceDesc)
    } else {
      terms
    }
  }

  override def toString: String = {
    val tableName = getTable.getQualifiedName
    val s = s"table:$tableName, fields:(${getRowType.getFieldNames.asScala.toList.mkString(", ")})"

    val sourceDesc = tableSource.explainSource()
    if (sourceDesc.nonEmpty) {
      s"Scan($s, source:$sourceDesc)"
    } else {
      s"Scan($s)"
    }
  }

  def copy(traitSet: RelTraitSet, tableSource: TableSource[_]): PhysicalTableSourceScan

}
