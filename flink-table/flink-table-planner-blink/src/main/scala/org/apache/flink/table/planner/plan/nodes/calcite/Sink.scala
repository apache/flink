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

package org.apache.flink.table.planner.plan.nodes.calcite

import org.apache.flink.table.catalog.{CatalogTable, ObjectIdentifier}
import org.apache.flink.table.connector.sink.DynamicTableSink
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.runtime.types.LogicalTypeDataTypeConverter.fromDataTypeToLogicalType

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.{RelNode, RelWriter, SingleRel}

import scala.collection.JavaConversions._

/**
  * Relational expression that writes out data of input node into a [[DynamicTableSink]].
  *
  * @param cluster  cluster that this relational expression belongs to
  * @param traitSet the traits of this rel
  * @param input    input relational expression
 *  @param tableIdentifier the full path of the table to retrieve.
 *  @param catalogTable Catalog table where this table source table comes from
 *  @param tableSink the [[DynamicTableSink]] for which to write into
  */
abstract class Sink(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    input: RelNode,
    val tableIdentifier: ObjectIdentifier,
    val catalogTable: CatalogTable,
    val tableSink: DynamicTableSink)
  extends SingleRel(cluster, traitSet, input) {

  override def deriveRowType(): RelDataType = {
    input.getRowType
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    super.explainTerms(pw)
      .item("table", tableIdentifier.asSummaryString())
      .item("fields", getRowType.getFieldNames.mkString(", "))
  }
}
