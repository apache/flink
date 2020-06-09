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

import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.runtime.types.LogicalTypeDataTypeConverter.fromDataTypeToLogicalType
import org.apache.flink.table.sinks.TableSink

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.{RelNode, RelWriter, SingleRel}

/**
  * Relational expression that writes out data of input node into a [[TableSink]].
  *
  * @param cluster  cluster that this relational expression belongs to
  * @param traitSet the traits of this rel
  * @param input    input relational expression
  * @param sink     Table sink to write into
  * @param sinkName Name of tableSink, which is not required property, that is, it could be null
  */
abstract class LegacySink(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    input: RelNode,
    val sink: TableSink[_],
    val sinkName: String)
  extends SingleRel(cluster, traitSet, input) {

  override def deriveRowType(): RelDataType = {
    val typeFactory = getCluster.getTypeFactory.asInstanceOf[FlinkTypeFactory]
    val outputType = sink.getConsumedDataType
    typeFactory.createFieldTypeFromLogicalType(fromDataTypeToLogicalType(outputType))
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    super.explainTerms(pw)
      .itemIf("name", sinkName, sinkName != null)
      .item("fields", sink.getTableSchema.getFieldNames.mkString(", "))
  }

}
