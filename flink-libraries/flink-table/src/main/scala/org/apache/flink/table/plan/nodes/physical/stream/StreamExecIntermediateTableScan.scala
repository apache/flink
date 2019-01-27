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

package org.apache.flink.table.plan.nodes.physical.stream

import org.apache.flink.table.plan.schema.IntermediateRelNodeTable
import org.apache.flink.table.plan.util.UpdatingPlanChecker

import org.apache.calcite.plan.{RelOptCluster, RelOptTable, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.TableScan

class StreamExecIntermediateTableScan(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    table: RelOptTable,
    relDataType: RelDataType)
  extends TableScan(cluster, traitSet, table)
  with StreamPhysicalRel {

  val intermediateTable: IntermediateRelNodeTable =
    getTable.unwrap(classOf[IntermediateRelNodeTable])

  override def deriveRowType(): RelDataType = relDataType

  def isAccRetract: Boolean = intermediateTable.isAccRetract

  override def producesUpdates: Boolean =
    !UpdatingPlanChecker.isAppendOnly(intermediateTable.relNode)

  override def producesRetractions: Boolean = producesUpdates && isAccRetract

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    new StreamExecIntermediateTableScan(cluster, traitSet, getTable, relDataType)
  }

  override def isDeterministic: Boolean = true

}

